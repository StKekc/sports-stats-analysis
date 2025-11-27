"""
ETL Загрузчики для различных типов данных
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging
import yaml

from database_manager import DatabaseManager, CacheManager
from etl_utils import DataCleaner, DirectoryParser, FieldMapper, DataValidator


class BaseLoader:
    """Базовый класс для всех загрузчиков"""
    
    def __init__(self, db_manager: DatabaseManager, cache: CacheManager,
                 config: Dict[str, Any]):
        """
        Args:
            db_manager: Менеджер базы данных
            cache: Менеджер кеша
            config: Конфигурация ETL
        """
        self.db = db_manager
        self.cache = cache
        self.config = config
        self.cleaner = DataCleaner(config)
        self.field_mapper = FieldMapper(config.get('field_mappings', {}))
        self.validator = DataValidator(config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def read_csv(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Прочитать CSV файл с обработкой ошибок
        
        Args:
            file_path: Путь к CSV файлу
        
        Returns:
            DataFrame или None при ошибке
        """
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            self.logger.debug(f"Read {len(df)} rows from {file_path}")
            return df
        except FileNotFoundError:
            self.logger.warning(f"File not found: {file_path}")
            return None
        except Exception as e:
            self.logger.error(f"Error reading {file_path}: {e}")
            return None


class ReferenceDataLoader(BaseLoader):
    """Загрузчик справочных данных (лиги, сезоны)"""
    
    def load_leagues(self, leagues_config_path: str) -> int:
        """
        Загрузить справочник лиг из конфигурационного файла
        
        Args:
            leagues_config_path: Путь к leagues.yaml
        
        Returns:
            Количество загруженных лиг
        """
        self.logger.info("Loading leagues...")
        
        with open(leagues_config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        leagues_data = []
        for code, info in config['leagues'].items():
            league_data = {
                'league_code': code,
                'league_name': info['name'],
                'country': info['country'],
                'comp_id': info['comp_id']
            }
            leagues_data.append(league_data)
        
        # Вставка в БД
        for league in leagues_data:
            league_id = self.db.insert_one('leagues', league)
            self.cache.leagues[league['league_code']] = league_id
            self.logger.debug(f"Loaded league: {league['league_code']} (ID: {league_id})")
        
        self.logger.info(f"Successfully loaded {len(leagues_data)} leagues")
        return len(leagues_data)
    
    def load_seasons_from_directories(self, raw_data_path: str) -> int:
        """
        Извлечь и загрузить сезоны из имен директорий
        
        Args:
            raw_data_path: Путь к raw/fbref директории
        
        Returns:
            Количество загруженных сезонов
        """
        self.logger.info("Loading seasons from directories...")
        
        raw_path = Path(raw_data_path)
        seasons_set = set()
        
        # Собрать уникальные сезоны из имен директорий
        for dir_path in raw_path.iterdir():
            if dir_path.is_dir():
                _, season_code = DirectoryParser.parse_directory_name(dir_path.name)
                if season_code:
                    seasons_set.add(season_code)
        
        # Загрузить в БД
        loaded_count = 0
        for season_code in sorted(seasons_set):
            start_year, end_year = DirectoryParser.extract_season_years(season_code)
            
            season_data = {
                'season_code': season_code,
                'start_year': start_year,
                'end_year': end_year
            }
            
            season_id = self.db.insert_one('seasons', season_data)
            self.cache.seasons[season_code] = season_id
            self.logger.debug(f"Loaded season: {season_code} (ID: {season_id})")
            loaded_count += 1
        
        self.logger.info(f"Successfully loaded {loaded_count} seasons")
        return loaded_count


class TeamsLoader(BaseLoader):
    """Загрузчик команд"""
    
    def load_teams_from_standings(self, standings_file: Path, 
                                   league_code: str, season_code: str) -> List[int]:
        """
        Загрузить команды из файла standings
        
        Args:
            standings_file: Путь к standings.csv
            league_code: Код лиги
            season_code: Код сезона
        
        Returns:
            Список ID загруженных команд
        """
        df = self.read_csv(standings_file)
        if df is None or df.empty:
            return []
        
        df = self.cleaner.clean_dataframe(df)
        
        team_ids = []
        for _, row in df.iterrows():
            team_name = row.get('squad')
            if pd.isna(team_name):
                continue
            
            team_id = self.get_or_create_team(team_name)
            if team_id:
                team_ids.append(team_id)
        
        return team_ids
    
    def get_or_create_team(self, team_name: str) -> Optional[int]:
        """
        Получить ID команды или создать новую
        
        Args:
            team_name: Имя команды
        
        Returns:
            ID команды
        """
        normalized_name = self.cleaner.normalize_team_name(team_name)
        
        # Проверка в кеше
        if normalized_name in self.cache.teams:
            return self.cache.teams[normalized_name]
        
        # Поиск или создание в БД
        team_data = {
            'team_name': team_name,
            'normalized_name': normalized_name
        }
        
        team_id, created = self.db.get_or_create('teams', 
                                                  {'normalized_name': normalized_name},
                                                  team_data)
        
        # Сохранение в кеш
        self.cache.teams[normalized_name] = team_id
        
        if created:
            self.logger.debug(f"Created team: {team_name} (ID: {team_id})")
        
        return team_id


class PlayersLoader(BaseLoader):
    """Загрузчик игроков"""
    
    def load_players_from_stats(self, player_stats_file: Path) -> List[int]:
        """
        Загрузить игроков из файла статистики
        
        Args:
            player_stats_file: Путь к player_standard_stats.csv
        
        Returns:
            Список ID загруженных игроков
        """
        df = self.read_csv(player_stats_file)
        if df is None or df.empty:
            return []
        
        df = self.cleaner.clean_dataframe(df)
        
        player_ids = []
        for _, row in df.iterrows():
            player_name = row.get('player')
            if pd.isna(player_name):
                continue
            
            nation = self.cleaner.parse_nation_code(row.get('nation'))
            born = self.cleaner.convert_to_int(row.get('born'))
            position = row.get('pos')
            
            player_id = self.get_or_create_player(player_name, nation, born, position)
            if player_id:
                player_ids.append(player_id)
        
        return player_ids
    
    def get_or_create_player(self, player_name: str, nation: Optional[str] = None,
                            born: Optional[int] = None, position: Optional[str] = None) -> Optional[int]:
        """
        Получить ID игрока или создать нового
        
        Args:
            player_name: Имя игрока
            nation: Код страны
            born: Год рождения
            position: Позиция
        
        Returns:
            ID игрока
        """
        # Ключ для кеша
        cache_key = (player_name, born if born else 0)
        
        # Проверка в кеше
        if cache_key in self.cache.players:
            return self.cache.players[cache_key]
        
        # Валидация
        player_data = {
            'player_name': player_name,
            'nation': nation,
            'born': born,
            'position': position
        }
        
        if not self.validator.validate_player_data(player_data):
            return None
        
        # Поиск или создание в БД
        lookup = {'player_name': player_name, 'born': born} if born else {'player_name': player_name}
        
        player_id, created = self.db.get_or_create('players', lookup, player_data)
        
        # Сохранение в кеш
        self.cache.players[cache_key] = player_id
        
        if created:
            self.logger.debug(f"Created player: {player_name} (ID: {player_id})")
        
        return player_id


class MatchesLoader(BaseLoader):
    """Загрузчик матчей"""
    
    def __init__(self, db_manager: DatabaseManager, cache: CacheManager,
                 config: Dict[str, Any], teams_loader: TeamsLoader):
        super().__init__(db_manager, cache, config)
        self.teams_loader = teams_loader
    
    def load_matches(self, schedule_file: Path, league_code: str, season_code: str) -> int:
        """
        Загрузить матчи из файла schedule_results.csv
        
        Args:
            schedule_file: Путь к schedule_results.csv
            league_code: Код лиги
            season_code: Код сезона
        
        Returns:
            Количество загруженных матчей
        """
        df = self.read_csv(schedule_file)
        if df is None or df.empty:
            return 0
        
        df = self.cleaner.clean_dataframe(df)
        df = self.field_mapper.map_fields(df, 'matches')
        
        # Получить ID лиги и сезона
        league_id = self.cache.leagues.get(league_code)
        season_id = self.cache.seasons.get(season_code)
        
        if not league_id or not season_id:
            self.logger.error(f"League or season not found: {league_code}, {season_code}")
            return 0
        
        matches_data = []
        for _, row in df.iterrows():
            # Парсинг даты
            match_date = self.cleaner.parse_date(row.get('match_date'))
            if not match_date:
                continue
            
            # Получение ID команд
            home_team_name = row.get('home_team_name')
            away_team_name = row.get('away_team_name')
            
            if pd.isna(home_team_name) or pd.isna(away_team_name):
                continue
            
            home_team_id = self.teams_loader.get_or_create_team(home_team_name)
            away_team_id = self.teams_loader.get_or_create_team(away_team_name)
            
            if not home_team_id or not away_team_id:
                continue
            
            # Парсинг счета
            score = row.get('score')
            home_goals, away_goals = self.cleaner.parse_score(score)
            
            # Парсинг времени
            match_time = self.cleaner.parse_time(row.get('match_time'))
            
            match_data = {
                'league_id': league_id,
                'season_id': season_id,
                'match_week': self.cleaner.convert_to_int(row.get('match_week')),
                'match_date': match_date,
                'match_time': match_time,
                'day_of_week': row.get('day_of_week'),
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'score': score,
                'home_goals': home_goals,
                'away_goals': away_goals,
                'home_xg': self.cleaner.convert_to_numeric(row.get('home_xg')),
                'away_xg': self.cleaner.convert_to_numeric(row.get('away_xg')),
                'venue': row.get('venue'),
                'referee': row.get('referee'),
                'attendance': self.cleaner.convert_to_int(row.get('attendance')),
                'match_report_url': row.get('match_report_url'),
                'notes': row.get('notes')
            }
            
            # Валидация
            if self.validator.validate_match_data(match_data):
                matches_data.append(match_data)
        
        # Вставка в БД батчами
        if matches_data:
            inserted = self.db.insert_many('matches', matches_data, 
                                          batch_size=self.config.get('etl', {}).get('batch_size', 1000))
            self.logger.info(f"Loaded {inserted} matches for {league_code} {season_code}")
            return inserted
        
        return 0


class StandingsLoader(BaseLoader):
    """Загрузчик турнирных таблиц"""
    
    def __init__(self, db_manager: DatabaseManager, cache: CacheManager,
                 config: Dict[str, Any], teams_loader: TeamsLoader):
        super().__init__(db_manager, cache, config)
        self.teams_loader = teams_loader
    
    def load_standings(self, standings_file: Path, league_code: str, season_code: str) -> int:
        """
        Загрузить турнирную таблицу
        
        Args:
            standings_file: Путь к standings.csv
            league_code: Код лиги
            season_code: Код сезона
        
        Returns:
            Количество загруженных записей
        """
        df = self.read_csv(standings_file)
        if df is None or df.empty:
            return 0
        
        df = self.cleaner.clean_dataframe(df)
        df = self.field_mapper.map_fields(df, 'standings')
        
        # Получить ID лиги и сезона
        league_id = self.cache.leagues.get(league_code)
        season_id = self.cache.seasons.get(season_code)
        
        if not league_id or not season_id:
            return 0
        
        standings_data = []
        for _, row in df.iterrows():
            team_name = row.get('team_name')
            if pd.isna(team_name):
                continue
            
            team_id = self.teams_loader.get_or_create_team(team_name)
            if not team_id:
                continue
            
            standing_data = {
                'league_id': league_id,
                'season_id': season_id,
                'team_id': team_id,
                'rank': self.cleaner.convert_to_int(row.get('rank')),
                'matches_played': self.cleaner.convert_to_int(row.get('matches_played')),
                'wins': self.cleaner.convert_to_int(row.get('wins')),
                'draws': self.cleaner.convert_to_int(row.get('draws')),
                'losses': self.cleaner.convert_to_int(row.get('losses')),
                'goals_for': self.cleaner.convert_to_int(row.get('goals_for')),
                'goals_against': self.cleaner.convert_to_int(row.get('goals_against')),
                'goal_difference': self.cleaner.convert_to_int(row.get('goal_difference')),
                'points': self.cleaner.convert_to_int(row.get('points')),
                'points_per_match': self.cleaner.convert_to_numeric(row.get('points_per_match')),
                'xg': self.cleaner.convert_to_numeric(row.get('xg')),
                'xga': self.cleaner.convert_to_numeric(row.get('xga')),
                'xgd': self.cleaner.convert_to_numeric(row.get('xgd')),
                'xgd_per_90': self.cleaner.convert_to_numeric(row.get('xgd_per_90')),
                'attendance': self.cleaner.convert_to_int(row.get('attendance')),
                'top_scorer': row.get('top_scorer'),
                'goalkeeper': row.get('goalkeeper'),
                'notes': row.get('notes')
            }
            
            standings_data.append(standing_data)
        
        if standings_data:
            inserted = self.db.insert_many('standings', standings_data)
            self.logger.info(f"Loaded {inserted} standings for {league_code} {season_code}")
            return inserted
        
        return 0


class TeamStatsLoader(BaseLoader):
    """Загрузчик командной статистики"""
    
    def __init__(self, db_manager: DatabaseManager, cache: CacheManager,
                 config: Dict[str, Any], teams_loader: TeamsLoader):
        super().__init__(db_manager, cache, config)
        self.teams_loader = teams_loader
    
    def load_team_stats(self, team_stats_file: Path, league_code: str, season_code: str) -> int:
        """
        Загрузить командную статистику
        
        Args:
            team_stats_file: Путь к team_standard_stats.csv
            league_code: Код лиги
            season_code: Код сезона
        
        Returns:
            Количество загруженных записей
        """
        df = self.read_csv(team_stats_file)
        if df is None or df.empty:
            return 0
        
        df = self.cleaner.clean_dataframe(df)
        df = self.field_mapper.map_fields(df, 'team_stats')
        
        league_id = self.cache.leagues.get(league_code)
        season_id = self.cache.seasons.get(season_code)
        
        if not league_id or not season_id:
            return 0
        
        stats_data = []
        for _, row in df.iterrows():
            team_name = row.get('team_name')
            if pd.isna(team_name):
                continue
            
            team_id = self.teams_loader.get_or_create_team(team_name)
            if not team_id:
                continue
            
            stat_data = {
                'league_id': league_id,
                'season_id': season_id,
                'team_id': team_id,
                'players_used': self.cleaner.convert_to_int(row.get('players_used')),
                'avg_age': self.cleaner.convert_to_numeric(row.get('avg_age')),
                'possession_pct': self.cleaner.convert_to_numeric(row.get('possession_pct')),
                'matches_played': self.cleaner.convert_to_int(row.get('matches_played')),
                'starts': self.cleaner.convert_to_int(row.get('starts')),
                'minutes': self.cleaner.convert_to_int(row.get('minutes')),
                'ninety_s': self.cleaner.convert_to_numeric(row.get('ninety_s')),
                'goals': self.cleaner.convert_to_int(row.get('goals')),
                'assists': self.cleaner.convert_to_int(row.get('assists')),
                'goals_assists': self.cleaner.convert_to_int(row.get('goals_assists')),
                'goals_non_penalty': self.cleaner.convert_to_int(row.get('goals_non_penalty')),
                'penalties': self.cleaner.convert_to_int(row.get('penalties')),
                'penalty_attempts': self.cleaner.convert_to_int(row.get('penalty_attempts')),
                'xg': self.cleaner.convert_to_numeric(row.get('xg')),
                'npxg': self.cleaner.convert_to_numeric(row.get('npxg')),
                'xag': self.cleaner.convert_to_numeric(row.get('xag')),
                'npxg_xag': self.cleaner.convert_to_numeric(row.get('npxg_xag')),
                'yellow_cards': self.cleaner.convert_to_int(row.get('yellow_cards')),
                'red_cards': self.cleaner.convert_to_int(row.get('red_cards')),
                'progressive_carries': self.cleaner.convert_to_int(row.get('progressive_carries')),
                'progressive_passes': self.cleaner.convert_to_int(row.get('progressive_passes')),
                'goals_per_90': self.cleaner.convert_to_numeric(row.get('goals_per_90')),
                'assists_per_90': self.cleaner.convert_to_numeric(row.get('assists_per_90')),
                'goals_assists_per_90': self.cleaner.convert_to_numeric(row.get('goals_assists_per_90')),
                'goals_non_penalty_per_90': self.cleaner.convert_to_numeric(row.get('goals_non_penalty_per_90')),
                'goals_assists_non_penalty_per_90': self.cleaner.convert_to_numeric(row.get('goals_assists_non_penalty_per_90')),
                'xg_per_90': self.cleaner.convert_to_numeric(row.get('xg_per_90')),
                'xag_per_90': self.cleaner.convert_to_numeric(row.get('xag_per_90')),
                'xg_xag_per_90': self.cleaner.convert_to_numeric(row.get('xg_xag_per_90')),
                'npxg_per_90': self.cleaner.convert_to_numeric(row.get('npxg_per_90')),
                'npxg_xag_per_90': self.cleaner.convert_to_numeric(row.get('npxg_xag_per_90'))
            }
            
            stats_data.append(stat_data)
        
        if stats_data:
            inserted = self.db.insert_many('team_season_stats', stats_data)
            self.logger.info(f"Loaded {inserted} team stats for {league_code} {season_code}")
            return inserted
        
        return 0

