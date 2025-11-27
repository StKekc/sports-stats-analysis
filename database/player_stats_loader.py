"""
Загрузчик статистики игроков
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from database_manager import DatabaseManager, CacheManager
from etl_loaders import BaseLoader, PlayersLoader, TeamsLoader


class PlayerStatsLoader(BaseLoader):
    """Загрузчик всех типов статистики игроков"""
    
    def __init__(self, db_manager: DatabaseManager, cache: CacheManager,
                 config: Dict[str, Any], players_loader: PlayersLoader, 
                 teams_loader: TeamsLoader):
        super().__init__(db_manager, cache, config)
        self.players_loader = players_loader
        self.teams_loader = teams_loader
    
    def get_or_create_player_team_season(self, player_id: int, team_id: int,
                                        league_id: int, season_id: int,
                                        age: Optional[float] = None) -> Optional[int]:
        """
        Получить или создать запись player_team_season
        
        Returns:
            ID записи player_team_season
        """
        cache_key = (player_id, team_id, league_id, season_id)
        
        # Проверка в кеше
        if cache_key in self.cache.player_team_seasons:
            return self.cache.player_team_seasons[cache_key]
        
        pts_data = {
            'player_id': player_id,
            'team_id': team_id,
            'league_id': league_id,
            'season_id': season_id,
            'age': age
        }
        
        pts_id, _ = self.db.get_or_create('player_team_seasons',
                                          {'player_id': player_id, 'team_id': team_id,
                                           'league_id': league_id, 'season_id': season_id},
                                          pts_data)
        
        self.cache.player_team_seasons[cache_key] = pts_id
        return pts_id
    
    def load_player_standard_stats(self, stats_file: Path, league_code: str, 
                                   season_code: str) -> int:
        """Загрузить стандартную статистику игроков"""
        df = self.read_csv(stats_file)
        if df is None or df.empty:
            return 0
        
        df = self.cleaner.clean_dataframe(df)
        df = self.field_mapper.map_fields(df, 'player_standard')
        
        league_id = self.cache.leagues.get(league_code)
        season_id = self.cache.seasons.get(season_code)
        
        if not league_id or not season_id:
            return 0
        
        stats_data = []
        for _, row in df.iterrows():
            # Получить или создать игрока
            player_name = row.get('player_name')
            if pd.isna(player_name):
                continue
            
            nation = self.cleaner.parse_nation_code(row.get('nation'))
            born = self.cleaner.convert_to_int(row.get('born'))
            position = row.get('position')
            
            player_id = self.players_loader.get_or_create_player(player_name, nation, born, position)
            if not player_id:
                continue
            
            # Получить команду
            team_name = row.get('team_name')
            if pd.isna(team_name):
                continue
            
            team_id = self.teams_loader.get_or_create_team(team_name)
            if not team_id:
                continue
            
            # Получить или создать player_team_season
            age = self.cleaner.convert_to_numeric(row.get('age'))
            pts_id = self.get_or_create_player_team_season(player_id, team_id, 
                                                           league_id, season_id, age)
            if not pts_id:
                continue
            
            # Подготовить данные статистики
            stat_data = {
                'player_team_season_id': pts_id,
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
                'progressive_carries': self.cleaner.convert_to_int(row.get('progressive_carries')),
                'progressive_passes': self.cleaner.convert_to_int(row.get('progressive_passes')),
                'progressive_receptions': self.cleaner.convert_to_int(row.get('progressive_receptions')),
                'yellow_cards': self.cleaner.convert_to_int(row.get('yellow_cards')),
                'red_cards': self.cleaner.convert_to_int(row.get('red_cards')),
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
            inserted = self.db.insert_many('player_standard_stats', stats_data)
            self.logger.info(f"Loaded {inserted} player standard stats for {league_code} {season_code}")
            return inserted
        
        return 0
    
    def load_player_shooting_stats(self, stats_file: Path, league_code: str, 
                                   season_code: str) -> int:
        """Загрузить статистику ударов игроков"""
        df = self.read_csv(stats_file)
        if df is None or df.empty:
            return 0
        
        df = self.cleaner.clean_dataframe(df)
        df = self.field_mapper.map_fields(df, 'player_shooting')
        
        league_id = self.cache.leagues.get(league_code)
        season_id = self.cache.seasons.get(season_code)
        
        stats_data = []
        for _, row in df.iterrows():
            player_name = row.get('player')
            team_name = row.get('squad')
            
            if pd.isna(player_name) or pd.isna(team_name):
                continue
            
            # Найти player_id и team_id через кеш
            player_id = self._find_player_in_cache(player_name)
            team_id = self.teams_loader.get_or_create_team(team_name)
            
            if not player_id or not team_id:
                continue
            
            pts_id = self.cache.player_team_seasons.get((player_id, team_id, league_id, season_id))
            if not pts_id:
                continue
            
            stat_data = {
                'player_team_season_id': pts_id,
                'ninety_s': self.cleaner.convert_to_numeric(row.get('ninety_s')),
                'goals': self.cleaner.convert_to_int(row.get('goals')),
                'shots': self.cleaner.convert_to_int(row.get('shots')),
                'shots_on_target': self.cleaner.convert_to_int(row.get('shots_on_target')),
                'sot_pct': self.cleaner.convert_to_numeric(row.get('sot_pct')),
                'shots_per_90': self.cleaner.convert_to_numeric(row.get('shots_per_90')),
                'sot_per_90': self.cleaner.convert_to_numeric(row.get('sot_per_90')),
                'goals_per_shot': self.cleaner.convert_to_numeric(row.get('goals_per_shot')),
                'goals_per_sot': self.cleaner.convert_to_numeric(row.get('goals_per_sot')),
                'avg_distance': self.cleaner.convert_to_numeric(row.get('avg_distance')),
                'free_kicks': self.cleaner.convert_to_int(row.get('free_kicks')),
                'penalties': self.cleaner.convert_to_int(row.get('penalties')),
                'penalty_attempts': self.cleaner.convert_to_int(row.get('penalty_attempts')),
                'xg': self.cleaner.convert_to_numeric(row.get('xg')),
                'npxg': self.cleaner.convert_to_numeric(row.get('npxg')),
                'npxg_per_shot': self.cleaner.convert_to_numeric(row.get('npxg_per_shot')),
                'goals_minus_xg': self.cleaner.convert_to_numeric(row.get('goals_minus_xg')),
                'np_goals_minus_xg': self.cleaner.convert_to_numeric(row.get('np_goals_minus_xg'))
            }
            
            stats_data.append(stat_data)
        
        if stats_data:
            inserted = self.db.insert_many('player_shooting_stats', stats_data)
            self.logger.info(f"Loaded {inserted} player shooting stats")
            return inserted
        
        return 0
    
    def load_player_passing_stats(self, stats_file: Path, league_code: str,
                                  season_code: str) -> int:
        """Загрузить статистику передач"""
        return self._load_generic_player_stats(stats_file, league_code, season_code,
                                               'player_passing', 'player_passing_stats')
    
    def load_player_defense_stats(self, stats_file: Path, league_code: str,
                                  season_code: str) -> int:
        """Загрузить оборонительную статистику"""
        return self._load_generic_player_stats(stats_file, league_code, season_code,
                                               'player_defense', 'player_defense_stats')
    
    def load_player_possession_stats(self, stats_file: Path, league_code: str,
                                    season_code: str) -> int:
        """Загрузить статистику владения"""
        return self._load_generic_player_stats(stats_file, league_code, season_code,
                                               'player_possession', 'player_possession_stats')
    
    def load_player_misc_stats(self, stats_file: Path, league_code: str,
                              season_code: str) -> int:
        """Загрузить разную статистику"""
        return self._load_generic_player_stats(stats_file, league_code, season_code,
                                               'player_misc', 'player_misc_stats')
    
    def load_player_passing_types_stats(self, stats_file: Path, league_code: str,
                                        season_code: str) -> int:
        """Загрузить статистику типов передач"""
        return self._load_generic_player_stats(stats_file, league_code, season_code,
                                               'player_passing_types', 'player_passing_types_stats')
    
    def load_player_keeper_stats(self, stats_file: Path, league_code: str,
                                season_code: str) -> int:
        """Загрузить статистику вратарей"""
        return self._load_generic_player_stats(stats_file, league_code, season_code,
                                               'player_keeper', 'player_keeper_stats')
    
    def load_player_keeper_adv_stats(self, stats_file: Path, league_code: str,
                                    season_code: str) -> int:
        """Загрузить продвинутую статистику вратарей"""
        return self._load_generic_player_stats(stats_file, league_code, season_code,
                                               'player_keeper_adv', 'player_keeper_adv_stats')
    
    def _load_generic_player_stats(self, stats_file: Path, league_code: str,
                                   season_code: str, mapping_key: str,
                                   table_name: str) -> int:
        """
        Универсальный загрузчик для игроковой статистики
        
        Args:
            stats_file: Путь к CSV
            league_code: Код лиги
            season_code: Код сезона
            mapping_key: Ключ маппинга полей
            table_name: Имя таблицы в БД
        
        Returns:
            Количество загруженных записей
        """
        df = self.read_csv(stats_file)
        if df is None or df.empty:
            return 0
        
        df = self.cleaner.clean_dataframe(df)
        
        # ВАЖНО: Применить маппинг полей ДО обработки
        df = self.field_mapper.map_fields(df, mapping_key)
        
        league_id = self.cache.leagues.get(league_code)
        season_id = self.cache.seasons.get(season_code)
        
        # Служебные колонки CSV, которые не нужно вставлять в БД
        skip_columns = ['rk', 'player', 'squad', 'nation', 'pos', 'age', 'born', 'matches', 
                       'mp', 'starts', 'min', 'ga90',  # Поля из keeper stats
                       '90', 'att_goal_k', 'cmp_launch']  # Дублирующиеся поля в keeper_adv
        
        stats_data = []
        for _, row in df.iterrows():
            player_name = row.get('player')
            team_name = row.get('squad')
            
            if pd.isna(player_name) or pd.isna(team_name):
                continue
            
            # Пропустить строки с заголовками (если они попали в данные)
            if isinstance(player_name, str) and player_name.lower() in ['player', 'rk', 'squad']:
                continue
            
            player_id = self._find_player_in_cache(player_name)
            team_id = self.teams_loader.get_or_create_team(team_name)
            
            if not player_id or not team_id:
                continue
            
            pts_id = self.cache.player_team_seasons.get((player_id, team_id, league_id, season_id))
            if not pts_id:
                continue
            
            # Конвертировать все поля (кроме служебных)
            stat_data = {'player_team_season_id': pts_id}
            for col in df.columns:
                # Пропустить служебные поля CSV
                if col in skip_columns:
                    continue
                
                value = row.get(col)
                if pd.isna(value):
                    stat_data[col] = None
                else:
                    # Попытка конвертации в число
                    numeric_value = self.cleaner.convert_to_numeric(value)
                    if numeric_value is not None:
                        stat_data[col] = numeric_value
                    else:
                        # Если не удалось конвертировать в число - пропустить эту колонку
                        # (вероятно это заголовок или текст)
                        continue
            
            stats_data.append(stat_data)
        
        if stats_data:
            inserted = self.db.insert_many(table_name, stats_data)
            self.logger.info(f"Loaded {inserted} records into {table_name}")
            return inserted
        
        return 0
    
    def _find_player_in_cache(self, player_name: str) -> Optional[int]:
        """
        Найти игрока в кеше по имени
        
        Args:
            player_name: Имя игрока
        
        Returns:
            ID игрока или None
        """
        # Поиск в кеше (может быть несколько игроков с одним именем)
        for (cached_name, born), player_id in self.cache.players.items():
            if cached_name == player_name:
                return player_id
        
        return None
    
    def load_all_player_stats(self, data_dir: Path, league_code: str, 
                             season_code: str) -> Dict[str, int]:
        """
        Загрузить всю статистику игроков из директории
        
        Args:
            data_dir: Директория с данными лиги-сезона
            league_code: Код лиги
            season_code: Код сезона
        
        Returns:
            Словарь со счетчиками загруженных записей
        """
        results = {}
        
        # Сначала загрузить стандартную статистику (создает player_team_seasons)
        standard_file = data_dir / 'player_standard_stats.csv'
        if standard_file.exists():
            results['standard'] = self.load_player_standard_stats(standard_file, 
                                                                  league_code, season_code)
        
        # Затем остальные типы статистики
        stats_files = {
            'shooting': ('player_shooting_stats.csv', self.load_player_shooting_stats),
            'passing': ('player_passing_stats.csv', self.load_player_passing_stats),
            'defense': ('player_defense_stats.csv', self.load_player_defense_stats),
            'possession': ('player_possession_stats.csv', self.load_player_possession_stats),
            'misc': ('player_misc_stats.csv', self.load_player_misc_stats),
            'passing_types': ('player_passing_types_stats.csv', self.load_player_passing_types_stats),
            'keepers': ('player_keepers_stats.csv', self.load_player_keeper_stats),
            'keepers_adv': ('player_keepers_adv_stats.csv', self.load_player_keeper_adv_stats),
        }
        
        for stat_type, (filename, loader_func) in stats_files.items():
            file_path = data_dir / filename
            if file_path.exists():
                results[stat_type] = loader_func(file_path, league_code, season_code)
        
        return results

