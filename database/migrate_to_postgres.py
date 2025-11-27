#!/usr/bin/env python3
"""
Главный скрипт миграции данных в PostgreSQL
"""

import sys
import os
from pathlib import Path
import yaml
import logging
from datetime import datetime
from typing import Dict, Any
import argparse
from tqdm import tqdm

# Добавить текущую директорию в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

from database_manager import DatabaseManager, CacheManager
from etl_loaders import (ReferenceDataLoader, TeamsLoader, PlayersLoader,
                         MatchesLoader, StandingsLoader, TeamStatsLoader)
from player_stats_loader import PlayerStatsLoader
from etl_utils import DirectoryParser


class MigrationOrchestrator:
    """Оркестратор процесса миграции"""
    
    def __init__(self, config_path: str):
        """
        Args:
            config_path: Путь к конфигурационному файлу
        """
        self.config = self.load_config(config_path)
        self.setup_logging()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("=" * 80)
        self.logger.info("STARTING SPORTS DATA MIGRATION TO POSTGRESQL")
        self.logger.info("=" * 80)
        
        # Инициализация менеджеров
        self.db = DatabaseManager(self.config['database'])
        self.cache = CacheManager()
        
        # Инициализация загрузчиков
        self.ref_loader = ReferenceDataLoader(self.db, self.cache, self.config)
        self.teams_loader = TeamsLoader(self.db, self.cache, self.config)
        self.players_loader = PlayersLoader(self.db, self.cache, self.config)
        self.matches_loader = MatchesLoader(self.db, self.cache, self.config, self.teams_loader)
        self.standings_loader = StandingsLoader(self.db, self.cache, self.config, self.teams_loader)
        self.team_stats_loader = TeamStatsLoader(self.db, self.cache, self.config, self.teams_loader)
        self.player_stats_loader = PlayerStatsLoader(self.db, self.cache, self.config,
                                                     self.players_loader, self.teams_loader)
        
        # Статистика миграции
        self.stats = {
            'leagues': 0,
            'seasons': 0,
            'teams': 0,
            'players': 0,
            'matches': 0,
            'standings': 0,
            'team_stats': 0,
            'player_stats': {}
        }
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Загрузить конфигурацию"""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def setup_logging(self):
        """Настроить логирование"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Создать директорию для логов
        log_dir = Path(self.config.get('paths', {}).get('logs', 'logs/etl'))
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Имя файла лога с timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"migration_{timestamp}.log"
        
        # Настройка логгера
        handlers = []
        
        # File handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)
        
        # Console handler
        if log_config.get('console', True):
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            console_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(console_handler)
        
        # Конфигурация root logger
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=handlers
        )
        
        print(f"Logging to: {log_file}")
    
    def create_schema(self, schema_file: str):
        """
        Создать схему БД из SQL файла
        
        Args:
            schema_file: Путь к schema.sql
        """
        self.logger.info("Creating database schema...")
        
        try:
            self.db.connect()
            self.db.execute_script(schema_file)
            self.logger.info("✓ Database schema created successfully")
        except Exception as e:
            self.logger.error(f"✗ Failed to create schema: {e}")
            raise
    
    def load_reference_data(self, leagues_config: str, raw_data_path: str):
        """
        Загрузить справочные данные
        
        Args:
            leagues_config: Путь к leagues.yaml
            raw_data_path: Путь к raw/fbref
        """
        self.logger.info("\n" + "=" * 80)
        self.logger.info("STEP 1: LOADING REFERENCE DATA")
        self.logger.info("=" * 80)
        
        # Загрузка лиг
        self.logger.info("Loading leagues...")
        self.stats['leagues'] = self.ref_loader.load_leagues(leagues_config)
        self.logger.info(f"✓ Loaded {self.stats['leagues']} leagues")
        
        # Загрузка сезонов
        self.logger.info("Loading seasons...")
        self.stats['seasons'] = self.ref_loader.load_seasons_from_directories(raw_data_path)
        self.logger.info(f"✓ Loaded {self.stats['seasons']} seasons")
        
        self.logger.info(f"Cache: {self.cache.get_cache_stats()}")
    
    def load_league_season_data(self, data_dir: Path, league_code: str, season_code: str):
        """
        Загрузить данные для одной лиги-сезона
        
        Args:
            data_dir: Директория с данными
            league_code: Код лиги
            season_code: Код сезона
        """
        self.logger.info(f"\nProcessing {league_code} {season_code}...")
        
        try:
            # 1. Standings (загружает команды)
            standings_file = data_dir / 'standings.csv'
            if standings_file.exists():
                count = self.standings_loader.load_standings(standings_file, league_code, season_code)
                self.stats['standings'] += count
                self.logger.info(f"  ✓ Standings: {count} records")
            
            # 2. Matches
            schedule_file = data_dir / 'schedule_results.csv'
            if schedule_file.exists():
                count = self.matches_loader.load_matches(schedule_file, league_code, season_code)
                self.stats['matches'] += count
                self.logger.info(f"  ✓ Matches: {count} records")
            
            # 3. Team Stats
            team_stats_file = data_dir / 'team_standard_stats.csv'
            if team_stats_file.exists():
                count = self.team_stats_loader.load_team_stats(team_stats_file, league_code, season_code)
                self.stats['team_stats'] += count
                self.logger.info(f"  ✓ Team Stats: {count} records")
            
            # 4. Player Stats (все типы)
            player_standard_file = data_dir / 'player_standard_stats.csv'
            if player_standard_file.exists():
                results = self.player_stats_loader.load_all_player_stats(data_dir, league_code, season_code)
                for stat_type, count in results.items():
                    if stat_type not in self.stats['player_stats']:
                        self.stats['player_stats'][stat_type] = 0
                    self.stats['player_stats'][stat_type] += count
                
                total_player_stats = sum(results.values())
                self.logger.info(f"  ✓ Player Stats: {total_player_stats} records")
        
        except Exception as e:
            error_mode = self.config.get('etl', {}).get('error_mode', 'strict')
            if error_mode == 'strict':
                self.logger.error(f"✗ Error processing {league_code} {season_code}: {e}")
                raise
            else:
                self.logger.warning(f"⚠ Error processing {league_code} {season_code}: {e} (continuing...)")
    
    def load_all_data(self, raw_data_path: str):
        """
        Загрузить все данные из raw директории
        
        Args:
            raw_data_path: Путь к data/raw/fbref
        """
        self.logger.info("\n" + "=" * 80)
        self.logger.info("STEP 2: LOADING MATCH AND STATISTICS DATA")
        self.logger.info("=" * 80)
        
        raw_path = Path(raw_data_path)
        
        # Получить список всех директорий
        league_season_dirs = [d for d in raw_path.iterdir() if d.is_dir()]
        self.logger.info(f"Found {len(league_season_dirs)} league-season directories")
        
        # Обработка с progress bar
        for data_dir in tqdm(league_season_dirs, desc="Processing directories"):
            league_code, season_code = DirectoryParser.parse_directory_name(data_dir.name)
            
            if not league_code or not season_code:
                self.logger.warning(f"Could not parse directory name: {data_dir.name}")
                continue
            
            # Проверка что лига и сезон есть в БД
            if league_code not in self.cache.leagues:
                self.logger.warning(f"League {league_code} not in cache, skipping")
                continue
            
            if season_code not in self.cache.seasons:
                self.logger.warning(f"Season {season_code} not in cache, skipping")
                continue
            
            self.load_league_season_data(data_dir, league_code, season_code)
        
        # Обновить статистику по командам и игрокам из кеша
        self.stats['teams'] = len(self.cache.teams)
        self.stats['players'] = len(self.cache.players)
    
    def print_statistics(self):
        """Вывести статистику миграции"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("MIGRATION STATISTICS")
        self.logger.info("=" * 80)
        
        self.logger.info(f"Leagues:       {self.stats['leagues']:>10,}")
        self.logger.info(f"Seasons:       {self.stats['seasons']:>10,}")
        self.logger.info(f"Teams:         {self.stats['teams']:>10,}")
        self.logger.info(f"Players:       {self.stats['players']:>10,}")
        self.logger.info(f"Matches:       {self.stats['matches']:>10,}")
        self.logger.info(f"Standings:     {self.stats['standings']:>10,}")
        self.logger.info(f"Team Stats:    {self.stats['team_stats']:>10,}")
        
        if self.stats['player_stats']:
            self.logger.info("\nPlayer Stats:")
            for stat_type, count in self.stats['player_stats'].items():
                self.logger.info(f"  {stat_type.capitalize()}: {count:>10,}")
        
        self.logger.info("=" * 80)
        
        # Статистика БД
        self.logger.info("\nDatabase Statistics:")
        try:
            db_stats = self.db.get_table_stats()
            for table, count in sorted(db_stats.items()):
                if count > 0:
                    self.logger.info(f"  {table}: {count:>10,}")
        except Exception as e:
            self.logger.warning(f"Could not get database statistics: {e}")
    
    def optimize_database(self):
        """Оптимизировать базу данных после загрузки"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("STEP 3: OPTIMIZING DATABASE")
        self.logger.info("=" * 80)
        
        try:
            self.logger.info("Running VACUUM ANALYZE...")
            self.db.vacuum_analyze()
            self.logger.info("✓ Database optimized")
        except Exception as e:
            self.logger.warning(f"⚠ Optimization failed: {e}")
    
    def run(self, schema_file: str, leagues_config: str, raw_data_path: str,
            skip_schema: bool = False, optimize: bool = True):
        """
        Запустить полный процесс миграции
        
        Args:
            schema_file: Путь к schema.sql
            leagues_config: Путь к leagues.yaml
            raw_data_path: Путь к data/raw/fbref
            skip_schema: Пропустить создание схемы
            optimize: Оптимизировать БД после загрузки
        """
        start_time = datetime.now()
        
        try:
            # Подключение к БД
            self.db.connect()
            
            # Создание схемы
            if not skip_schema:
                self.create_schema(schema_file)
            else:
                self.logger.info("Skipping schema creation (--skip-schema)")
            
            # Загрузка справочников
            self.load_reference_data(leagues_config, raw_data_path)
            
            # Загрузка основных данных
            self.load_all_data(raw_data_path)
            
            # Оптимизация
            if optimize:
                self.optimize_database()
            
            # Статистика
            self.print_statistics()
            
            # Время выполнения
            elapsed = datetime.now() - start_time
            self.logger.info(f"\n✓ Migration completed successfully in {elapsed}")
            
        except Exception as e:
            self.logger.error(f"\n✗ Migration failed: {e}", exc_info=True)
            raise
        
        finally:
            self.db.disconnect()


def main():
    """Точка входа"""
    parser = argparse.ArgumentParser(
        description='Migrate sports statistics data to PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full migration
  python migrate_to_postgres.py
  
  # Skip schema creation (if already exists)
  python migrate_to_postgres.py --skip-schema
  
  # Custom config
  python migrate_to_postgres.py --config my_config.yaml
  
  # Custom paths
  python migrate_to_postgres.py --raw-data ../data/raw/fbref --schema db_schema.sql
        """
    )
    
    parser.add_argument('--config', default='database/config.yaml',
                       help='Path to config.yaml (default: database/config.yaml)')
    parser.add_argument('--schema', default='database/schema.sql',
                       help='Path to schema.sql (default: database/schema.sql)')
    parser.add_argument('--leagues-config', default='src/config/leagues.yaml',
                       help='Path to leagues.yaml (default: src/config/leagues.yaml)')
    parser.add_argument('--raw-data', default='data/raw/fbref',
                       help='Path to raw data directory (default: data/raw/fbref)')
    parser.add_argument('--skip-schema', action='store_true',
                       help='Skip schema creation')
    parser.add_argument('--no-optimize', action='store_true',
                       help='Skip database optimization')
    
    args = parser.parse_args()
    
    # Проверка путей
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    
    schema_path = Path(args.schema)
    if not args.skip_schema and not schema_path.exists():
        print(f"Error: Schema file not found: {schema_path}")
        sys.exit(1)
    
    leagues_config_path = Path(args.leagues_config)
    if not leagues_config_path.exists():
        print(f"Error: Leagues config not found: {leagues_config_path}")
        sys.exit(1)
    
    raw_data_path = Path(args.raw_data)
    if not raw_data_path.exists():
        print(f"Error: Raw data directory not found: {raw_data_path}")
        sys.exit(1)
    
    # Запуск миграции
    orchestrator = MigrationOrchestrator(str(config_path))
    orchestrator.run(
        schema_file=str(schema_path),
        leagues_config=str(leagues_config_path),
        raw_data_path=str(raw_data_path),
        skip_schema=args.skip_schema,
        optimize=not args.no_optimize
    )


if __name__ == '__main__':
    main()

