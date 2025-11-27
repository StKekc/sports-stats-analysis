"""
Database Manager для управления подключением и операциями с PostgreSQL
"""

import psycopg2
from psycopg2 import sql, extras
from psycopg2.extras import execute_values
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime


class DatabaseManager:
    """
    Класс для управления подключением к PostgreSQL и выполнения операций
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация менеджера БД
        
        Args:
            config: Словарь с настройками подключения (host, port, database, user, password)
        """
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self) -> None:
        """Установить подключение к базе данных"""
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                connect_timeout=self.config.get('connection_timeout', 30)
            )
            self.logger.info(f"Successfully connected to database: {self.config['database']}")
        except psycopg2.Error as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self) -> None:
        """Закрыть подключение к базе данных"""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")
            self.connection = None
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """
        Context manager для получения курсора
        
        Args:
            cursor_factory: Фабрика для создания специального курсора (например, RealDictCursor)
        
        Yields:
            Курсор базы данных
        """
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Transaction failed: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Tuple]:
        """
        Выполнить SQL запрос и вернуть результаты
        
        Args:
            query: SQL запрос
            params: Параметры для запроса
        
        Returns:
            Список кортежей с результатами
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            if cursor.description:  # Если запрос возвращает результаты
                return cursor.fetchall()
            return []
    
    def execute_query_dict(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Выполнить SQL запрос и вернуть результаты в виде списка словарей
        
        Args:
            query: SQL запрос
            params: Параметры для запроса
        
        Returns:
            Список словарей с результатами
        """
        with self.get_cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            if cursor.description:
                return [dict(row) for row in cursor.fetchall()]
            return []
    
    def insert_one(self, table: str, data: Dict[str, Any]) -> Optional[int]:
        """
        Вставить одну запись в таблицу
        
        Args:
            table: Имя таблицы
            data: Словарь с данными
        
        Returns:
            ID вставленной записи (если есть RETURNING clause)
        """
        columns = data.keys()
        values = [data[col] for col in columns]
        
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING *").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        
        with self.get_cursor() as cursor:
            cursor.execute(query, values)
            result = cursor.fetchone()
            if result:
                return result[0]  # Возвращаем первую колонку (обычно ID)
        return None
    
    def insert_many(self, table: str, data: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """
        Вставить множество записей в таблицу (батчами)
        
        Args:
            table: Имя таблицы
            data: Список словарей с данными
            batch_size: Размер батча
        
        Returns:
            Количество вставленных записей
        """
        if not data:
            return 0
        
        columns = list(data[0].keys())
        inserted_count = 0
        
        # Обработка данных батчами
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            values = [[row.get(col) for col in columns] for row in batch]
            
            query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING").format(
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, columns))
            )
            
            try:
                with self.get_cursor() as cursor:
                    execute_values(cursor, query, values, page_size=batch_size)
                    inserted_count += len(batch)
                    self.logger.debug(f"Inserted batch of {len(batch)} records into {table}")
            except Exception as e:
                self.logger.error(f"Failed to insert batch into {table}: {e}")
                raise
        
        self.logger.info(f"Successfully inserted {inserted_count} records into {table}")
        return inserted_count
    
    def upsert_one(self, table: str, data: Dict[str, Any], 
                   conflict_columns: List[str], update_columns: List[str]) -> Optional[int]:
        """
        Вставить или обновить одну запись (UPSERT)
        
        Args:
            table: Имя таблицы
            data: Словарь с данными
            conflict_columns: Колонки для определения конфликта
            update_columns: Колонки для обновления при конфликте
        
        Returns:
            ID записи
        """
        columns = data.keys()
        values = [data[col] for col in columns]
        
        query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) "
            "ON CONFLICT ({}) DO UPDATE SET {} RETURNING *"
        ).format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns)),
            sql.SQL(', ').join(map(sql.Identifier, conflict_columns)),
            sql.SQL(', ').join([
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                for col in update_columns
            ])
        )
        
        with self.get_cursor() as cursor:
            cursor.execute(query, values)
            result = cursor.fetchone()
            if result:
                return result[0]
        return None
    
    def get_or_create(self, table: str, lookup: Dict[str, Any], 
                      defaults: Optional[Dict[str, Any]] = None) -> Tuple[int, bool]:
        """
        Получить существующую запись или создать новую
        
        Args:
            table: Имя таблицы
            lookup: Словарь для поиска записи
            defaults: Дополнительные поля для создания новой записи
        
        Returns:
            Tuple (id, created) - ID записи и флаг создания
        """
        # Попытка найти существующую запись
        where_clause = sql.SQL(' AND ').join([
            sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
            for k in lookup.keys()
        ])
        
        select_query = sql.SQL("SELECT * FROM {} WHERE {}").format(
            sql.Identifier(table),
            where_clause
        )
        
        with self.get_cursor() as cursor:
            cursor.execute(select_query, list(lookup.values()))
            result = cursor.fetchone()
            
            if result:
                return result[0], False  # Запись существует
            
            # Создание новой записи
            insert_data = {**lookup, **(defaults or {})}
            new_id = self.insert_one(table, insert_data)
            return new_id, True
    
    def truncate_table(self, table: str, cascade: bool = False) -> None:
        """
        Очистить таблицу
        
        Args:
            table: Имя таблицы
            cascade: Очистить связанные таблицы
        """
        cascade_clause = " CASCADE" if cascade else ""
        query = sql.SQL("TRUNCATE TABLE {}{}").format(
            sql.Identifier(table),
            sql.SQL(cascade_clause)
        )
        
        with self.get_cursor() as cursor:
            cursor.execute(query)
        
        self.logger.warning(f"Table {table} truncated{' (CASCADE)' if cascade else ''}")
    
    def table_exists(self, table: str) -> bool:
        """
        Проверить существование таблицы
        
        Args:
            table: Имя таблицы
        
        Returns:
            True если таблица существует
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """
        result = self.execute_query(query, (table,))
        return result[0][0] if result else False
    
    def get_table_row_count(self, table: str) -> int:
        """
        Получить количество строк в таблице
        
        Args:
            table: Имя таблицы
        
        Returns:
            Количество строк
        """
        query = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))
        with self.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()[0]
    
    def get_table_stats(self) -> Dict[str, int]:
        """
        Получить статистику по всем таблицам
        
        Returns:
            Словарь {имя_таблицы: количество_строк}
        """
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        tables = self.execute_query(query)
        
        stats = {}
        for (table_name,) in tables:
            stats[table_name] = self.get_table_row_count(table_name)
        
        return stats
    
    def execute_script(self, script_path: str) -> None:
        """
        Выполнить SQL скрипт из файла
        
        Args:
            script_path: Путь к SQL файлу
        """
        with open(script_path, 'r', encoding='utf-8') as f:
            script = f.read()
        
        with self.get_cursor() as cursor:
            cursor.execute(script)
        
        self.logger.info(f"Successfully executed script: {script_path}")
    
    def create_indexes(self) -> None:
        """Создать индексы (если они еще не созданы)"""
        # Индексы создаются в schema.sql
        self.logger.info("Indexes should be created via schema.sql")
    
    def drop_indexes(self, table: str) -> None:
        """
        Удалить все индексы для таблицы (для ускорения загрузки)
        
        Args:
            table: Имя таблицы
        """
        query = """
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = %s 
            AND schemaname = 'public'
            AND indexname NOT LIKE '%_pkey'
        """
        indexes = self.execute_query(query, (table,))
        
        for (index_name,) in indexes:
            drop_query = sql.SQL("DROP INDEX IF EXISTS {}").format(
                sql.Identifier(index_name)
            )
            with self.get_cursor() as cursor:
                cursor.execute(drop_query)
            self.logger.info(f"Dropped index: {index_name}")
    
    def vacuum_analyze(self, table: Optional[str] = None) -> None:
        """
        Выполнить VACUUM ANALYZE для оптимизации
        
        Args:
            table: Имя таблицы (если None - для всей БД)
        """
        old_isolation = self.connection.isolation_level
        self.connection.set_isolation_level(0)  # AUTOCOMMIT mode
        
        try:
            with self.get_cursor() as cursor:
                if table:
                    cursor.execute(sql.SQL("VACUUM ANALYZE {}").format(sql.Identifier(table)))
                    self.logger.info(f"VACUUM ANALYZE completed for {table}")
                else:
                    cursor.execute("VACUUM ANALYZE")
                    self.logger.info("VACUUM ANALYZE completed for entire database")
        finally:
            self.connection.set_isolation_level(old_isolation)
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
        return False


class CacheManager:
    """
    Менеджер кеша для хранения ID справочников в памяти
    (для избежания повторных запросов к БД)
    """
    
    def __init__(self):
        self.leagues = {}  # {league_code: league_id}
        self.seasons = {}  # {season_code: season_id}
        self.teams = {}    # {normalized_name: team_id}
        self.players = {}  # {(player_name, born): player_id}
        self.player_team_seasons = {}  # {(player_id, team_id, league_id, season_id): pts_id}
    
    def clear(self):
        """Очистить весь кеш"""
        self.leagues.clear()
        self.seasons.clear()
        self.teams.clear()
        self.players.clear()
        self.player_team_seasons.clear()
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Получить статистику кеша"""
        return {
            'leagues': len(self.leagues),
            'seasons': len(self.seasons),
            'teams': len(self.teams),
            'players': len(self.players),
            'player_team_seasons': len(self.player_team_seasons)
        }

