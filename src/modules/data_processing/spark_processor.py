"""
Модуль для обработки спортивной статистики через Apache Spark SQL
"""

import logging
from typing import Dict, Optional
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from .spark_config import SparkConfig


logger = logging.getLogger(__name__)


class SparkProcessor:
    """
    Класс для обработки спортивной статистики с использованием Apache Spark
    Читает данные из PostgreSQL, выполняет агрегации через Spark SQL
    """
    
    def __init__(self, db_config: Dict[str, str], spark_config: Optional[SparkConfig] = None):
        """
        Инициализация процессора
        
        Args:
            db_config: Конфигурация подключения к PostgreSQL
            spark_config: Конфигурация Spark (если None, создается по умолчанию)
        """
        self.db_config = db_config
        self.spark_config = spark_config or SparkConfig()
        self.spark: Optional[SparkSession] = None
        self.jdbc_url = self.spark_config.get_postgres_jdbc_url(db_config)
        self.jdbc_properties = self.spark_config.get_jdbc_properties(db_config)
    
    def initialize_spark(self):
        """Инициализирует Spark сессию"""
        if self.spark is None:
            logger.info("Инициализация Spark сессии...")
            self.spark = self.spark_config.create_spark_session()
            logger.info("✅ Spark сессия создана")
    
    def read_table_from_postgres(self, table_name: str) -> DataFrame:
        """
        Читает таблицу из PostgreSQL в Spark DataFrame
        
        Args:
            table_name: Название таблицы
        
        Returns:
            DataFrame: Spark DataFrame с данными таблицы
        """
        self.initialize_spark()
        
        logger.info(f"Чтение таблицы '{table_name}' из PostgreSQL...")
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=table_name,
            properties=self.jdbc_properties
        )
        
        count = df.count()
        logger.info(f"✅ Загружено {count} записей из таблицы '{table_name}'")
        
        return df
    
    def read_query_from_postgres(self, query: str) -> DataFrame:
        """
        Выполняет SQL запрос к PostgreSQL и возвращает результат как Spark DataFrame
        
        Args:
            query: SQL запрос
        
        Returns:
            DataFrame: Spark DataFrame с результатами запроса
        """
        self.initialize_spark()
        
        logger.info(f"Выполнение запроса к PostgreSQL...")
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=f"({query}) as query",
            properties=self.jdbc_properties
        )
        
        return df
    
    def calculate_home_away_win_rate(
        self, 
        league_filter: Optional[str] = None,
        season_filter: Optional[str] = None,
        top_n: int = 10
    ) -> pd.DataFrame:
        """
        ЗАДАЧА 2: Процент побед (дома/в гостях)
        
        Выполняет Spark SQL запрос для расчета процента побед команд
        дома и в гостях.
        
        Args:
            league_filter: Фильтр по лиге (например, 'epl')
            season_filter: Фильтр по сезону (например, '2024-2025')
            top_n: Количество топ команд для вывода
        
        Returns:
            pd.DataFrame: DataFrame с результатами (pandas для визуализации)
        """
        self.initialize_spark()
        
        logger.info("=" * 70)
        logger.info("ЗАДАЧА 2: Расчет процента побед дома/в гостях через Spark SQL")
        logger.info("=" * 70)
        
        # Читаем необходимые таблицы из PostgreSQL
        matches_df = self.read_table_from_postgres("matches")
        teams_df = self.read_table_from_postgres("teams")
        leagues_df = self.read_table_from_postgres("leagues")
        seasons_df = self.read_table_from_postgres("seasons")
        
        # Регистрируем таблицы для Spark SQL
        matches_df.createOrReplaceTempView("matches")
        teams_df.createOrReplaceTempView("teams")
        leagues_df.createOrReplaceTempView("leagues")
        seasons_df.createOrReplaceTempView("seasons")
        
        # Формируем WHERE условия для фильтрации
        where_conditions = ["m.home_goals IS NOT NULL", "m.away_goals IS NOT NULL"]
        
        if league_filter:
            where_conditions.append(f"l.league_code = '{league_filter}'")
        
        if season_filter:
            where_conditions.append(f"s.season_code = '{season_filter}'")
        
        where_clause = " AND ".join(where_conditions)
        
        # Spark SQL запрос для расчета процента побед
        spark_sql_query = f"""
        WITH team_matches AS (
            -- Объединяем все матчи команды (дома и в гостях)
            SELECT 
                t.team_id,
                t.team_name,
                l.league_name,
                m.match_id,
                m.home_team_id,
                m.away_team_id,
                m.home_goals,
                m.away_goals
            FROM teams t
            JOIN matches m ON (t.team_id = m.home_team_id OR t.team_id = m.away_team_id)
            JOIN leagues l ON m.league_id = l.league_id
            JOIN seasons s ON m.season_id = s.season_id
            WHERE {where_clause}
        ),
        home_stats AS (
            -- Статистика домашних матчей
            SELECT 
                team_id,
                team_name,
                league_name,
                COUNT(*) as home_matches,
                SUM(CASE WHEN home_team_id = team_id AND home_goals > away_goals THEN 1 ELSE 0 END) as home_wins
            FROM team_matches
            WHERE home_team_id = team_id
            GROUP BY team_id, team_name, league_name
        ),
        away_stats AS (
            -- Статистика выездных матчей
            SELECT 
                team_id,
                COUNT(*) as away_matches,
                SUM(CASE WHEN away_team_id = team_id AND away_goals > home_goals THEN 1 ELSE 0 END) as away_wins
            FROM team_matches
            WHERE away_team_id = team_id
            GROUP BY team_id
        )
        -- Итоговая статистика с расчетом процентов
        SELECT 
            h.team_id,
            h.team_name,
            h.league_name,
            h.home_matches,
            h.home_wins,
            ROUND((h.home_wins * 100.0) / NULLIF(h.home_matches, 0), 2) as home_win_pct,
            a.away_matches,
            a.away_wins,
            ROUND((a.away_wins * 100.0) / NULLIF(a.away_matches, 0), 2) as away_win_pct,
            ROUND(
                ((h.home_wins + a.away_wins) * 100.0) / NULLIF((h.home_matches + a.away_matches), 0), 
                2
            ) as total_win_pct
        FROM home_stats h
        JOIN away_stats a ON h.team_id = a.team_id
        ORDER BY total_win_pct DESC, home_win_pct DESC
        LIMIT {top_n}
        """
        
        logger.info("Выполнение Spark SQL запроса...")
        logger.info(f"Фильтры: лига={league_filter or 'все'}, сезон={season_filter or 'все'}")
        
        # Выполняем запрос
        result_df = self.spark.sql(spark_sql_query)
        
        # Показываем результаты в консоли
        logger.info(f"\n🏆 Топ-{top_n} команд по проценту побед:")
        result_df.show(truncate=False)
        
        # Конвертируем в pandas для дальнейшей визуализации
        pandas_df = result_df.toPandas()
        
        logger.info(f"✅ Обработка завершена. Найдено команд: {len(pandas_df)}")
        
        return pandas_df
    
    def get_detailed_match_statistics(self) -> pd.DataFrame:
        """
        Получает детальную статистику по всем матчам
        
        Returns:
            pd.DataFrame: DataFrame с детальной статистикой
        """
        self.initialize_spark()
        
        query = """
        SELECT 
            m.match_id,
            m.match_date,
            l.league_name,
            s.season_code,
            ht.team_name as home_team,
            at.team_name as away_team,
            m.home_goals,
            m.away_goals,
            m.home_xg,
            m.away_xg,
            CASE 
                WHEN m.home_goals > m.away_goals THEN 'HOME_WIN'
                WHEN m.home_goals < m.away_goals THEN 'AWAY_WIN'
                ELSE 'DRAW'
            END as result
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        JOIN leagues l ON m.league_id = l.league_id
        JOIN seasons s ON m.season_id = s.season_id
        WHERE m.home_goals IS NOT NULL
        ORDER BY m.match_date DESC
        """
        
        result_df = self.read_query_from_postgres(query)
        return result_df.toPandas()
    
    def close(self):
        """Закрывает Spark сессию"""
        if self.spark is not None:
            logger.info("Закрытие Spark сессии...")
            self.spark_config.stop_spark_session()
            self.spark = None
            logger.info("✅ Spark сессия закрыта")
    
    def __enter__(self):
        """Context manager: инициализирует Spark при входе"""
        self.initialize_spark()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager: закрывает Spark при выходе"""
        self.close()

