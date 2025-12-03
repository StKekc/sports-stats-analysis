"""
Модуль для обработки спортивной статистики через Apache Spark SQL
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
    
    def calculate_team_dynamics(
        self,
        league_filter: Optional[str] = None,
        season_filter: Optional[str] = None,
        team_names: Optional[List[str]] = None,
        output_parquet_path: Optional[str] = None
    ) -> pd.DataFrame:
        """
        ЗАДАЧА 3: Динамика результатов по сезонам/месяцам
        
        Рассчитывает кумулятивные метрики для каждой команды по ходу сезона
        с использованием оконных функций Spark.
        
        Args:
            league_filter: Фильтр по коду лиги (например, 'epl')
            season_filter: Фильтр по сезону (например, '2023-2024')
            team_names: Список названий команд для фильтрации (если None — все команды)
            output_parquet_path: Путь для сохранения результата в Parquet
        
        Returns:
            pd.DataFrame с колонками:
                - team_id, team_name, league_name, season_code
                - match_date, match_number
                - points, goal_diff (за матч)
                - cumulative_points, cumulative_goal_diff (накопительные)
                - goals_for, goals_against (за матч)
        """
        self.initialize_spark()
        
        logger.info("=" * 70)
        logger.info("ЗАДАЧА 3: Динамика результатов по сезонам (Spark Window Functions)")
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
        
        # Формируем WHERE условия
        where_conditions = ["m.home_goals IS NOT NULL", "m.away_goals IS NOT NULL"]
        
        if league_filter:
            where_conditions.append(f"l.league_code = '{league_filter}'")
        
        if season_filter:
            where_conditions.append(f"s.season_code = '{season_filter}'")
        
        where_clause = " AND ".join(where_conditions)
        
        # Дополнительный фильтр по командам (если указан)
        team_filter_clause = ""
        if team_names:
            team_names_str = ", ".join([f"'{name}'" for name in team_names])
            team_filter_clause = f"AND t.team_name IN ({team_names_str})"
        
        logger.info(f"Фильтры: лига={league_filter or 'все'}, сезон={season_filter or 'все'}")
        if team_names:
            logger.info(f"Команды: {', '.join(team_names)}")
        
        # Spark SQL запрос: объединяем домашние и гостевые матчи
        # и рассчитываем метрики для каждой команды
        spark_sql_query = f"""
        WITH all_team_matches AS (
            -- Домашние матчи
            SELECT 
                t.team_id,
                t.team_name,
                l.league_id,
                l.league_name,
                l.league_code,
                s.season_id,
                s.season_code,
                m.match_id,
                m.match_date,
                m.home_goals as goals_for,
                m.away_goals as goals_against,
                CASE 
                    WHEN m.home_goals > m.away_goals THEN 3
                    WHEN m.home_goals = m.away_goals THEN 1
                    ELSE 0
                END as points,
                (m.home_goals - m.away_goals) as goal_diff,
                'home' as venue_type
            FROM matches m
            JOIN teams t ON m.home_team_id = t.team_id
            JOIN leagues l ON m.league_id = l.league_id
            JOIN seasons s ON m.season_id = s.season_id
            WHERE {where_clause} {team_filter_clause}
            
            UNION ALL
            
            -- Гостевые матчи
            SELECT 
                t.team_id,
                t.team_name,
                l.league_id,
                l.league_name,
                l.league_code,
                s.season_id,
                s.season_code,
                m.match_id,
                m.match_date,
                m.away_goals as goals_for,
                m.home_goals as goals_against,
                CASE 
                    WHEN m.away_goals > m.home_goals THEN 3
                    WHEN m.away_goals = m.home_goals THEN 1
                    ELSE 0
                END as points,
                (m.away_goals - m.home_goals) as goal_diff,
                'away' as venue_type
            FROM matches m
            JOIN teams t ON m.away_team_id = t.team_id
            JOIN leagues l ON m.league_id = l.league_id
            JOIN seasons s ON m.season_id = s.season_id
            WHERE {where_clause} {team_filter_clause}
        )
        SELECT 
            team_id,
            team_name,
            league_id,
            league_name,
            league_code,
            season_id,
            season_code,
            match_id,
            match_date,
            goals_for,
            goals_against,
            points,
            goal_diff,
            venue_type
        FROM all_team_matches
        ORDER BY team_id, season_id, match_date
        """
        
        logger.info("Выполнение Spark SQL запроса для получения данных матчей...")
        base_df = self.spark.sql(spark_sql_query)
        
        # Применяем оконные функции для кумулятивных метрик
        logger.info("Применение оконных функций для расчета кумулятивных метрик...")
        
        # Определяем окно: партиционирование по команде и сезону, сортировка по дате
        window_spec = Window.partitionBy("team_id", "season_id").orderBy("match_date")
        
        # Добавляем кумулятивные метрики
        result_df = base_df \
            .withColumn("match_number", F.row_number().over(window_spec)) \
            .withColumn("cumulative_points", F.sum("points").over(window_spec)) \
            .withColumn("cumulative_goal_diff", F.sum("goal_diff").over(window_spec)) \
            .withColumn("cumulative_goals_for", F.sum("goals_for").over(window_spec)) \
            .withColumn("cumulative_goals_against", F.sum("goals_against").over(window_spec))
        
        # Показываем результаты в консоли
        logger.info(f"\n📊 Пример данных динамики команд:")
        result_df.show(20, truncate=False)
        
        # Сохраняем в Parquet если указан путь
        if output_parquet_path:
            parquet_path = Path(output_parquet_path)
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Сохранение данных в Parquet: {output_parquet_path}")
            result_df.write.mode("overwrite").parquet(str(parquet_path))
            logger.info(f"✅ Данные сохранены в Parquet: {output_parquet_path}")
        
        # Конвертируем в pandas для визуализации
        pandas_df = result_df.toPandas()
        
        # Статистика
        unique_teams = pandas_df['team_name'].nunique()
        unique_seasons = pandas_df['season_code'].nunique()
        total_records = len(pandas_df)
        
        logger.info(f"\n✅ Обработка завершена:")
        logger.info(f"   - Команд: {unique_teams}")
        logger.info(f"   - Сезонов: {unique_seasons}")
        logger.info(f"   - Всего записей: {total_records}")
        
        return pandas_df
    
    def load_dynamics_from_parquet(self, parquet_path: str) -> pd.DataFrame:
        """
        Загружает данные динамики из Parquet файла
        
        Args:
            parquet_path: Путь к Parquet файлу
        
        Returns:
            pd.DataFrame с данными динамики
        """
        self.initialize_spark()
        
        logger.info(f"Загрузка данных из Parquet: {parquet_path}")
        
        df = self.spark.read.parquet(parquet_path)
        pandas_df = df.toPandas()
        
        logger.info(f"✅ Загружено {len(pandas_df)} записей")
        
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

