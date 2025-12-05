"""
Модуль для обработки спортивной статистики через Apache Spark SQL
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from .spark_config import SparkConfig
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from scipy.stats import mstats

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


    def analyze_team_playing_styles(
            self,
            league_filter: Optional[str] = None,
            season_filter: Optional[str] = None,
            min_matches: int = 10,
            n_clusters: Optional[int] = None
    ) -> pd.DataFrame:
        """
        ЗАДАЧА 1: Анализ игровых стилей команд через кластеризацию
        Анализирует статистику команд и определяет их игровые стили
        с использованием кластеризации K-means на основе Spark SQL.
        Args:
            league_filter: Фильтр по лиге (например, 'epl')
            season_filter: Фильтр по сезону (например, '2023-2024')
            min_matches: Минимальное количество матчей для включения команды
            n_clusters: Количество кластеров (если None, определяется автоматически)
        Returns:
            pd.DataFrame: DataFrame с командами и их игровыми стилями
        """
        self.initialize_spark()

        logger.info("=" * 70)
        logger.info("ЗАДАЧА 1: Анализ игровых стилей команд через Spark и кластеризацию")
        logger.info("=" * 70)

        # 1. Загружаем и подготавливаем данные через Spark SQL
        logger.info("📥 Загрузка данных статистики команд...")
        prepared_data = self._prepare_team_style_data(
            league_filter, season_filter, min_matches
        )

        # 2. Создаем метрики для кластеризации
        logger.info("📊 Подготовка метрик для анализа стилей...")
        metrics_df = self._create_style_metrics(prepared_data)

        # 3. Выполняем кластеризацию
        logger.info("🔍 Выполнение кластеризации K-means...")
        clustering_result = self._perform_team_clustering(metrics_df, n_clusters)

        # 4. Анализируем кластеры
        logger.info("📈 Анализ характеристик кластеров...")
        final_result = self._analyze_cluster_styles(clustering_result)

        # 5. Сохраняем результаты
        logger.info("💾 Сохранение результатов...")
        self._save_style_analysis_results(final_result)

        logger.info(f"✅ Анализ завершен. Определено {final_result['n_clusters']} стилей игры")

        return final_result['teams_with_styles']

    def _prepare_team_style_data(
            self,
            league_filter: Optional[str],
            season_filter: Optional[str],
            min_matches: int
    ) -> DataFrame:
        """
        Подготавливает данные статистики команд через Spark SQL
        Returns:
            DataFrame: Подготовленные данные команд
        """
        # Читаем таблицы из PostgreSQL
        team_stats_df = self.read_table_from_postgres("team_season_stats")
        teams_df = self.read_table_from_postgres("teams")
        leagues_df = self.read_table_from_postgres("leagues")
        seasons_df = self.read_table_from_postgres("seasons")

        # Регистрируем таблицы для Spark SQL
        team_stats_df.createOrReplaceTempView("team_season_stats")
        teams_df.createOrReplaceTempView("teams")
        leagues_df.createOrReplaceTempView("leagues")
        seasons_df.createOrReplaceTempView("seasons")

        # Формируем условия фильтрации
        where_conditions = ["tss.matches_played > 0"]

        if league_filter:
            where_conditions.append(f"l.league_code = '{league_filter}'")

        if season_filter:
            where_conditions.append(f"s.season_code = '{season_filter}'")

        where_clause = " AND ".join(where_conditions)

        # SQL запрос для получения очищенных данных
        sql_query = f"""
        WITH team_stats_clean AS (
            SELECT 
                tss.*,
                t.team_name,
                l.league_name,
                l.league_code,
                s.season_code,
                -- Заполняем пропущенные значения
                COALESCE(tss.goals_per_90, 0) as goals_per_90_clean,
                COALESCE(tss.xg_per_90, 0.1) as xg_per_90_clean,
                COALESCE(tss.assists_per_90, 0) as assists_per_90_clean,
                COALESCE(tss.possession_pct, 0) as possession_pct_clean,
                COALESCE(tss.progressive_passes, 0) as progressive_passes_clean,
                COALESCE(tss.yellow_cards, 0) as yellow_cards_clean,
                COALESCE(tss.avg_age, 25.0) as avg_age_clean,
                COALESCE(tss.players_used, 20) as players_used_clean
            FROM team_season_stats tss
            LEFT JOIN teams t ON tss.team_id = t.team_id
            LEFT JOIN leagues l ON tss.league_id = l.league_id
            LEFT JOIN seasons s ON tss.season_id = s.season_id
            WHERE {where_clause} 
                AND tss.matches_played >= {min_matches}
                AND tss.minutes > 0
        )
        SELECT 
            team_id,
            team_name,
            league_name,
            league_code,
            season_code,
            season_id,
            matches_played,
            minutes,
            -- Основные метрики
            goals_per_90_clean as goals_per_90,
            xg_per_90_clean as xg_per_90,
            assists_per_90_clean as assists_per_90,
            possession_pct_clean as possession_pct,
            progressive_passes_clean as progressive_passes,
            yellow_cards_clean as yellow_cards,
            avg_age_clean as avg_age,
            players_used_clean as players_used
        FROM team_stats_clean
        """

        logger.info(f"Фильтры: лига={league_filter or 'все'}, "
                    f"сезон={season_filter or 'все'}, мин.матчей={min_matches}")

        result_df = self.spark.sql(sql_query)
        count = result_df.count()
        logger.info(f"✅ Подготовлено {count} записей статистики команд")

        return result_df

    def _create_style_metrics(self, team_data: DataFrame) -> DataFrame:
        """
        Создает метрики для анализа игровых стилей
        Args:
            team_data: DataFrame с базовой статистикой
        Returns:
            DataFrame: DataFrame с рассчитанными метриками стиля
        """
        from pyspark.sql import functions as F

        logger.info("Создание метрик игрового стиля...")

        # Рассчитываем дополнительные метрики
        metrics_df = team_data.select(
            F.col("team_id"),
            F.col("team_name"),
            F.col("league_name"),
            F.col("season_code"),
            F.col("season_id"),

            # 1. Атакующий потенциал (базовые метрики)
            F.col("goals_per_90").alias("attacking_power"),
            F.col("xg_per_90").alias("expected_attacking"),

            # 2. Эффективность атаки (голы / xG)
            (F.col("goals_per_90") /
             F.when(F.col("xg_per_90") > 0.1, F.col("xg_per_90"))
             .otherwise(0.1)).alias("attack_efficiency"),

            # 3. Креативность и создание моментов
            F.col("assists_per_90").alias("creativity"),
            F.col("progressive_passes").alias("progressive_actions"),

            # 4. Контроль и владение мячом
            F.col("possession_pct").alias("possession_control"),

            # 5. Агрессивность и физическая игра
            F.col("yellow_cards").alias("aggressiveness"),

            # 6. Возрастная структура
            F.col("avg_age").alias("team_age_profile"),

            # 7. Широта использования состава
            F.col("players_used").alias("squad_rotation"),

            # 8. Разнообразие атаки (прогрессивные действия на гол)
            (F.col("progressive_passes") /
             F.when(F.col("goals_per_90") > 0.5, F.col("goals_per_90"))
             .otherwise(0.5)).alias("attack_variety"),

            # 9. Интенсивность атаки (голы на владение)
            (F.col("goals_per_90") * 100 /
             F.when(F.col("possession_pct") > 10, F.col("possession_pct"))
             .otherwise(10)).alias("attack_intensity")
        )

        # Фильтруем некорректные данные
        metrics_df = metrics_df.filter(
            (F.col("attacking_power") >= 0) &
            (F.col("possession_control") >= 0) &
            (F.col("possession_control") <= 100) &
            (F.col("team_age_profile") >= 18) &
            (F.col("team_age_profile") <= 40)
        )

        # Заполняем оставшиеся пропуски
        metrics_df = metrics_df.fillna(0)

        final_count = metrics_df.count()
        logger.info(f"✅ Создано {final_count} записей с {len(metrics_df.columns)} метриками стиля")

        return metrics_df

    def _perform_team_clustering(
            self,
            metrics_df: DataFrame,
            n_clusters: Optional[int] = None
    ) -> Dict:
        """
        Выполняет кластеризацию команд по стилям игры
        Args:
            metrics_df: DataFrame с метриками стиля
            n_clusters: Количество кластеров (None для автоопределения)
        Returns:
            Dict: Результаты кластеризации
        """
        logger.info("Выполнение кластеризации K-means...")

        # Список метрик для кластеризации
        feature_columns = [
            "attacking_power",
            "attack_efficiency",
            "creativity",
            "possession_control",
            "aggressiveness",
            "team_age_profile",
            "squad_rotation",
            "attack_variety",
            "attack_intensity"
        ]

        # Конвертируем в pandas для scikit-learn
        logger.info("Конвертация Spark DataFrame в pandas для ML...")
        pandas_df = metrics_df.select(
            "team_id", "team_name", "league_name", "season_code", *feature_columns
        ).toPandas()

        # Проверяем достаточно ли данных
        if len(pandas_df) < 10:
            logger.warning(f"Слишком мало данных для кластеризации: {len(pandas_df)} записей")
            return {"data": pandas_df, "labels": np.zeros(len(pandas_df), dtype=int)}

        # Подготавливаем данные для кластеризации
        X = pandas_df[feature_columns].values

        # Стандартизация
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Определяем оптимальное количество кластеров
        if n_clusters is None:
            optimal_k = self._find_optimal_cluster_count(X_scaled)
            n_clusters = optimal_k
            logger.info(f"Автоопределение: оптимальное количество кластеров = {n_clusters}")

        # Ограничиваем максимальное количество кластеров
        n_clusters = min(n_clusters, max(2, len(pandas_df) // 10))

        # Выполняем кластеризацию K-means
        from sklearn.cluster import KMeans
        from sklearn.metrics import silhouette_score

        kmeans = KMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init=20,
            max_iter=300
        )

        cluster_labels = kmeans.fit_predict(X_scaled)

        # Вычисляем метрики качества
        if n_clusters > 1:
            silhouette_avg = silhouette_score(X_scaled, cluster_labels)
        else:
            silhouette_avg = 0

        logger.info(f"Кластеризация завершена:")
        logger.info(f"  • Количество кластеров: {n_clusters}")
        logger.info(f"  • Средний силуэтный коэффициент: {silhouette_avg:.3f}")
        logger.info(f"  • Inertia: {kmeans.inertia_:.2f}")

        # Распределение по кластерам
        cluster_counts = pd.Series(cluster_labels).value_counts().sort_index()
        for cluster_id, count in cluster_counts.items():
            logger.info(f"  • Кластер {cluster_id}: {count} команд ({count / len(pandas_df) * 100:.1f}%)")

        return {
            "data": pandas_df,
            "features": feature_columns,
            "labels": cluster_labels,
            "centers": kmeans.cluster_centers_,
            "scaler": scaler,
            "model": kmeans,
            "silhouette_score": silhouette_avg,
            "n_clusters": n_clusters,
            "cluster_counts": cluster_counts.to_dict()
        }

    def _find_optimal_cluster_count(self, X_scaled: np.ndarray, max_k: int = 8) -> int:
        """
        Находит оптимальное количество кластеров
        Args:
            X_scaled: Масштабированные данные
            max_k: Максимальное количество кластеров для проверки
        Returns:
            int: Оптимальное количество кластеров
        """
        from sklearn.cluster import KMeans
        from sklearn.metrics import silhouette_score

        logger.info("Поиск оптимального количества кластеров...")

        n_samples = len(X_scaled)
        max_k = min(max_k, n_samples // 3)  # Не больше чем n_samples/3

        if max_k < 2:
            return 2

        inertia_values = []
        silhouette_values = []
        k_range = range(2, max_k + 1)

        for k in k_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(X_scaled)

            inertia_values.append(kmeans.inertia_)

            if len(set(labels)) > 1:  # Не вычисляем силуэт для одного кластера
                silhouette_values.append(silhouette_score(X_scaled, labels))
            else:
                silhouette_values.append(0)

        # Метод локтя: находим точку наибольшего изгиба
        if len(inertia_values) > 1:
            diffs = np.diff(inertia_values)
            diff_ratios = diffs[1:] / diffs[:-1] if len(diffs) > 1 else [0]

            if len(diff_ratios) > 0:
                elbow_k = np.argmax(diff_ratios) + 3  # +3 потому что начинаем с k=2
            else:
                elbow_k = 2
        else:
            elbow_k = 2

        # Метод силуэта: выбираем k с максимальным силуэтом
        if len(silhouette_values) > 0:
            silhouette_k = k_range[np.argmax(silhouette_values)]
        else:
            silhouette_k = 2

        # Комбинируем оба метода
        optimal_k = max(elbow_k, silhouette_k, 3)
        optimal_k = min(optimal_k, max_k)

        logger.info(f"Оптимальное количество кластеров: {optimal_k}")
        logger.info(f"  • Метод локтя: {elbow_k}")
        logger.info(f"  • Метод силуэта: {silhouette_k}")

        return optimal_k

    def _analyze_cluster_styles(self, clustering_result: Dict) -> Dict:
        """
        Анализирует кластеры и определяет игровые стили
        Args:
            clustering_result: Результаты кластеризации
        Returns:
            Dict: Результаты анализа с определенными стилями
        """
        logger.info("Анализ характеристик кластеров...")

        pandas_df = clustering_result["data"]
        cluster_labels = clustering_result["labels"]
        cluster_centers = clustering_result["centers"]
        n_clusters = clustering_result["n_clusters"]
        scaler = clustering_result["scaler"]

        # Добавляем метки кластеров в данные
        pandas_df["cluster"] = cluster_labels

        # Анализируем каждый кластер
        cluster_analysis = []
        style_mapping = {}

        for cluster_id in range(n_clusters):
            # Фильтруем данные кластера
            cluster_data = pandas_df[pandas_df["cluster"] == cluster_id]

            if len(cluster_data) == 0:
                continue

            # Вычисляем средние значения метрик
            cluster_means = cluster_data.mean(numeric_only=True)

            # Определяем стиль игры на основе характеристик
            style_name = self._determine_playing_style_from_metrics(
                attacking=cluster_means.get("attacking_power", 0),
                possession=cluster_means.get("possession_control", 0),
                efficiency=cluster_means.get("attack_efficiency", 0),
                creativity=cluster_means.get("creativity", 0),
                aggressiveness=cluster_means.get("aggressiveness", 0),
                age=cluster_means.get("team_age_profile", 0)
            )

            # Характеристики кластера
            cluster_info = {
                "cluster_id": cluster_id,
                "style_name": style_name,
                "team_count": len(cluster_data),
                "percentage": len(cluster_data) / len(pandas_df) * 100,
                "avg_attacking": cluster_means.get("attacking_power", 0),
                "avg_possession": cluster_means.get("possession_control", 0),
                "avg_efficiency": cluster_means.get("attack_efficiency", 0),
                "avg_creativity": cluster_means.get("creativity", 0),
                "avg_aggressiveness": cluster_means.get("aggressiveness", 0),
                "avg_age": cluster_means.get("team_age_profile", 0),
                "top_teams": cluster_data.nlargest(3, "attacking_power")["team_name"].tolist()
            }

            cluster_analysis.append(cluster_info)
            style_mapping[cluster_id] = style_name

            logger.info(f"Кластер {cluster_id}: {style_name}")
            logger.info(f"  • Команд: {cluster_info['team_count']} ({cluster_info['percentage']:.1f}%)")
            logger.info(f"  • Средняя атака: {cluster_info['avg_attacking']:.2f}")
            logger.info(f"  • Среднее владение: {cluster_info['avg_possession']:.1f}%")
            logger.info(f"  • Топ команды: {', '.join(cluster_info['top_teams'])}")

        # Присваиваем стили командам
        pandas_df["playing_style"] = pandas_df["cluster"].map(style_mapping)

        # Анализируем распределение по лигам
        league_distribution = pandas_df.groupby(["league_name", "playing_style"]).size().unstack(fill_value=0)

        # Анализируем изменения стилей по сезонам
        style_changes = self._analyze_style_changes_over_time(pandas_df)

        return {
            "teams_with_styles": pandas_df,
            "cluster_analysis": pd.DataFrame(cluster_analysis),
            "league_distribution": league_distribution,
            "style_changes": style_changes,
            "style_mapping": style_mapping,
            "n_clusters": n_clusters,
            "silhouette_score": clustering_result["silhouette_score"]
        }

    def _determine_playing_style_from_metrics(
            self,
            attacking: float,
            possession: float,
            efficiency: float,
            creativity: float,
            aggressiveness: float,
            age: float
    ) -> str:
        """
        Определяет название игрового стиля на основе метрик
        Returns:
            str: Название стиля игры
        """
        # Определяем основные характеристики
        is_attacking = attacking > 1.4
        is_possession = possession > 55
        is_efficient = efficiency > 1.05
        is_creative = creativity > 0.8
        is_aggressive = aggressiveness > 70
        is_young = age < 26

        # Определяем стиль на основе комбинации характеристик
        if is_attacking:
            if is_possession:
                if is_efficient:
                    return "Доминирующие эффективные"
                else:
                    return "Контролирующие атакующие"
            else:
                if is_efficient:
                    return "Эффективные контратакующие"
                else:
                    return "Прямые атакующие"
        else:
            if is_possession:
                if is_aggressive:
                    return "Агрессивные контроллеры"
                else:
                    return "Пассивные контроллеры"
            else:
                if is_aggressive:
                    return "Агрессивные оборонительные"
                elif is_young:
                    return "Молодые перспективные"
                else:
                    return "Сбалансированные оборонительные"

    def _analyze_style_changes_over_time(self, teams_df: pd.DataFrame) -> pd.DataFrame:
        """
        Анализирует изменения стилей команд по сезонам
        Args:
            teams_df: DataFrame с командами и стилями
        Returns:
            DataFrame: Анализ изменений стилей
        """
        logger.info("Анализ изменений стилей по сезонам...")

        # Сортируем по команде и сезону
        teams_sorted = teams_df.sort_values(["team_id", "season_id"])

        # Находим команды с несколькими сезонами
        team_season_counts = teams_sorted.groupby("team_id").size()
        teams_with_multiple = team_season_counts[team_season_counts > 1].index

        if len(teams_with_multiple) == 0:
            logger.info("Нет команд с несколькими сезонами для анализа изменений")
            return pd.DataFrame()

        style_changes = []

        for team_id in teams_with_multiple:
            team_data = teams_sorted[teams_sorted["team_id"] == team_id]

            # Проверяем изменения стиля
            for i in range(1, len(team_data)):
                prev_season = team_data.iloc[i - 1]
                curr_season = team_data.iloc[i]

                if prev_season["playing_style"] != curr_season["playing_style"]:
                    style_changes.append({
                        "team_id": team_id,
                        "team_name": team_data.iloc[0]["team_name"],
                        "league_name": team_data.iloc[0]["league_name"],
                        "from_season": prev_season["season_code"],
                        "to_season": curr_season["season_code"],
                        "from_style": prev_season["playing_style"],
                        "to_style": curr_season["playing_style"],
                        "change_description": f"{prev_season['playing_style']} → {curr_season['playing_style']}"
                    })

        if style_changes:
            changes_df = pd.DataFrame(style_changes)
            logger.info(f"Найдено {len(changes_df)} изменений стилей")

            # Анализ частых переходов
            common_changes = changes_df["change_description"].value_counts().head(5)
            logger.info("Самые частые изменения стилей:")
            for change, count in common_changes.items():
                logger.info(f"  • {change}: {count} команд")

            return changes_df
        else:
            logger.info("Изменений стилей не обнаружено")
            return pd.DataFrame()

    def _save_style_analysis_results(self, analysis_results: Dict):
        """
        Сохраняет результаты анализа стилей
        Args:
            analysis_results: Результаты анализа
        """
        import os
        from datetime import datetime

        # Создаем папку для результатов
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_dir = f"data/team_styles_analysis_{timestamp}"
        os.makedirs(results_dir, exist_ok=True)

        logger.info(f"Сохранение результатов в {results_dir}")

        # 1. Сохраняем команды со стилями
        teams_path = os.path.join(results_dir, "teams_with_playing_styles.csv")
        analysis_results["teams_with_styles"].to_csv(teams_path, index=False, encoding='utf-8-sig')
        logger.info(f"✅ Команды со стилями: {teams_path}")

        # 2. Сохраняем анализ кластеров
        clusters_path = os.path.join(results_dir, "cluster_analysis.csv")
        analysis_results["cluster_analysis"].to_csv(clusters_path, index=False, encoding='utf-8-sig')
        logger.info(f"✅ Анализ кластеров: {clusters_path}")

        # 3. Сохраняем распределение по лигам
        leagues_path = os.path.join(results_dir, "league_distribution.csv")
        analysis_results["league_distribution"].to_csv(leagues_path, encoding='utf-8-sig')
        logger.info(f"✅ Распределение по лигам: {leagues_path}")

        # 4. Сохраняем изменения стилей (если есть)
        if len(analysis_results["style_changes"]) > 0:
            changes_path = os.path.join(results_dir, "style_changes.csv")
            analysis_results["style_changes"].to_csv(changes_path, index=False, encoding='utf-8-sig')
            logger.info(f"✅ Изменения стилей: {changes_path}")

        # 5. Сохраняем сводный отчет
        report_path = os.path.join(results_dir, "analysis_report.txt")
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 70 + "\n")
            f.write("АНАЛИЗ ИГРОВЫХ СТИЛЕЙ КОМАНД\n")
            f.write("=" * 70 + "\n\n")

            f.write(f"Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Всего команд проанализировано: {len(analysis_results['teams_with_styles'])}\n")
            f.write(f"Количество стилей: {analysis_results['n_clusters']}\n")
            f.write(f"Качество кластеризации (силуэт): {analysis_results['silhouette_score']:.3f}\n\n")

            f.write("СТИЛИ ИГРЫ:\n")
            f.write("-" * 50 + "\n")
            for _, row in analysis_results["cluster_analysis"].iterrows():
                f.write(f"\n{row['style_name']} (Кластер {row['cluster_id']}):\n")
                f.write(f"  • Команд: {row['team_count']} ({row['percentage']:.1f}%)\n")
                f.write(f"  • Атака: {row['avg_attacking']:.2f} гол/90 мин\n")
                f.write(f"  • Владение: {row['avg_possession']:.1f}%\n")
                f.write(f"  • Эффективность: {row['avg_efficiency']:.2f}\n")
                f.write(f"  • Примеры команд: {', '.join(row['top_teams'][:3])}\n")

        logger.info(f"✅ Отчет: {report_path}")
        logger.info(f"📁 Все результаты сохранены в папку: {os.path.abspath(results_dir)}")

    def get_team_style_recommendations(
            self,
            team_name: str,
            season: Optional[str] = None
    ) -> Dict:
        """
        Получает рекомендации по игровому стилю для конкретной команды
        Args:
            team_name: Название команды
            season: Сезон (если не указан - последний доступный)
        Returns:
            Dict: Рекомендации и анализ команды
        """
        self.initialize_spark()

        logger.info(f"🔍 Анализ рекомендаций для команды: {team_name}")

        # Пытаемся найти команду в последних результатах анализа
        if hasattr(self, 'last_style_analysis'):
            teams_df = self.last_style_analysis['teams_with_styles']

            # Фильтруем по названию команды
            team_data = teams_df[teams_df['team_name'] == team_name]

            if not team_data.empty:
                if season:
                    team_data = team_data[team_data['season_code'] == season]

                if not team_data.empty:
                    team_row = team_data.iloc[0]

                    # Получаем рекомендации
                    recommendations = self._generate_team_recommendations(team_row)

                    return {
                        'team_name': team_name,
                        'season': team_row['season_code'],
                        'league': team_row['league_name'],
                        'current_style': team_row['playing_style'],
                        'cluster': team_row['cluster'],
                        'attacking_power': float(team_row['attacking_power']),
                        'possession_control': float(team_row['possession_control']),
                        'attack_efficiency': float(team_row['attack_efficiency']),
                        'recommendations': recommendations
                    }

        logger.warning(f"Команда {team_name} не найдена в результатах анализа")
        return {}

    def _generate_team_recommendations(self, team_row: pd.Series) -> List[str]:
        """
        Генерирует рекомендации для команды на основе её метрик
        Args:
            team_row: Данные команды
        Returns:
            List[str]: Список рекомендаций
        """
        recommendations = []

        attacking = team_row['attacking_power']
        possession = team_row['possession_control']
        efficiency = team_row['attack_efficiency']
        creativity = team_row.get('creativity', 0)
        aggressiveness = team_row.get('aggressiveness', 0)

        # Анализируем атакующий потенциал
        if attacking < 1.0:
            recommendations.append("Увеличить атакующий потенциал: больше создавать опасных моментов")
        elif attacking > 1.8:
            recommendations.append("Улучшить реализацию: больше голов из созданных моментов")

        # Анализируем владение мячом
        if possession < 45:
            recommendations.append("Увеличить владение мячом: больше контроля и точных передач")
        elif possession > 60:
            recommendations.append("Сделать владение более эффективным: больше опасных действий при владении")

        # Анализируем эффективность
        if efficiency < 0.9:
            recommendations.append("Повысить эффективность атаки: улучшить качество завершения моментов")

        # Анализируем креативность
        if creativity < 0.5:
            recommendations.append("Развивать креативность: больше голевых передач и ключевых пасов")

        # Анализируем агрессивность
        if aggressiveness > 80:
            recommendations.append("Снизить агрессивность: меньше фолов и желтых карточек")
        elif aggressiveness < 40:
            recommendations.append("Увеличить агрессивность в обороне: больше прессинга")

        return recommendations


    
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

