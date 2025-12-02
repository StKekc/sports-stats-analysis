"""
Конфигурация Apache Spark для обработки спортивной статистики
"""

import os
from pyspark.sql import SparkSession
from typing import Dict, Optional


class SparkConfig:
    """Класс для настройки и создания Spark сессии"""
    
    # JDBC драйвер для PostgreSQL (будет скачан автоматически)
    POSTGRESQL_DRIVER = "org.postgresql.Driver"
    POSTGRESQL_JAR_PACKAGE = "org.postgresql:postgresql:42.7.1"
    
    def __init__(self, app_name: str = "SportsStatsAnalysis"):
        """
        Инициализация конфигурации Spark
        
        Args:
            app_name: Название Spark приложения
        """
        self.app_name = app_name
        self.spark_session: Optional[SparkSession] = None
    
    def create_spark_session(
        self, 
        master: str = "local[*]",
        memory: str = "4g",
        **kwargs
    ) -> SparkSession:
        """
        Создает и возвращает Spark сессию с необходимыми настройками
        
        Args:
            master: Spark master URL (default: local[*] - использовать все ядра)
            memory: Объем памяти для драйвера
            **kwargs: Дополнительные конфигурационные параметры
        
        Returns:
            SparkSession: Настроенная Spark сессия
        """
        if self.spark_session is not None:
            return self.spark_session
        
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master) \
            .config("spark.driver.memory", memory) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars.packages", self.POSTGRESQL_JAR_PACKAGE) \
            .config("spark.driver.extraClassPath", self._get_jdbc_driver_path())
        
        # Дополнительные конфигурации
        for key, value in kwargs.items():
            builder = builder.config(key, value)
        
        self.spark_session = builder.getOrCreate()
        
        # Установка уровня логирования
        self.spark_session.sparkContext.setLogLevel("WARN")
        
        return self.spark_session
    
    def _get_jdbc_driver_path(self) -> str:
        """
        Определяет путь к JDBC драйверу PostgreSQL
        
        Returns:
            str: Путь к драйверу или пустая строка
        """
        # Spark автоматически скачает драйвер через maven
        return ""
    
    def get_postgres_jdbc_url(self, db_config: Dict[str, str]) -> str:
        """
        Формирует JDBC URL для подключения к PostgreSQL
        
        Args:
            db_config: Словарь с параметрами подключения
                      (host, port, database, user, password)
        
        Returns:
            str: JDBC URL
        """
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 5432)
        database = db_config.get('database', 'sports_stats')
        
        return f"jdbc:postgresql://{host}:{port}/{database}"
    
    def get_jdbc_properties(self, db_config: Dict[str, str]) -> Dict[str, str]:
        """
        Формирует properties для JDBC подключения
        
        Args:
            db_config: Словарь с параметрами подключения
        
        Returns:
            Dict: Properties для JDBC
        """
        return {
            "user": db_config.get('user', 'postgres'),
            "password": db_config.get('password', 'postgres'),
            "driver": self.POSTGRESQL_DRIVER
        }
    
    def stop_spark_session(self):
        """Останавливает Spark сессию"""
        if self.spark_session is not None:
            self.spark_session.stop()
            self.spark_session = None
    
    def __enter__(self):
        """Context manager: создает сессию при входе"""
        return self.create_spark_session()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager: останавливает сессию при выходе"""
        self.stop_spark_session()

