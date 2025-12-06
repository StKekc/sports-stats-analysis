"""
Конфигурация Apache Spark для обработки спортивной статистики
"""

import os
import platform
from pyspark.sql import SparkSession
from typing import Dict, Optional

# Исправление для Windows: обход проблемы с Hadoop NativeIO
if platform.system() == "Windows":
    # Устанавливаем переменную окружения для обхода проблемы с winutils
    os.environ['HADOOP_HOME'] = os.environ.get('HADOOP_HOME', '')
    # Отключаем проверку NativeIO на Windows
    os.environ['HADOOP_OPTS'] = os.environ.get('HADOOP_OPTS', '') + ' -Djava.library.path='
    # Подавляем предупреждение о native-hadoop library (это нормально для Windows)
    import warnings
    import logging
    # Подавляем предупреждения PySpark о native-hadoop
    logging.getLogger("py4j").setLevel(logging.ERROR)
    # Подавляем предупреждения о native library через переменные окружения
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'


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

    def _setup_environment(self):
        """Настройка окружения для Windows"""
        import sys

        if sys.platform == 'win32':
            print("Настройка окружения Windows для Spark...")

            # Проверяем Java
            if 'JAVA_HOME' not in os.environ:
                print("⚠️ JAVA_HOME не установлена. Установите переменную окружения.")
                print("   Или добавьте: set JAVA_HOME=C:\\Program Files\\Java\\jdk-17")
                print("   Затем перезапустите терминал/IDE.")
            else:
                java_home = os.environ['JAVA_HOME']
                print(f"✅ JAVA_HOME: {java_home}")
                
                # Проверяем, что java.exe существует
                java_exe = os.path.join(java_home, 'bin', 'java.exe')
                if not os.path.exists(java_exe):
                    print(f"⚠️ ВНИМАНИЕ: java.exe не найден в {os.path.join(java_home, 'bin')}")
                    print(f"   Проверьте правильность пути JAVA_HOME: {java_home}")
                else:
                    # Добавляем Java в PATH если еще не добавлена
                    java_bin = os.path.join(java_home, 'bin')
                    current_path = os.environ.get('PATH', '')
                    if java_bin not in current_path:
                        os.environ['PATH'] = java_bin + ";" + current_path
                        print(f"✅ Java добавлена в PATH: {java_bin}")

            # Устанавливаем временные директории
            temp_dirs = [
                "C:/temp/spark-warehouse",
                "C:/temp/spark-events",
                "C:/temp/spark/tmp",
                "C:/temp/hadoop/tmp"
            ]

            for temp_dir in temp_dirs:
                try:
                    os.makedirs(temp_dir, exist_ok=True)
                except Exception as e:
                    print(f"⚠️ Не удалось создать директорию {temp_dir}: {e}")

            # Устанавливаем переменные окружения для Hadoop/Spark на Windows
            os.environ['HADOOP_HOME'] = os.environ.get('HADOOP_HOME', '')
            os.environ['HADOOP_OPTS'] = os.environ.get('HADOOP_OPTS', '') + ' -Djava.library.path='
            os.environ['SPARK_LOCAL_DIRS'] = 'C:/temp/spark/tmp'
            
            # Устанавливаем PySpark переменные
            os.environ['PYSPARK_PYTHON'] = sys.executable
            os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    def _verify_java_setup(self):
        """Проверяет настройку Java перед созданием Spark сессии"""
        import sys
        import subprocess
        
        if sys.platform == 'win32':
            if 'JAVA_HOME' not in os.environ:
                raise RuntimeError(
                    "JAVA_HOME не установлена! Установите Java JDK и установите переменную окружения JAVA_HOME.\n"
                    "Например: set JAVA_HOME=C:\\Program Files\\Java\\jdk-17"
                )
            
            java_home = os.environ['JAVA_HOME']
            java_exe = os.path.join(java_home, 'bin', 'java.exe')
            
            if not os.path.exists(java_exe):
                raise RuntimeError(
                    f"java.exe не найден в {os.path.join(java_home, 'bin')}\n"
                    f"Проверьте правильность пути JAVA_HOME: {java_home}"
                )
            
            # Проверяем, что Java работает
            # Примечание: java -version выводит информацию в stderr, поэтому перенаправляем stderr в stdout
            try:
                result = subprocess.run(
                    [java_exe, '-version'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=5
                )
                if result.returncode != 0:
                    raise RuntimeError(f"Java не работает корректно. Код возврата: {result.returncode}")
            except subprocess.TimeoutExpired:
                raise RuntimeError("Таймаут при проверке Java. Java может быть недоступна.")
            except Exception as e:
                raise RuntimeError(f"Ошибка при проверке Java: {e}")

    def create_spark_session(
            self,
            master: str = "local[*]",
            memory: str = "4g",
            **kwargs
    ) -> SparkSession:
        # Для Windows используем более консервативные настройки по умолчанию
        if platform.system() == "Windows":
            if master == "local[*]":
                master = "local[1]"  # Используем только 1 ядро для стабильности
            if memory in ["4g", "2g"]:
                memory = "1g"  # Уменьшаем память для Windows до минимума
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

        # Настраиваем окружение ПЕРЕД созданием сессии
        self._setup_environment()
        
        # Дополнительная проверка Java для Windows (мягкая проверка - только предупреждения)
        if platform.system() == "Windows":
            try:
                self._verify_java_setup()
                # Дополнительная диагностика
                if 'JAVA_HOME' in os.environ:
                    java_home = os.environ['JAVA_HOME']
                    java_exe = os.path.join(java_home, 'bin', 'java.exe')
                    if os.path.exists(java_exe):
                        print(f"✅ Java готова к использованию: {java_exe}")
                    else:
                        print(f"⚠️  java.exe не найден в {os.path.join(java_home, 'bin')}")
            except RuntimeError as e:
                # Не прерываем выполнение, только предупреждаем
                import warnings
                warnings.warn(f"Проблема с настройкой Java: {e}. Spark может не работать корректно.", RuntimeWarning)
                print(f"⚠️  Предупреждение: {e}")
                print(f"   Попытка продолжить без проверки Java...")

        # Базовые настройки
        spark_config = {
            "spark.app.name": self.app_name,
            "spark.master": master,
            "spark.driver.memory": memory,
            "spark.jars.packages": self.POSTGRESQL_JAR_PACKAGE,
            "spark.driver.extraClassPath": self._get_jdbc_driver_path(),

            # Общие настройки для производительности
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
           # "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
           # "spark.kryo.registrator": "org.apache.spark.serializer.KryoRegistrator",
        }
        
        # Адаптивное выполнение только для не-Windows систем
        if platform.system() != "Windows":
            spark_config.update({
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            })

        # НАСТРОЙКИ ДЛЯ WINDOWS И RDD ОПЕРАЦИЙ
        if platform.system() == "Windows":
            # Убеждаемся, что память установлена правильно
            spark_config["spark.driver.memory"] = memory
            
            spark_config.update({
                # Критичные настройки для Windows
                "spark.python.worker.reuse": "false",  # Отключаем повторное использование воркеров
                "spark.python.worker.timeout": "600",  # Увеличиваем таймаут воркеров (секунды)
                "spark.executor.heartbeatInterval": "60s",  # Увеличиваем интервал heartbeat
                "spark.network.timeout": "900s",  # Увеличиваем сетевой таймаут
                "spark.rpc.message.maxSize": "512",  # Уменьшаем максимальный размер сообщений
                "spark.rpc.askTimeout": "600s",  # Увеличиваем таймаут RPC
                "spark.rpc.lookupTimeout": "300s",  # Увеличиваем таймаут поиска RPC
                
                # Настройки для стабильности Java gateway на Windows
                "spark.driver.host": "localhost",
                "spark.driver.bindAddress": "127.0.0.1",
                "spark.driver.port": "0",  # Автоматический выбор порта
                "spark.blockManager.port": "0",  # Автоматический выбор порта
                "spark.ui.port": "0",  # Отключаем UI для уменьшения нагрузки
                "spark.ui.enabled": "false",  # Отключаем UI
                
                # Критичные настройки для Java gateway - минимальные требования
                "spark.driver.maxResultSize": "512m",  # Ограничиваем размер результатов
                # Настройки GC (без параметров памяти - они задаются через spark.driver.memory)
                "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200",
                # Для executor не используем extraJavaOptions, так как в local режиме executor не используется
                
                # Отключаем ненужные функции для уменьшения нагрузки
                "spark.sql.adaptive.enabled": "false",  # Отключаем адаптивное выполнение для простоты
                "spark.sql.adaptive.coalescePartitions.enabled": "false",
                
                # Настройки для работы с файловой системой Windows
                "spark.sql.warehouse.dir": "file:///C:/temp/spark-warehouse",
                "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
                "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored": "true",
                "spark.cleaner.referenceTracking.cleanCheckpoints": "false",  # Отключаем очистку checkpoint
                "spark.local.dir": "C:/temp/spark/tmp",  # Явно указываем директорию для временных файлов

                # Оптимизации для Windows
                "spark.io.compression.codec": "snappy",
                "spark.shuffle.compress": "true",
                "spark.shuffle.spill.compress": "true",
            })
        else:
            # Настройки для Linux/Mac
            spark_config.update({
                "spark.python.worker.reuse": "true",
                "spark.python.worker.timeout": "60",
                "spark.network.timeout": "120s",
            })

        # Настройки для RDD операций (особенно важны для задачи 4)
        spark_config.update({
            "spark.default.parallelism": "4",  # Уменьшаем параллелизм для Windows
            "spark.sql.shuffle.partitions": "4",  # Уменьшаем количество партиций
            "spark.rdd.compress": "true",  # Сжимаем RDD данные
            "spark.shuffle.file.buffer": "1mb",  # Увеличиваем буфер shuffle
            "spark.reducer.maxSizeInFlight": "96mb",  # Увеличиваем размер данных в полете
        })

        # Создаем builder с конфигурацией
        builder = SparkSession.builder

        # Применяем все конфигурации
        for key, value in spark_config.items():
            builder = builder.config(key, value)

        # Дополнительные конфигурации из kwargs
        for key, value in kwargs.items():
            builder = builder.config(key, value)

        # Создаем сессию с обработкой ошибок
        try:
            # Подавляем предупреждения о native-hadoop library на Windows
            if platform.system() == "Windows":
                import warnings
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message=".*native-hadoop.*")
                    warnings.filterwarnings("ignore", message=".*Unable to load native-hadoop.*")
                    self.spark_session = builder.getOrCreate()
            else:
                self.spark_session = builder.getOrCreate()
            
            # Установка уровня логирования
            self.spark_session.sparkContext.setLogLevel("WARN")
            # Подавляем предупреждения о native library в логах Spark
            if platform.system() == "Windows":
                import logging
                logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
                logging.getLogger("org.apache.hadoop").setLevel(logging.ERROR)
            print("✅ Spark сессия успешно создана")
        except Exception as e:
            error_msg = str(e)
            # Если не удалось создать с текущими настройками, пробуем минимальную конфигурацию
            if platform.system() == "Windows" and ("JAVA_GATEWAY_EXITED" in error_msg or "Java gateway" in error_msg):
                print("\n⚠️  Попытка создать Spark сессию с минимальными настройками...")
                print("   (Это может занять несколько секунд)")
                try:
                    # Минимальная конфигурация для Windows - абсолютный минимум
                    minimal_config = {
                        "spark.app.name": self.app_name,
                        "spark.master": "local[1]",
                        "spark.driver.memory": "512m",
                        "spark.driver.maxResultSize": "256m",
                        "spark.ui.enabled": "false",
                        "spark.sql.adaptive.enabled": "false",
                        "spark.jars.packages": self.POSTGRESQL_JAR_PACKAGE,
                        # Дополнительные настройки для стабильности
                        "spark.driver.host": "127.0.0.1",
                        "spark.driver.bindAddress": "127.0.0.1",
                        "spark.driver.port": "0",
                        "spark.blockManager.port": "0",
                        "spark.network.timeout": "600s",
                        "spark.python.worker.reuse": "false",
                        "spark.python.worker.timeout": "300",
                    }
                    minimal_builder = SparkSession.builder
                    for key, value in minimal_config.items():
                        minimal_builder = minimal_builder.config(key, value)
                    
                    # Подавляем предупреждения при создании минимальной сессии
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        self.spark_session = minimal_builder.getOrCreate()
                    
                    self.spark_session.sparkContext.setLogLevel("ERROR")  # Минимальное логирование
                    print("✅ Spark сессия создана с минимальными настройками")
                except Exception as e2:
                    # Если и минимальная конфигурация не работает, выбрасываем исходную ошибку с диагностикой
                    print(f"\n❌ Не удалось создать Spark сессию даже с минимальными настройками")
                    print(f"   Ошибка: {error_msg}")
                    # Дополнительная диагностика перед выбросом ошибки
                    print("\n🔍 Диагностика:")
                    if 'JAVA_HOME' in os.environ:
                        java_home = os.environ['JAVA_HOME']
                        java_exe = os.path.join(java_home, 'bin', 'java.exe')
                        print(f"   JAVA_HOME: {java_home}")
                        print(f"   java.exe существует: {os.path.exists(java_exe)}")
                        if os.path.exists(java_exe):
                            # Проверяем версию Java
                            try:
                                import subprocess
                                result = subprocess.run(
                                    [java_exe, '-version'],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    text=True,
                                    timeout=5
                                )
                                if result.returncode == 0:
                                    version = result.stdout.split('\n')[0] if result.stdout else "Unknown"
                                    print(f"   Java версия: {version.strip()}")
                            except:
                                pass
                    else:
                        print("   ⚠️  JAVA_HOME не установлена!")
                    print("\n💡 Рекомендации:")
                    print("   1. Убедитесь, что Java JDK 8, 11, 17 или 21 установлена")
                    print("   2. Установите переменную окружения JAVA_HOME")
                    print("   3. Перезапустите терминал/IDE после установки Java")
                    print("   4. Попробуйте уменьшить память: --memory 1g")
                    print("   5. Закройте другие приложения, использующие Java")
                    raise e
            else:
                raise

        # Дополнительная настройка для Windows после создания сессии
        if platform.system() == "Windows":
            # Устанавливаем переменные окружения для правильной работы с файлами
            import sys
            os.environ['PYSPARK_PYTHON'] = sys.executable
            os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

            # Создаем временные директории если их нет
            temp_dirs = [
                "C:/temp/spark-warehouse",
                "C:/temp/spark-events",
                "C:/temp/hadoop/tmp"
            ]

            for temp_dir in temp_dirs:
                os.makedirs(temp_dir, exist_ok=True)

        return self.spark_session

    def create_spark_session_for_rdd(
            self,
            master: str = "local[2]",  # Уменьшаем количество ядер для RDD операций
            memory: str = "2g",  # Уменьшаем память для RDD
            **kwargs
    ) -> SparkSession:
        """
        Создает специальную Spark сессию для RDD операций (Задача 4)

        Args:
            master: Spark master URL (меньше ядер для стабильности)
            memory: Объем памяти для драйвера
            **kwargs: Дополнительные параметры

        Returns:
            SparkSession: Сессия оптимизированная для RDD
        """
        # Дополнительные настройки для RDD
        rdd_kwargs = {
            "spark.python.worker.reuse": "false",
            "spark.python.worker.timeout": "600",  # Еще больше для RDD
            "spark.network.timeout": "900s",  # Больше таймаут
            "spark.default.parallelism": "2",  # Минимум партиций
            "spark.sql.shuffle.partitions": "2",  # Минимум партиций
            "spark.locality.wait": "3s",  # Увеличиваем время ожидания
        }

        # Объединяем с пользовательскими настройками
        rdd_kwargs.update(kwargs)

        return self.create_spark_session(master=master, memory=memory, **rdd_kwargs)

    def configure_for_windows_rdd(self):
        """
        Применяет дополнительные настройки для RDD операций на Windows
        Вызывайте этот метод перед выполнением RDD операций
        """
        if platform.system() == "Windows" and self.spark_session is not None:
            try:
                # Не все настройки можно изменить после создания сессии
                # Эти настройки должны быть в create_spark_session
                pass
            except Exception as e:
                import warnings
                warnings.warn(f"Не удалось применить все настройки RDD: {e}")
    
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
            try:
                # На Windows подавляем ошибки при удалении временных файлов
                if platform.system() == "Windows":
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        # Пытаемся корректно остановить сессию
                        try:
                            self.spark_session.stop()
                        except Exception as e:
                            error_msg = str(e).lower()
                            # Игнорируем ошибки удаления временных файлов на Windows
                            if any(keyword in error_msg for keyword in [
                                "delete", "temp", "temporary", "cleanup", 
                                "unable to delete", "exception while deleting"
                            ]):
                                # Это нормально для Windows, просто игнорируем
                                pass
                            else:
                                # Другие ошибки логируем, но не прерываем выполнение
                                import logging
                                logger = logging.getLogger(__name__)
                                logger.debug(f"Предупреждение при остановке Spark сессии: {e}")
                else:
                    # Для не-Windows систем обычная остановка
                    self.spark_session.stop()
            except Exception as e:
                # Игнорируем все ошибки при закрытии (временные файлы могут быть заблокированы)
                import logging
                logger = logging.getLogger(__name__)
                logger.debug(f"Предупреждение при остановке Spark сессии: {e}")
            finally:
                self.spark_session = None
    
    def __enter__(self):
        """Context manager: создает сессию при входе"""
        return self.create_spark_session()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager: останавливает сессию при выходе"""
        self.stop_spark_session()

