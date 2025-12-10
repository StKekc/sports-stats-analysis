"""
Модуль для настройки Java окружения для PySpark на Windows
Должен быть импортирован ДО импорта PySpark
"""

import sys
import os
import subprocess
from pathlib import Path


def setup_java_for_spark():
    """
    Настраивает Java окружение для PySpark на Windows
    Должен быть вызван ДО импорта PySpark
    """
    if sys.platform != 'win32':
        return  # Только для Windows
    
    # Проверяем, не установлена ли уже JAVA_HOME
    if 'JAVA_HOME' in os.environ and os.path.exists(os.environ.get('JAVA_HOME', '')):
        java_home = os.environ['JAVA_HOME']
        java_exe = os.path.join(java_home, 'bin', 'java.exe')
        if os.path.exists(java_exe):
            print(f"✅ JAVA_HOME уже установлена: {java_home}")
            _add_java_to_path(java_home)
            _setup_spark_env()
            return
    
    # Ищем Java автоматически
    java_home = _find_java()
    
    if java_home:
        os.environ['JAVA_HOME'] = java_home
        print(f"✅ Найден и установлен JAVA_HOME: {java_home}")
        _add_java_to_path(java_home)
        _setup_spark_env()
    else:
        print("⚠️  ВНИМАНИЕ: Java не найдена автоматически!")
        print("   Установите Java JDK 8, 11, 17 или 21 и установите переменную окружения JAVA_HOME")
        print("   Или укажите путь вручную в коде")


def _find_java():
    """Ищет установленную Java на Windows"""
    possible_paths = [
        "C:\\Program Files\\Java\\jdk-17",
        "C:\\Program Files\\Java\\jdk-17.0.12",
        "C:\\Program Files\\Java\\jdk-17.0.11",
        "C:\\Program Files\\Java\\jdk-17.0.10",
        "C:\\Program Files\\Java\\jdk-17.0.9",
        "C:\\Program Files\\Java\\jdk-17.0.8",
        "C:\\Program Files\\Java\\jdk-17.0.7",
        "C:\\Program Files\\Java\\jdk-17.0.6",
        "C:\\Program Files\\Java\\jdk-17.0.5",
        "C:\\Program Files\\Java\\jdk-17.0.4",
        "C:\\Program Files\\Java\\jdk-17.0.3",
        "C:\\Program Files\\Java\\jdk-17.0.2",
        "C:\\Program Files\\Java\\jdk-17.0.1",
        "C:\\Program Files\\Java\\jdk-11",
        "C:\\Program Files\\Java\\jdk-21",
        "C:\\Program Files\\Java\\jdk-8",
        os.path.expandvars("%PROGRAMFILES%\\Java\\jdk-17"),
        os.path.expandvars("%PROGRAMFILES%\\Java\\jdk-11"),
        os.path.expandvars("%PROGRAMFILES%\\Java\\jdk-21"),
        os.path.expandvars("%PROGRAMFILES%\\Java\\jdk-8"),
        os.path.expandvars("%PROGRAMFILES(X86)%\\Java\\jdk-17"),
        os.path.expandvars("%PROGRAMFILES(X86)%\\Java\\jdk-11"),
        os.path.expandvars("%PROGRAMFILES(X86)%\\Java\\jdk-21"),
        os.path.expandvars("%PROGRAMFILES(X86)%\\Java\\jdk-8"),
    ]
    
    # Проверяем все подпапки в Program Files\Java
    java_base_paths = [
        os.path.expandvars("%PROGRAMFILES%\\Java"),
        os.path.expandvars("%PROGRAMFILES(X86)%\\Java"),
        "C:\\Program Files\\Java",
        "C:\\Program Files (x86)\\Java"
    ]
    
    for base_path in java_base_paths:
        if os.path.exists(base_path):
            try:
                for item in os.listdir(base_path):
                    item_path = os.path.join(base_path, item)
                    if os.path.isdir(item_path) and ('jdk' in item.lower() or 'java' in item.lower()):
                        java_exe = os.path.join(item_path, 'bin', 'java.exe')
                        if os.path.exists(java_exe):
                            possible_paths.append(item_path)
            except (PermissionError, OSError):
                pass
    
    # Проверяем пути
    for path in possible_paths:
        if os.path.exists(path):
            java_exe = os.path.join(path, 'bin', 'java.exe')
            if os.path.exists(java_exe):
                return path
    
    return None


def _add_java_to_path(java_home):
    """Добавляет Java в PATH и проверяет доступность"""
    java_bin = os.path.join(java_home, 'bin')
    java_exe = os.path.join(java_bin, 'java.exe')
    
    if os.path.exists(java_exe):
        current_path = os.environ.get('PATH', '')
        if java_bin not in current_path:
            os.environ['PATH'] = java_bin + ";" + current_path
            print(f"✅ Добавлено в PATH: {java_bin}")
        
        # Проверяем версию Java
        # Примечание: java -version выводит информацию в stderr, поэтому перенаправляем stderr в stdout
        try:
            result = subprocess.run(
                [java_exe, '-version'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                version_line = result.stdout.split('\n')[0] if result.stdout else "Unknown"
                print(f"✅ Java версия: {version_line.strip()}")
        except Exception as e:
            print(f"⚠️  Не удалось проверить версию Java: {e}")
    else:
        print(f"⚠️  ВНИМАНИЕ: java.exe не найден в {java_bin}")


def _setup_spark_env():
    """Настраивает переменные окружения для Spark на Windows"""
    # Устанавливаем PySpark для Windows
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    print(f"✅ Python установлен: {sys.executable}")
    
    # Создаем необходимые временные директории
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
            print(f"⚠️  Не удалось создать директорию {temp_dir}: {e}")
    
    # Устанавливаем переменные окружения для Hadoop/Spark на Windows
    os.environ['HADOOP_HOME'] = os.environ.get('HADOOP_HOME', '')
    os.environ['HADOOP_OPTS'] = os.environ.get('HADOOP_OPTS', '') + ' -Djava.library.path='
    os.environ['SPARK_LOCAL_DIRS'] = 'C:/temp/spark/tmp'

