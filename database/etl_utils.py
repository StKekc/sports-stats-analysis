"""
Утилиты для ETL процесса
"""

import re
import pandas as pd
import numpy as np
from typing import Any, Optional, Dict, List
from datetime import datetime, time
import logging


class DataCleaner:
    """Класс для очистки и нормализации данных"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: Конфигурация с настройками валидации
        """
        self.config = config
        self.null_values = config.get('special_values', {}).get('null_values', [])
        self.replacements = config.get('special_values', {}).get('replacements', {})
        self.logger = logging.getLogger(__name__)
    
    def clean_value(self, value: Any) -> Optional[Any]:
        """
        Очистить значение от специальных символов и NULL
        
        Args:
            value: Значение для очистки
        
        Returns:
            Очищенное значение или None
        """
        # Если уже None или NaN
        if pd.isna(value) or value is None:
            return None
        
        # Преобразовать в строку для проверки
        str_value = str(value).strip()
        
        # Проверка на пустые значения
        if str_value in self.null_values or str_value == '':
            return None
        
        # Замена специальных значений
        if str_value in self.replacements:
            return self.replacements[str_value]
        
        return value
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Очистить весь DataFrame
        
        Args:
            df: DataFrame для очистки
        
        Returns:
            Очищенный DataFrame
        """
        df = df.copy()
        
        # Заменить пустые строки на NaN
        df.replace(self.null_values, np.nan, inplace=True)
        
        # Заменить специальные значения
        for old, new in self.replacements.items():
            df.replace(old, new, inplace=True)
        
        # Удалить лишние пробелы из строковых колонок
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
        
        return df
    
    @staticmethod
    def normalize_team_name(name: str) -> str:
        """
        Нормализовать имя команды для избежания дубликатов
        
        Args:
            name: Имя команды
        
        Returns:
            Нормализованное имя (lowercase, без лишних пробелов)
        """
        if not name:
            return ""
        
        # Удалить лишние пробелы и привести к нижнему регистру
        normalized = ' '.join(name.strip().split()).lower()
        
        # Удалить специальные символы (опционально)
        # normalized = re.sub(r'[^\w\s-]', '', normalized)
        
        return normalized
    
    @staticmethod
    def parse_score(score: str) -> tuple[Optional[int], Optional[int]]:
        """
        Распарсить счет матча (например, "4-1" -> (4, 1))
        
        Args:
            score: Строка со счетом
        
        Returns:
            Кортеж (home_goals, away_goals) или (None, None)
        """
        if not score or pd.isna(score):
            return None, None
        
        # Поддержка разных форматов разделителей
        score = str(score).strip()
        patterns = [
            r'(\d+)\s*[-–—]\s*(\d+)',  # 4-1, 4–1, 4—1
            r'(\d+)\s*:\s*(\d+)',       # 4:1
        ]
        
        for pattern in patterns:
            match = re.match(pattern, score)
            if match:
                try:
                    return int(match.group(1)), int(match.group(2))
                except ValueError:
                    pass
        
        return None, None
    
    @staticmethod
    def parse_nation_code(nation: str) -> Optional[str]:
        """
        Извлечь код страны из строки (например, "eng ENG" -> "ENG")
        
        Args:
            nation: Строка с кодом страны
        
        Returns:
            Код страны (3 буквы) или None
        """
        if not nation or pd.isna(nation):
            return None
        
        nation = str(nation).strip()
        
        # Найти 3-буквенный код (обычно в верхнем регистре в конце)
        match = re.search(r'\b([A-Z]{3})\b', nation)
        if match:
            return match.group(1)
        
        # Если не найдено, вернуть последние 3 символа (если длина подходит)
        if len(nation) >= 3:
            return nation[-3:].upper()
        
        return None
    
    @staticmethod
    def parse_date(date_str: str) -> Optional[datetime]:
        """
        Распарсить дату из разных форматов
        
        Args:
            date_str: Строка с датой
        
        Returns:
            Объект datetime или None
        """
        if not date_str or pd.isna(date_str):
            return None
        
        date_str = str(date_str).strip()
        
        # Список форматов для попытки парсинга
        formats = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%Y/%m/%d',
            '%d.%m.%Y',
            '%Y.%m.%d',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Попытка использовать pandas
        try:
            return pd.to_datetime(date_str)
        except:
            pass
        
        return None
    
    @staticmethod
    def parse_time(time_str: str) -> Optional[time]:
        """
        Распарсить время матча
        
        Args:
            time_str: Строка со временем (например, "20:00", "20:00 (21:00)")
        
        Returns:
            Объект time или None
        """
        if not time_str or pd.isna(time_str):
            return None
        
        time_str = str(time_str).strip()
        
        # Убрать информацию в скобках
        time_str = re.sub(r'\s*\([^)]*\)', '', time_str)
        
        # Форматы времени
        formats = ['%H:%M', '%H:%M:%S']
        
        for fmt in formats:
            try:
                dt = datetime.strptime(time_str, fmt)
                return dt.time()
            except ValueError:
                continue
        
        return None
    
    @staticmethod
    def convert_to_numeric(value: Any) -> Optional[float]:
        """
        Конвертировать значение в число
        
        Args:
            value: Значение для конвертации
        
        Returns:
            Число или None
        """
        if pd.isna(value) or value is None:
            return None
        
        try:
            # Удалить запятые (для больших чисел вроде "1,000")
            if isinstance(value, str):
                value = value.replace(',', '')
            
            return float(value)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def convert_to_int(value: Any) -> Optional[int]:
        """
        Конвертировать значение в целое число
        
        Args:
            value: Значение для конвертации
        
        Returns:
            Целое число или None
        """
        numeric = DataCleaner.convert_to_numeric(value)
        if numeric is not None:
            return int(numeric)
        return None
    
    def validate_value(self, value: Any, value_type: str, min_val: Optional[float] = None,
                      max_val: Optional[float] = None) -> bool:
        """
        Валидировать значение
        
        Args:
            value: Значение для валидации
            value_type: Тип значения ('int', 'float', 'date', 'pct')
            min_val: Минимальное допустимое значение
            max_val: Максимальное допустимое значение
        
        Returns:
            True если валидно
        """
        if pd.isna(value) or value is None:
            return True  # NULL допустим
        
        try:
            if value_type in ['int', 'float']:
                numeric = float(value)
                
                # Проверка отрицательных значений (если включено)
                if self.config.get('validation', {}).get('check_negative_stats', True):
                    if numeric < 0:
                        self.logger.warning(f"Negative value detected: {numeric}")
                        return False
                
                # Проверка диапазона
                if min_val is not None and numeric < min_val:
                    return False
                if max_val is not None and numeric > max_val:
                    return False
            
            elif value_type == 'date':
                date_obj = self.parse_date(str(value))
                if date_obj is None:
                    return False
                
                # Проверка года
                min_year = self.config.get('validation', {}).get('min_year', 2000)
                max_year = self.config.get('validation', {}).get('max_year', 2030)
                
                if not (min_year <= date_obj.year <= max_year):
                    self.logger.warning(f"Date out of range: {date_obj}")
                    return False
            
            elif value_type == 'pct':
                pct = float(value)
                if not (0 <= pct <= 100):
                    self.logger.warning(f"Percentage out of range: {pct}")
                    return False
            
            return True
        
        except Exception as e:
            self.logger.error(f"Validation error for value {value}: {e}")
            return False


class DirectoryParser:
    """Парсер имен директорий для извлечения лиги и сезона"""
    
    @staticmethod
    def parse_directory_name(dir_name: str) -> tuple[Optional[str], Optional[str]]:
        """
        Распарсить имя директории для получения лиги и сезона
        
        Args:
            dir_name: Имя директории (например, "epl_2019-2020")
        
        Returns:
            Кортеж (league_code, season_code) или (None, None)
        """
        if not dir_name:
            return None, None
        
        # Паттерн: league_season
        pattern = r'^([a-z0-9]+)_(.+)$'
        match = re.match(pattern, dir_name.lower())
        
        if match:
            league_code = match.group(1)
            season_code = match.group(2)
            return league_code, season_code
        
        return None, None
    
    @staticmethod
    def extract_season_years(season_code: str) -> tuple[Optional[int], Optional[int]]:
        """
        Извлечь года из кода сезона
        
        Args:
            season_code: Код сезона (например, "2019-2020" или "2024")
        
        Returns:
            Кортеж (start_year, end_year)
        """
        if not season_code:
            return None, None
        
        # Паттерн: YYYY-YYYY
        match = re.match(r'(\d{4})-(\d{4})', season_code)
        if match:
            return int(match.group(1)), int(match.group(2))
        
        # Паттерн: YYYY (один год - для лиг с календарным годом)
        match = re.match(r'(\d{4})', season_code)
        if match:
            year = int(match.group(1))
            return year, year
        
        return None, None


class FieldMapper:
    """Маппер полей из CSV в поля БД"""
    
    def __init__(self, mappings: Dict[str, Dict[str, str]]):
        """
        Args:
            mappings: Словарь маппингов из конфигурации
        """
        self.mappings = mappings
    
    def map_fields(self, data: pd.DataFrame, mapping_key: str) -> pd.DataFrame:
        """
        Переименовать колонки DataFrame согласно маппингу
        
        Args:
            data: DataFrame с исходными данными
            mapping_key: Ключ маппинга из конфигурации
        
        Returns:
            DataFrame с переименованными колонками
        """
        if mapping_key not in self.mappings:
            return data
        
        mapping = self.mappings[mapping_key]
        
        # Переименовать только существующие колонки
        rename_dict = {old: new for old, new in mapping.items() if old in data.columns}
        
        return data.rename(columns=rename_dict)
    
    def get_db_field_name(self, csv_field: str, mapping_key: str) -> str:
        """
        Получить имя поля БД по имени поля CSV
        
        Args:
            csv_field: Имя поля в CSV
            mapping_key: Ключ маппинга
        
        Returns:
            Имя поля в БД
        """
        if mapping_key in self.mappings:
            return self.mappings[mapping_key].get(csv_field, csv_field)
        return csv_field


class DataValidator:
    """Валидатор данных перед вставкой в БД"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def validate_match_data(self, match: Dict[str, Any]) -> bool:
        """Валидировать данные матча"""
        # Проверка обязательных полей
        required = ['match_date', 'home_team_id', 'away_team_id']
        for field in required:
            if field not in match or match[field] is None:
                self.logger.error(f"Missing required field: {field}")
                return False
        
        # Команды не должны быть одинаковыми
        if match['home_team_id'] == match['away_team_id']:
            self.logger.error("Home and away teams are the same")
            return False
        
        # Валидация голов
        if match.get('home_goals') is not None:
            max_goals = self.config.get('validation', {}).get('max_goals_per_match', 20)
            if match['home_goals'] > max_goals or match['away_goals'] > max_goals:
                self.logger.warning(f"Unusually high goals: {match['home_goals']}-{match['away_goals']}")
        
        return True
    
    def validate_player_data(self, player: Dict[str, Any]) -> bool:
        """Валидировать данные игрока"""
        # Имя игрока обязательно
        if not player.get('player_name'):
            self.logger.error("Player name is missing")
            return False
        
        # Валидация года рождения
        if player.get('born'):
            min_year = self.config.get('validation', {}).get('min_birth_year', 1950)
            max_year = datetime.now().year
            if not (min_year <= player['born'] <= max_year):
                self.logger.warning(f"Invalid birth year: {player['born']}")
                return False
        
        return True

