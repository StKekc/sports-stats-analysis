"""
Модуль приема данных
Отвечает за парсинг и преобразование полученных данных в структурированный формат
"""
from .fbref_parser import (
    biggest_table,
    flatten_and_normalize_columns,
    detect_standings_and_teamstats
)

__all__ = [
    'biggest_table',
    'flatten_and_normalize_columns', 
    'detect_standings_and_teamstats'
]

