"""
Модуль извлечения данных
Отвечает за загрузку данных из внешних источников
"""
from .fbref_scraper import FBrefScraper, get_page_html

__all__ = ['FBrefScraper', 'get_page_html']
