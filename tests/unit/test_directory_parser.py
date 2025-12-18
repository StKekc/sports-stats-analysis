"""
Тесты для DirectoryParser из etl_utils.py
"""

import pytest
from database.etl_utils import DirectoryParser


class TestParseDirectoryName:
    """Тесты для метода parse_directory_name"""
    
    def test_parse_none_returns_none_none(self):
        """Тест: None возвращает (None, None)"""
        assert DirectoryParser.parse_directory_name(None) == (None, None)
    
    def test_parse_empty_string_returns_none_none(self):
        """Тест: пустая строка возвращает (None, None)"""
        assert DirectoryParser.parse_directory_name('') == (None, None)
    
    def test_parse_valid_directory_name(self):
        """Тест: парсинг валидного имени директории"""
        league, season = DirectoryParser.parse_directory_name('epl_2019-2020')
        assert league == 'epl'
        assert season == '2019-2020'
    
    def test_parse_another_valid_directory(self):
        """Тест: парсинг другого валидного имени"""
        league, season = DirectoryParser.parse_directory_name('laliga_2020-2021')
        assert league == 'laliga'
        assert season == '2020-2021'
    
    def test_parse_numeric_league_code(self):
        """Тест: парсинг с числовым кодом лиги"""
        league, season = DirectoryParser.parse_directory_name('bundesliga1_2021-2022')
        assert league == 'bundesliga1'
        assert season == '2021-2022'
    
    def test_parse_single_year_season(self):
        """Тест: парсинг сезона с одним годом"""
        league, season = DirectoryParser.parse_directory_name('epl_2024')
        assert league == 'epl'
        assert season == '2024'
    
    def test_parse_uppercase_converted_to_lowercase(self):
        """Тест: верхний регистр приводится к нижнему"""
        league, season = DirectoryParser.parse_directory_name('EPL_2019-2020')
        assert league == 'epl'
        assert season == '2019-2020'
    
    def test_parse_invalid_format_returns_none_none(self):
        """Тест: невалидный формат возвращает (None, None)"""
        assert DirectoryParser.parse_directory_name('invalid') == (None, None)
        assert DirectoryParser.parse_directory_name('no-underscore') == (None, None)
    
    def test_parse_complex_season_code(self):
        """Тест: парсинг сложного кода сезона"""
        league, season = DirectoryParser.parse_directory_name('epl_2019-2020-spring')
        assert league == 'epl'
        assert season == '2019-2020-spring'


class TestExtractSeasonYears:
    """Тесты для метода extract_season_years"""
    
    def test_extract_none_returns_none_none(self):
        """Тест: None возвращает (None, None)"""
        assert DirectoryParser.extract_season_years(None) == (None, None)
    
    def test_extract_empty_string_returns_none_none(self):
        """Тест: пустая строка возвращает (None, None)"""
        assert DirectoryParser.extract_season_years('') == (None, None)
    
    def test_extract_two_years_format(self):
        """Тест: извлечение двух годов (YYYY-YYYY)"""
        start, end = DirectoryParser.extract_season_years('2019-2020')
        assert start == 2019
        assert end == 2020
    
    def test_extract_another_two_years(self):
        """Тест: извлечение других двух годов"""
        start, end = DirectoryParser.extract_season_years('2023-2024')
        assert start == 2023
        assert end == 2024
    
    def test_extract_single_year_format(self):
        """Тест: извлечение одного года (YYYY)"""
        start, end = DirectoryParser.extract_season_years('2024')
        assert start == 2024
        assert end == 2024
    
    def test_extract_another_single_year(self):
        """Тест: извлечение другого одного года"""
        start, end = DirectoryParser.extract_season_years('2020')
        assert start == 2020
        assert end == 2020
    
    def test_extract_invalid_format_returns_none_none(self):
        """Тест: невалидный формат возвращает (None, None)"""
        assert DirectoryParser.extract_season_years('invalid') == (None, None)
        assert DirectoryParser.extract_season_years('20-21') == (None, None)
    
    def test_extract_with_extra_text(self):
        """Тест: извлечение года из строки с дополнительным текстом"""
        # Должен извлечь первый найденный паттерн
        start, end = DirectoryParser.extract_season_years('2020-2021-extra')
        assert start == 2020
        assert end == 2021
    
    def test_extract_old_year(self):
        """Тест: извлечение старого года"""
        start, end = DirectoryParser.extract_season_years('2000-2001')
        assert start == 2000
        assert end == 2001
    
    def test_extract_future_year(self):
        """Тест: извлечение будущего года"""
        start, end = DirectoryParser.extract_season_years('2029-2030')
        assert start == 2029
        assert end == 2030



