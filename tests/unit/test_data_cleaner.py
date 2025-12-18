"""
Тесты для DataCleaner из etl_utils.py
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, time
from database.etl_utils import DataCleaner


@pytest.fixture
def config():
    """Конфигурация для тестов"""
    return {
        'special_values': {
            'null_values': ['', 'N/A', 'NULL', 'None', '-'],
            'replacements': {
                'TBD': 'To Be Determined',
                'n/a': None
            }
        },
        'validation': {
            'check_negative_stats': True,
            'min_year': 2000,
            'max_year': 2030,
            'min_birth_year': 1950,
            'max_goals_per_match': 20
        }
    }


@pytest.fixture
def cleaner(config):
    """Фикстура DataCleaner"""
    return DataCleaner(config)


class TestCleanValue:
    """Тесты для метода clean_value"""
    
    def test_clean_none_returns_none(self, cleaner):
        """Тест: None должен возвращать None"""
        assert cleaner.clean_value(None) is None
    
    def test_clean_nan_returns_none(self, cleaner):
        """Тест: NaN должен возвращать None"""
        assert cleaner.clean_value(np.nan) is None
    
    def test_clean_empty_string_returns_none(self, cleaner):
        """Тест: пустая строка должна возвращать None"""
        assert cleaner.clean_value('') is None
    
    def test_clean_whitespace_string_returns_none(self, cleaner):
        """Тест: строка с пробелами должна возвращать None"""
        assert cleaner.clean_value('   ') is None
    
    def test_clean_null_value_returns_none(self, cleaner):
        """Тест: значения из null_values должны возвращать None"""
        assert cleaner.clean_value('N/A') is None
        assert cleaner.clean_value('NULL') is None
        assert cleaner.clean_value('-') is None
    
    def test_clean_replacement_value(self, cleaner):
        """Тест: значения из replacements должны заменяться"""
        assert cleaner.clean_value('TBD') == 'To Be Determined'
        assert cleaner.clean_value('n/a') is None
    
    def test_clean_valid_string_unchanged(self, cleaner):
        """Тест: обычные строки не изменяются"""
        assert cleaner.clean_value('Arsenal') == 'Arsenal'
    
    def test_clean_number_unchanged(self, cleaner):
        """Тест: числа не изменяются"""
        assert cleaner.clean_value(42) == 42
        assert cleaner.clean_value(3.14) == 3.14


class TestCleanDataFrame:
    """Тесты для метода clean_dataframe"""
    
    def test_clean_empty_dataframe(self, cleaner):
        """Тест: очистка пустого DataFrame"""
        df = pd.DataFrame()
        result = cleaner.clean_dataframe(df)
        assert result.empty
    
    def test_replace_null_values(self, cleaner):
        """Тест: замена null values на NaN"""
        df = pd.DataFrame({
            'col1': ['Arsenal', 'N/A', 'Chelsea'],
            'col2': [1, 2, 3]
        })
        result = cleaner.clean_dataframe(df)
        assert pd.isna(result.loc[1, 'col1'])
    
    def test_strip_whitespace_from_strings(self, cleaner):
        """Тест: удаление пробелов из строк"""
        df = pd.DataFrame({
            'team': ['  Arsenal  ', 'Chelsea ', ' Liverpool']
        })
        result = cleaner.clean_dataframe(df)
        assert result['team'].iloc[0] == 'Arsenal'
        assert result['team'].iloc[1] == 'Chelsea'
        assert result['team'].iloc[2] == 'Liverpool'
    
    def test_dataframe_copy_not_modified(self, cleaner):
        """Тест: исходный DataFrame не изменяется"""
        df = pd.DataFrame({'col': ['  test  ']})
        original_value = df['col'].iloc[0]
        cleaner.clean_dataframe(df)
        assert df['col'].iloc[0] == original_value


class TestNormalizeTeamName:
    """Тесты для статического метода normalize_team_name"""
    
    def test_normalize_empty_string(self):
        """Тест: пустая строка возвращает пустую строку"""
        assert DataCleaner.normalize_team_name('') == ''
    
    def test_normalize_none(self):
        """Тест: None возвращает пустую строку"""
        assert DataCleaner.normalize_team_name(None) == ''
    
    def test_normalize_to_lowercase(self):
        """Тест: приведение к нижнему регистру"""
        assert DataCleaner.normalize_team_name('ARSENAL') == 'arsenal'
        assert DataCleaner.normalize_team_name('Chelsea') == 'chelsea'
    
    def test_normalize_removes_extra_spaces(self):
        """Тест: удаление лишних пробелов"""
        assert DataCleaner.normalize_team_name('  Arsenal  ') == 'arsenal'
        assert DataCleaner.normalize_team_name('Manchester  United') == 'manchester united'
    
    def test_normalize_multiple_spaces(self):
        """Тест: множественные пробелы схлопываются"""
        assert DataCleaner.normalize_team_name('Man    City') == 'man city'


class TestParseScore:
    """Тесты для статического метода parse_score"""
    
    def test_parse_none_returns_none_none(self):
        """Тест: None возвращает (None, None)"""
        assert DataCleaner.parse_score(None) == (None, None)
    
    def test_parse_empty_string_returns_none_none(self):
        """Тест: пустая строка возвращает (None, None)"""
        assert DataCleaner.parse_score('') == (None, None)
    
    def test_parse_nan_returns_none_none(self):
        """Тест: NaN возвращает (None, None)"""
        assert DataCleaner.parse_score(np.nan) == (None, None)
    
    def test_parse_score_with_hyphen(self):
        """Тест: парсинг счета с дефисом"""
        assert DataCleaner.parse_score('4-1') == (4, 1)
        assert DataCleaner.parse_score('0-0') == (0, 0)
    
    def test_parse_score_with_spaces(self):
        """Тест: парсинг счета с пробелами"""
        assert DataCleaner.parse_score('4 - 1') == (4, 1)
        assert DataCleaner.parse_score('  3  -  2  ') == (3, 2)
    
    def test_parse_score_with_colon(self):
        """Тест: парсинг счета с двоеточием"""
        assert DataCleaner.parse_score('4:1') == (4, 1)
        assert DataCleaner.parse_score('2 : 3') == (2, 3)
    
    def test_parse_score_with_different_dashes(self):
        """Тест: парсинг счета с различными типами дефисов"""
        assert DataCleaner.parse_score('4–1') == (4, 1)  # en dash
        assert DataCleaner.parse_score('4—1') == (4, 1)  # em dash
    
    def test_parse_invalid_score_returns_none_none(self):
        """Тест: неверный формат возвращает (None, None)"""
        assert DataCleaner.parse_score('invalid') == (None, None)
        assert DataCleaner.parse_score('4-') == (None, None)
        assert DataCleaner.parse_score('abc') == (None, None)


class TestParseNationCode:
    """Тесты для статического метода parse_nation_code"""
    
    def test_parse_none_returns_none(self):
        """Тест: None возвращает None"""
        assert DataCleaner.parse_nation_code(None) is None
    
    def test_parse_empty_string_returns_none(self):
        """Тест: пустая строка возвращает None"""
        assert DataCleaner.parse_nation_code('') is None
    
    def test_parse_nan_returns_none(self):
        """Тест: NaN возвращает None"""
        assert DataCleaner.parse_nation_code(np.nan) is None
    
    def test_parse_three_letter_code(self):
        """Тест: извлечение 3-буквенного кода"""
        assert DataCleaner.parse_nation_code('eng ENG') == 'ENG'
        assert DataCleaner.parse_nation_code('ESP') == 'ESP'
        assert DataCleaner.parse_nation_code('FRA France') == 'FRA'
    
    def test_parse_code_at_end(self):
        """Тест: код в конце строки"""
        assert DataCleaner.parse_nation_code('england ENG') == 'ENG'
    
    def test_parse_fallback_to_last_three_chars(self):
        """Тест: если код не найден, берем последние 3 символа"""
        assert DataCleaner.parse_nation_code('abc') == 'ABC'


class TestParseDate:
    """Тесты для статического метода parse_date"""
    
    def test_parse_none_returns_none(self):
        """Тест: None возвращает None"""
        assert DataCleaner.parse_date(None) is None
    
    def test_parse_empty_string_returns_none(self):
        """Тест: пустая строка возвращает None"""
        assert DataCleaner.parse_date('') is None
    
    def test_parse_iso_format(self):
        """Тест: парсинг ISO формата YYYY-MM-DD"""
        result = DataCleaner.parse_date('2023-12-25')
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 25
    
    def test_parse_dd_mm_yyyy_slash(self):
        """Тест: парсинг формата DD/MM/YYYY"""
        result = DataCleaner.parse_date('25/12/2023')
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 25
    
    def test_parse_dd_mm_yyyy_dot(self):
        """Тест: парсинг формата DD.MM.YYYY"""
        result = DataCleaner.parse_date('25.12.2023')
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 25
    
    def test_parse_invalid_date_returns_none(self):
        """Тест: неверная дата возвращает None"""
        assert DataCleaner.parse_date('invalid') is None
        assert DataCleaner.parse_date('32/13/2023') is None


class TestParseTime:
    """Тесты для статического метода parse_time"""
    
    def test_parse_none_returns_none(self):
        """Тест: None возвращает None"""
        assert DataCleaner.parse_time(None) is None
    
    def test_parse_empty_string_returns_none(self):
        """Тест: пустая строка возвращает None"""
        assert DataCleaner.parse_time('') is None
    
    def test_parse_simple_time(self):
        """Тест: парсинг простого времени HH:MM"""
        result = DataCleaner.parse_time('20:00')
        assert result.hour == 20
        assert result.minute == 0
    
    def test_parse_time_with_seconds(self):
        """Тест: парсинг времени HH:MM:SS"""
        result = DataCleaner.parse_time('20:30:45')
        assert result.hour == 20
        assert result.minute == 30
        assert result.second == 45
    
    def test_parse_time_with_parentheses(self):
        """Тест: парсинг времени с информацией в скобках"""
        result = DataCleaner.parse_time('20:00 (21:00)')
        assert result.hour == 20
        assert result.minute == 0
    
    def test_parse_invalid_time_returns_none(self):
        """Тест: неверное время возвращает None"""
        assert DataCleaner.parse_time('invalid') is None
        assert DataCleaner.parse_time('25:00') is None


class TestConvertToNumeric:
    """Тесты для статического метода convert_to_numeric"""
    
    def test_convert_none_returns_none(self):
        """Тест: None возвращает None"""
        assert DataCleaner.convert_to_numeric(None) is None
    
    def test_convert_nan_returns_none(self):
        """Тест: NaN возвращает None"""
        assert DataCleaner.convert_to_numeric(np.nan) is None
    
    def test_convert_integer_string(self):
        """Тест: преобразование строки с целым числом"""
        assert DataCleaner.convert_to_numeric('42') == 42.0
    
    def test_convert_float_string(self):
        """Тест: преобразование строки с float"""
        assert DataCleaner.convert_to_numeric('3.14') == 3.14
    
    def test_convert_negative_number(self):
        """Тест: преобразование отрицательного числа"""
        assert DataCleaner.convert_to_numeric('-5') == -5.0
    
    def test_convert_with_comma_separator(self):
        """Тест: преобразование числа с запятой-разделителем"""
        assert DataCleaner.convert_to_numeric('1,000') == 1000.0
        assert DataCleaner.convert_to_numeric('1,000,000') == 1000000.0
    
    def test_convert_invalid_string_returns_none(self):
        """Тест: невалидная строка возвращает None"""
        assert DataCleaner.convert_to_numeric('invalid') is None
        assert DataCleaner.convert_to_numeric('abc123') is None


class TestConvertToInt:
    """Тесты для статического метода convert_to_int"""
    
    def test_convert_none_returns_none(self):
        """Тест: None возвращает None"""
        assert DataCleaner.convert_to_int(None) is None
    
    def test_convert_integer_string(self):
        """Тест: преобразование строки в int"""
        assert DataCleaner.convert_to_int('42') == 42
    
    def test_convert_float_string_to_int(self):
        """Тест: преобразование float строки в int"""
        assert DataCleaner.convert_to_int('42.7') == 42
    
    def test_convert_with_comma(self):
        """Тест: преобразование числа с запятой"""
        assert DataCleaner.convert_to_int('1,000') == 1000
    
    def test_convert_invalid_returns_none(self):
        """Тест: невалидное значение возвращает None"""
        assert DataCleaner.convert_to_int('invalid') is None


class TestValidateValue:
    """Тесты для метода validate_value"""
    
    def test_validate_none_always_valid(self, cleaner):
        """Тест: None всегда валиден"""
        assert cleaner.validate_value(None, 'int')
        assert cleaner.validate_value(None, 'float')
        assert cleaner.validate_value(None, 'date')
    
    def test_validate_nan_always_valid(self, cleaner):
        """Тест: NaN всегда валиден"""
        assert cleaner.validate_value(np.nan, 'int')
    
    def test_validate_positive_integer(self, cleaner):
        """Тест: валидация положительного целого"""
        assert cleaner.validate_value(42, 'int')
        assert cleaner.validate_value(0, 'int')
    
    def test_validate_negative_integer_invalid(self, cleaner):
        """Тест: отрицательное число невалидно при включенной проверке"""
        assert not cleaner.validate_value(-5, 'int')
    
    def test_validate_float_in_range(self, cleaner):
        """Тест: валидация float в диапазоне"""
        assert cleaner.validate_value(5.5, 'float', min_val=0, max_val=10)
    
    def test_validate_float_out_of_range(self, cleaner):
        """Тест: float вне диапазона невалиден"""
        assert not cleaner.validate_value(15, 'float', min_val=0, max_val=10)
        assert not cleaner.validate_value(-5, 'float', min_val=0, max_val=10)
    
    def test_validate_percentage_valid(self, cleaner):
        """Тест: валидация процента в диапазоне 0-100"""
        assert cleaner.validate_value(50, 'pct')
        assert cleaner.validate_value(0, 'pct')
        assert cleaner.validate_value(100, 'pct')
    
    def test_validate_percentage_invalid(self, cleaner):
        """Тест: процент вне диапазона 0-100 невалиден"""
        assert not cleaner.validate_value(150, 'pct')
        assert not cleaner.validate_value(-10, 'pct')
    
    def test_validate_date_in_range(self, cleaner):
        """Тест: валидация даты в допустимом диапазоне"""
        assert cleaner.validate_value('2023-12-25', 'date')
        assert cleaner.validate_value('2010-01-01', 'date')
    
    def test_validate_date_out_of_range(self, cleaner):
        """Тест: дата вне допустимого диапазона невалидна"""
        assert not cleaner.validate_value('1999-01-01', 'date')
        assert not cleaner.validate_value('2031-01-01', 'date')
    
    def test_validate_invalid_date_format(self, cleaner):
        """Тест: невалидный формат даты"""
        assert not cleaner.validate_value('invalid-date', 'date')



