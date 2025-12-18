"""
Тесты граничных случаев и крайних значений
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime


class TestEmptyInputs:
    """Тесты для пустых входных данных"""
    
    def test_empty_string(self):
        """Тест: пустая строка"""
        s = ''
        assert len(s) == 0
        assert not s
        assert s == ''
    
    def test_empty_list(self):
        """Тест: пустой список"""
        lst = []
        assert len(lst) == 0
        assert not lst
    
    def test_empty_dict(self):
        """Тест: пустой словарь"""
        d = {}
        assert len(d) == 0
        assert not d
    
    def test_empty_dataframe(self):
        """Тест: пустой DataFrame"""
        df = pd.DataFrame()
        assert df.empty
        assert len(df) == 0
    
    def test_none_value(self):
        """Тест: None значение"""
        val = None
        assert val is None
        assert not val


class TestBoundaryValues:
    """Тесты для граничных значений"""
    
    def test_zero(self):
        """Тест: ноль"""
        assert 0 == 0
        assert not 0
        assert 0 + 1 == 1
    
    def test_negative_zero(self):
        """Тест: отрицательный ноль"""
        assert -0 == 0
    
    def test_very_large_number(self):
        """Тест: очень большое число"""
        large = 10**10
        assert large > 0
        assert large == 10000000000
    
    def test_very_small_number(self):
        """Тест: очень маленькое число"""
        small = 0.0000001
        assert small > 0
        assert small < 1
    
    def test_negative_number(self):
        """Тест: отрицательное число"""
        neg = -5
        assert neg < 0
        assert abs(neg) == 5


class TestSpecialCharacters:
    """Тесты для специальных символов"""
    
    def test_whitespace_string(self):
        """Тест: строка с пробелами"""
        s = '   '
        assert len(s) == 3
        assert s.strip() == ''
    
    def test_newline_character(self):
        """Тест: символ новой строки"""
        s = 'line1\nline2'
        lines = s.split('\n')
        assert len(lines) == 2
    
    def test_tab_character(self):
        """Тест: символ табуляции"""
        s = 'col1\tcol2'
        cols = s.split('\t')
        assert len(cols) == 2
    
    def test_special_symbols(self):
        """Тест: специальные символы"""
        s = '!@#$%^&*()'
        assert len(s) > 0
        assert '!' in s
    
    def test_unicode_characters(self):
        """Тест: Unicode символы"""
        s = 'Реал Мадрид'
        assert len(s) > 0
        assert 'Реал' in s


class TestDataTypeEdgeCases:
    """Тесты для граничных случаев типов данных"""
    
    def test_int_to_str(self):
        """Тест: преобразование int в str"""
        num = 42
        s = str(num)
        assert s == '42'
        assert isinstance(s, str)
    
    def test_str_to_int_valid(self):
        """Тест: преобразование валидной строки в int"""
        s = '42'
        num = int(s)
        assert num == 42
    
    def test_str_to_int_invalid(self):
        """Тест: преобразование невалидной строки в int"""
        with pytest.raises(ValueError):
            int('invalid')
    
    def test_float_to_int(self):
        """Тест: преобразование float в int"""
        f = 3.7
        i = int(f)
        assert i == 3
    
    def test_int_division(self):
        """Тест: целочисленное деление"""
        assert 5 // 2 == 2
        assert 5 / 2 == 2.5


class TestNullAndNaN:
    """Тесты для null и NaN значений"""
    
    def test_none_is_none(self):
        """Тест: None is None"""
        assert None is None
    
    def test_none_not_equal_none(self):
        """Тест: None == None"""
        assert None == None
    
    def test_nan_not_equal_nan(self):
        """Тест: NaN != NaN"""
        assert np.nan != np.nan
    
    def test_isnan_function(self):
        """Тест: функция isnan"""
        assert np.isnan(np.nan)
        assert not np.isnan(5)
    
    def test_pandas_isna(self):
        """Тест: pandas isna"""
        assert pd.isna(None)
        assert pd.isna(np.nan)
        assert not pd.isna(0)
    
    def test_fillna_with_zero(self):
        """Тест: заполнение NaN нулем"""
        s = pd.Series([1, np.nan, 3])
        filled = s.fillna(0)
        assert filled.iloc[1] == 0


class TestStringEdgeCases:
    """Тесты для граничных случаев строк"""
    
    def test_single_character(self):
        """Тест: одиночный символ"""
        s = 'a'
        assert len(s) == 1
        assert s[0] == 'a'
    
    def test_very_long_string(self):
        """Тест: очень длинная строка"""
        s = 'a' * 10000
        assert len(s) == 10000
    
    def test_string_with_quotes(self):
        """Тест: строка с кавычками"""
        s = "It's a test"
        assert "'" in s
    
    def test_string_concatenation(self):
        """Тест: конкатенация строк"""
        s1 = 'Hello'
        s2 = 'World'
        result = s1 + ' ' + s2
        assert result == 'Hello World'
    
    def test_string_multiplication(self):
        """Тест: умножение строк"""
        s = 'a' * 3
        assert s == 'aaa'


class TestListEdgeCases:
    """Тесты для граничных случаев списков"""
    
    def test_single_element_list(self):
        """Тест: список с одним элементом"""
        lst = [42]
        assert len(lst) == 1
        assert lst[0] == 42
    
    def test_list_negative_indexing(self):
        """Тест: отрицательная индексация"""
        lst = [1, 2, 3]
        assert lst[-1] == 3
        assert lst[-2] == 2
    
    def test_list_out_of_bounds(self):
        """Тест: выход за границы списка"""
        lst = [1, 2, 3]
        with pytest.raises(IndexError):
            _ = lst[10]
    
    def test_list_slice_beyond_length(self):
        """Тест: срез за пределами длины"""
        lst = [1, 2, 3]
        sliced = lst[1:100]
        assert sliced == [2, 3]
    
    def test_list_nested(self):
        """Тест: вложенные списки"""
        nested = [[1, 2], [3, 4]]
        assert nested[0][1] == 2


class TestDictEdgeCases:
    """Тесты для граничных случаев словарей"""
    
    def test_dict_missing_key(self):
        """Тест: отсутствующий ключ"""
        d = {'a': 1}
        with pytest.raises(KeyError):
            _ = d['b']
    
    def test_dict_get_with_default(self):
        """Тест: get с значением по умолчанию"""
        d = {'a': 1}
        assert d.get('b', 'default') == 'default'
    
    def test_dict_overwrite_value(self):
        """Тест: перезапись значения"""
        d = {'a': 1}
        d['a'] = 2
        assert d['a'] == 2
    
    def test_dict_numeric_keys(self):
        """Тест: числовые ключи"""
        d = {1: 'one', 2: 'two'}
        assert d[1] == 'one'
    
    def test_dict_tuple_keys(self):
        """Тест: кортежи как ключи"""
        d = {(1, 2): 'value'}
        assert d[(1, 2)] == 'value'


class TestDataFrameEdgeCases:
    """Тесты для граничных случаев DataFrame"""
    
    def test_single_row_dataframe(self):
        """Тест: DataFrame с одной строкой"""
        df = pd.DataFrame({'col': [1]})
        assert len(df) == 1
    
    def test_single_column_dataframe(self):
        """Тест: DataFrame с одной колонкой"""
        df = pd.DataFrame({'col': [1, 2, 3]})
        assert df.shape[1] == 1
    
    def test_dataframe_all_nan(self):
        """Тест: DataFrame со всеми NaN"""
        df = pd.DataFrame({'col': [np.nan, np.nan]})
        assert df['col'].isna().all()
    
    def test_dataframe_duplicate_columns(self):
        """Тест: DataFrame с дубликатами колонок"""
        df = pd.DataFrame([[1, 2]], columns=['a', 'a'])
        # pandas позволяет дубликаты колонок
        assert len(df.columns) == 2
    
    def test_dataframe_mixed_types(self):
        """Тест: DataFrame со смешанными типами"""
        df = pd.DataFrame({'col': [1, 'two', 3.0]})
        assert df['col'].dtype == 'object'


class TestComparisonEdgeCases:
    """Тесты для граничных случаев сравнения"""
    
    def test_compare_none_to_zero(self):
        """Тест: сравнение None и 0"""
        assert None != 0
        assert not (None == 0)
    
    def test_compare_empty_string_to_none(self):
        """Тест: сравнение пустой строки и None"""
        assert '' != None
        assert not ('' == None)
    
    def test_compare_empty_list_to_false(self):
        """Тест: пустой список и False"""
        assert [] != False
        assert not []  # but [] is falsy
    
    def test_compare_nan_with_pd_isna(self):
        """Тест: сравнение NaN через pd.isna"""
        assert pd.isna(np.nan)
        assert pd.isna(None)


class TestMathematicalEdgeCases:
    """Тесты для математических граничных случаев"""
    
    def test_division_by_one(self):
        """Тест: деление на 1"""
        assert 42 / 1 == 42
    
    def test_multiplication_by_zero(self):
        """Тест: умножение на 0"""
        assert 42 * 0 == 0
    
    def test_power_of_zero(self):
        """Тест: возведение в степень 0"""
        assert 42 ** 0 == 1
    
    def test_power_of_one(self):
        """Тест: возведение в степень 1"""
        assert 42 ** 1 == 42
    
    def test_modulo_operation(self):
        """Тест: операция остатка"""
        assert 10 % 3 == 1
        assert 9 % 3 == 0



