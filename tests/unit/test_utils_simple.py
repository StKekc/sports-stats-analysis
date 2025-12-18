"""
Простые тесты для различных утилит
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, time


class TestPandasBasics:
    """Базовые тесты для работы с pandas"""
    
    def test_dataframe_creation(self):
        """Тест: создание DataFrame"""
        df = pd.DataFrame({'col': [1, 2, 3]})
        assert len(df) == 3
    
    def test_dataframe_empty(self):
        """Тест: пустой DataFrame"""
        df = pd.DataFrame()
        assert df.empty
    
    def test_dataframe_shape(self):
        """Тест: размер DataFrame"""
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        assert df.shape == (2, 2)
    
    def test_dataframe_columns(self):
        """Тест: колонки DataFrame"""
        df = pd.DataFrame({'team': ['Arsenal'], 'points': [48]})
        assert 'team' in df.columns
        assert 'points' in df.columns
    
    def test_dataframe_iloc(self):
        """Тест: доступ по индексу"""
        df = pd.DataFrame({'col': [10, 20, 30]})
        assert df.iloc[0, 0] == 10
        assert df.iloc[2, 0] == 30
    
    def test_dataframe_loc(self):
        """Тест: доступ по имени колонки"""
        df = pd.DataFrame({'points': [48, 42]})
        assert df.loc[0, 'points'] == 48
    
    def test_dataframe_head(self):
        """Тест: первые строки"""
        df = pd.DataFrame({'col': range(100)})
        head = df.head(5)
        assert len(head) == 5
    
    def test_dataframe_tail(self):
        """Тест: последние строки"""
        df = pd.DataFrame({'col': range(100)})
        tail = df.tail(3)
        assert len(tail) == 3
    
    def test_dataframe_copy(self):
        """Тест: копирование DataFrame"""
        df1 = pd.DataFrame({'col': [1, 2, 3]})
        df2 = df1.copy()
        df2.iloc[0, 0] = 999
        assert df1.iloc[0, 0] == 1
    
    def test_dataframe_reset_index(self):
        """Тест: сброс индекса"""
        df = pd.DataFrame({'col': [1, 2, 3]}, index=[5, 10, 15])
        df_reset = df.reset_index(drop=True)
        assert df_reset.index.tolist() == [0, 1, 2]


class TestStringOperations:
    """Тесты для работы со строками"""
    
    def test_string_lower(self):
        """Тест: приведение к нижнему регистру"""
        assert 'ARSENAL'.lower() == 'arsenal'
    
    def test_string_upper(self):
        """Тест: приведение к верхнему регистру"""
        assert 'arsenal'.upper() == 'ARSENAL'
    
    def test_string_strip(self):
        """Тест: удаление пробелов"""
        assert '  Arsenal  '.strip() == 'Arsenal'
    
    def test_string_replace(self):
        """Тест: замена подстроки"""
        assert 'Man City'.replace('Man', 'Manchester') == 'Manchester City'
    
    def test_string_split(self):
        """Тест: разделение строки"""
        parts = 'Arsenal FC'.split()
        assert len(parts) == 2
        assert parts[0] == 'Arsenal'
    
    def test_string_join(self):
        """Тест: объединение строк"""
        assert ' '.join(['Manchester', 'United']) == 'Manchester United'
    
    def test_string_in_operator(self):
        """Тест: проверка вхождения"""
        assert 'Arsenal' in 'Arsenal FC'
        assert 'Chelsea' not in 'Arsenal FC'
    
    def test_string_startswith(self):
        """Тест: проверка начала строки"""
        assert 'Arsenal FC'.startswith('Arsenal')
    
    def test_string_endswith(self):
        """Тест: проверка конца строки"""
        assert 'Arsenal FC'.endswith('FC')
    
    def test_string_format(self):
        """Тест: форматирование строки"""
        team = 'Arsenal'
        result = f'{team} is a football team'
        assert result == 'Arsenal is a football team'


class TestNumberOperations:
    """Тесты для работы с числами"""
    
    def test_integer_addition(self):
        """Тест: сложение целых чисел"""
        assert 2 + 3 == 5
    
    def test_integer_subtraction(self):
        """Тест: вычитание"""
        assert 10 - 3 == 7
    
    def test_integer_multiplication(self):
        """Тест: умножение"""
        assert 4 * 5 == 20
    
    def test_integer_division(self):
        """Тест: деление"""
        assert 10 / 2 == 5.0
    
    def test_float_operations(self):
        """Тест: операции с float"""
        assert 3.5 + 2.5 == 6.0
    
    def test_round_function(self):
        """Тест: округление"""
        assert round(3.7) == 4
        assert round(3.14159, 2) == 3.14
    
    def test_abs_function(self):
        """Тест: модуль числа"""
        assert abs(-5) == 5
        assert abs(5) == 5
    
    def test_max_function(self):
        """Тест: максимум"""
        assert max(1, 5, 3) == 5
    
    def test_min_function(self):
        """Тест: минимум"""
        assert min(1, 5, 3) == 1
    
    def test_sum_function(self):
        """Тест: сумма"""
        assert sum([1, 2, 3, 4]) == 10


class TestListOperations:
    """Тесты для работы со списками"""
    
    def test_list_creation(self):
        """Тест: создание списка"""
        teams = ['Arsenal', 'Chelsea', 'Liverpool']
        assert len(teams) == 3
    
    def test_list_append(self):
        """Тест: добавление элемента"""
        teams = ['Arsenal']
        teams.append('Chelsea')
        assert len(teams) == 2
    
    def test_list_extend(self):
        """Тест: расширение списка"""
        teams1 = ['Arsenal']
        teams2 = ['Chelsea', 'Liverpool']
        teams1.extend(teams2)
        assert len(teams1) == 3
    
    def test_list_indexing(self):
        """Тест: индексация"""
        teams = ['Arsenal', 'Chelsea', 'Liverpool']
        assert teams[0] == 'Arsenal'
        assert teams[-1] == 'Liverpool'
    
    def test_list_slicing(self):
        """Тест: срезы"""
        nums = [0, 1, 2, 3, 4, 5]
        assert nums[1:4] == [1, 2, 3]
    
    def test_list_in_operator(self):
        """Тест: проверка наличия"""
        teams = ['Arsenal', 'Chelsea']
        assert 'Arsenal' in teams
        assert 'Liverpool' not in teams
    
    def test_list_count(self):
        """Тест: подсчет элементов"""
        nums = [1, 2, 2, 3, 2]
        assert nums.count(2) == 3
    
    def test_list_sort(self):
        """Тест: сортировка"""
        nums = [3, 1, 4, 1, 5]
        nums.sort()
        assert nums == [1, 1, 3, 4, 5]
    
    def test_list_reverse(self):
        """Тест: разворот списка"""
        nums = [1, 2, 3]
        nums.reverse()
        assert nums == [3, 2, 1]
    
    def test_list_comprehension(self):
        """Тест: list comprehension"""
        squares = [x**2 for x in range(5)]
        assert squares == [0, 1, 4, 9, 16]


class TestDictOperations:
    """Тесты для работы со словарями"""
    
    def test_dict_creation(self):
        """Тест: создание словаря"""
        team = {'name': 'Arsenal', 'points': 48}
        assert team['name'] == 'Arsenal'
    
    def test_dict_get(self):
        """Тест: метод get"""
        team = {'name': 'Arsenal'}
        assert team.get('name') == 'Arsenal'
        assert team.get('missing', 'default') == 'default'
    
    def test_dict_keys(self):
        """Тест: получение ключей"""
        team = {'name': 'Arsenal', 'points': 48}
        assert 'name' in team.keys()
    
    def test_dict_values(self):
        """Тест: получение значений"""
        team = {'name': 'Arsenal', 'points': 48}
        assert 48 in team.values()
    
    def test_dict_items(self):
        """Тест: получение пар ключ-значение"""
        team = {'name': 'Arsenal'}
        items = list(team.items())
        assert items[0] == ('name', 'Arsenal')
    
    def test_dict_update(self):
        """Тест: обновление словаря"""
        team = {'name': 'Arsenal'}
        team['points'] = 48
        assert team['points'] == 48
    
    def test_dict_in_operator(self):
        """Тест: проверка наличия ключа"""
        team = {'name': 'Arsenal'}
        assert 'name' in team
        assert 'missing' not in team
    
    def test_dict_len(self):
        """Тест: длина словаря"""
        team = {'name': 'Arsenal', 'points': 48, 'wins': 15}
        assert len(team) == 3


class TestNumpyOperations:
    """Тесты для работы с numpy"""
    
    def test_numpy_array_creation(self):
        """Тест: создание массива"""
        arr = np.array([1, 2, 3])
        assert len(arr) == 3
    
    def test_numpy_isnan(self):
        """Тест: проверка на NaN"""
        assert np.isnan(np.nan)
        assert not np.isnan(5)
    
    def test_numpy_nan_equality(self):
        """Тест: NaN не равен NaN"""
        assert np.nan != np.nan
        assert pd.isna(np.nan)
    
    def test_numpy_mean(self):
        """Тест: среднее значение"""
        arr = np.array([1, 2, 3, 4, 5])
        assert np.mean(arr) == 3.0
    
    def test_numpy_sum(self):
        """Тест: сумма"""
        arr = np.array([1, 2, 3])
        assert np.sum(arr) == 6
    
    def test_numpy_max(self):
        """Тест: максимум"""
        arr = np.array([1, 5, 3])
        assert np.max(arr) == 5
    
    def test_numpy_min(self):
        """Тест: минимум"""
        arr = np.array([1, 5, 3])
        assert np.min(arr) == 1


class TestDatetimeOperations:
    """Тесты для работы с датами"""
    
    def test_datetime_creation(self):
        """Тест: создание datetime"""
        dt = datetime(2023, 12, 25, 20, 0)
        assert dt.year == 2023
        assert dt.month == 12
        assert dt.day == 25
    
    def test_datetime_now(self):
        """Тест: текущее время"""
        now = datetime.now()
        assert now.year >= 2023
    
    def test_time_creation(self):
        """Тест: создание time"""
        t = time(20, 30, 45)
        assert t.hour == 20
        assert t.minute == 30
        assert t.second == 45
    
    def test_datetime_strftime(self):
        """Тест: форматирование даты"""
        dt = datetime(2023, 12, 25)
        formatted = dt.strftime('%Y-%m-%d')
        assert formatted == '2023-12-25'
    
    def test_datetime_comparison(self):
        """Тест: сравнение дат"""
        dt1 = datetime(2023, 12, 25)
        dt2 = datetime(2023, 12, 26)
        assert dt1 < dt2



