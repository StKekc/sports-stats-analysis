"""
Простые вспомогательные тесты для интеграции
"""

import pytest
import pandas as pd
from pathlib import Path


class TestPathOperations:
    """Тесты для работы с путями"""
    
    def test_path_creation(self):
        """Тест: создание Path объекта"""
        p = Path('/tmp/test.csv')
        assert p.name == 'test.csv'
    
    def test_path_parent(self):
        """Тест: родительская директория"""
        p = Path('/data/raw/file.csv')
        assert p.parent == Path('/data/raw')
    
    def test_path_suffix(self):
        """Тест: расширение файла"""
        p = Path('file.csv')
        assert p.suffix == '.csv'
    
    def test_path_stem(self):
        """Тест: имя без расширения"""
        p = Path('file.csv')
        assert p.stem == 'file'
    
    def test_path_exists(self, tmp_path):
        """Тест: проверка существования"""
        test_file = tmp_path / "test.txt"
        assert not test_file.exists()
        test_file.write_text("content")
        assert test_file.exists()
    
    def test_path_is_file(self, tmp_path):
        """Тест: проверка на файл"""
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")
        assert test_file.is_file()
    
    def test_path_is_dir(self, tmp_path):
        """Тест: проверка на директорию"""
        test_dir = tmp_path / "subdir"
        test_dir.mkdir()
        assert test_dir.is_dir()
    
    def test_path_joinpath(self):
        """Тест: объединение путей"""
        p = Path('/data')
        full_path = p / 'raw' / 'file.csv'
        assert str(full_path) == '/data/raw/file.csv'
    
    def test_path_as_posix(self):
        """Тест: POSIX формат пути"""
        p = Path('data/raw/file.csv')
        assert '/' in p.as_posix()


class TestDataFrameFiltering:
    """Тесты для фильтрации DataFrame"""
    
    def test_filter_by_condition(self):
        """Тест: фильтрация по условию"""
        df = pd.DataFrame({'points': [48, 42, 40]})
        filtered = df[df['points'] > 41]
        assert len(filtered) == 2
    
    def test_filter_equals(self):
        """Тест: фильтрация по равенству"""
        df = pd.DataFrame({'team': ['Arsenal', 'Chelsea', 'Arsenal']})
        filtered = df[df['team'] == 'Arsenal']
        assert len(filtered) == 2
    
    def test_filter_isin(self):
        """Тест: фильтрация isin"""
        df = pd.DataFrame({'team': ['Arsenal', 'Chelsea', 'Liverpool']})
        filtered = df[df['team'].isin(['Arsenal', 'Liverpool'])]
        assert len(filtered) == 2
    
    def test_filter_notna(self):
        """Тест: фильтрация не-null"""
        df = pd.DataFrame({'col': [1, None, 3]})
        filtered = df[df['col'].notna()]
        assert len(filtered) == 2
    
    def test_filter_multiple_conditions(self):
        """Тест: множественные условия"""
        df = pd.DataFrame({
            'team': ['Arsenal', 'Chelsea', 'Liverpool'],
            'points': [48, 42, 44]
        })
        filtered = df[(df['points'] > 42) & (df['team'] != 'Liverpool')]
        assert len(filtered) == 1


class TestDataFrameAggregation:
    """Тесты для агрегации DataFrame"""
    
    def test_groupby_sum(self):
        """Тест: группировка с суммой"""
        df = pd.DataFrame({
            'team': ['Arsenal', 'Arsenal', 'Chelsea'],
            'goals': [2, 3, 1]
        })
        grouped = df.groupby('team')['goals'].sum()
        assert grouped['Arsenal'] == 5
    
    def test_groupby_count(self):
        """Тест: группировка с подсчетом"""
        df = pd.DataFrame({
            'team': ['Arsenal', 'Arsenal', 'Chelsea'],
            'match': [1, 2, 1]
        })
        grouped = df.groupby('team').size()
        assert grouped['Arsenal'] == 2
    
    def test_groupby_mean(self):
        """Тест: группировка со средним"""
        df = pd.DataFrame({
            'team': ['Arsenal', 'Arsenal'],
            'goals': [2, 4]
        })
        grouped = df.groupby('team')['goals'].mean()
        assert grouped['Arsenal'] == 3.0
    
    def test_describe(self):
        """Тест: статистика describe"""
        df = pd.DataFrame({'points': [48, 42, 44, 40]})
        stats = df['points'].describe()
        assert stats['count'] == 4
        assert stats['mean'] == 43.5
    
    def test_value_counts(self):
        """Тест: подсчет уникальных значений"""
        df = pd.DataFrame({'team': ['Arsenal', 'Chelsea', 'Arsenal']})
        counts = df['team'].value_counts()
        assert counts['Arsenal'] == 2


class TestDataFrameMerge:
    """Тесты для объединения DataFrame"""
    
    def test_merge_inner(self):
        """Тест: inner join"""
        df1 = pd.DataFrame({'id': [1, 2], 'team': ['Arsenal', 'Chelsea']})
        df2 = pd.DataFrame({'id': [1, 3], 'points': [48, 40]})
        merged = pd.merge(df1, df2, on='id', how='inner')
        assert len(merged) == 1
    
    def test_merge_left(self):
        """Тест: left join"""
        df1 = pd.DataFrame({'id': [1, 2], 'team': ['Arsenal', 'Chelsea']})
        df2 = pd.DataFrame({'id': [1], 'points': [48]})
        merged = pd.merge(df1, df2, on='id', how='left')
        assert len(merged) == 2
    
    def test_concat_vertical(self):
        """Тест: вертикальная конкатенация"""
        df1 = pd.DataFrame({'col': [1, 2]})
        df2 = pd.DataFrame({'col': [3, 4]})
        result = pd.concat([df1, df2], ignore_index=True)
        assert len(result) == 4
    
    def test_concat_horizontal(self):
        """Тест: горизонтальная конкатенация"""
        df1 = pd.DataFrame({'a': [1, 2]})
        df2 = pd.DataFrame({'b': [3, 4]})
        result = pd.concat([df1, df2], axis=1)
        assert result.shape == (2, 2)


class TestDataFrameTypeConversion:
    """Тесты для преобразования типов"""
    
    def test_astype_int(self):
        """Тест: преобразование в int"""
        df = pd.DataFrame({'col': ['1', '2', '3']})
        df['col'] = df['col'].astype(int)
        assert df['col'].dtype == 'int64'
    
    def test_astype_float(self):
        """Тест: преобразование в float"""
        df = pd.DataFrame({'col': ['1.5', '2.5']})
        df['col'] = df['col'].astype(float)
        assert df['col'].dtype == 'float64'
    
    def test_astype_str(self):
        """Тест: преобразование в str"""
        df = pd.DataFrame({'col': [1, 2, 3]})
        df['col'] = df['col'].astype(str)
        assert df['col'].dtype == 'object'
    
    def test_to_numeric(self):
        """Тест: pd.to_numeric"""
        df = pd.DataFrame({'col': ['1', '2', 'invalid']})
        df['col'] = pd.to_numeric(df['col'], errors='coerce')
        assert pd.isna(df['col'].iloc[2])
    
    def test_to_datetime(self):
        """Тест: преобразование в datetime"""
        df = pd.DataFrame({'date': ['2023-12-25', '2023-12-26']})
        df['date'] = pd.to_datetime(df['date'])
        assert df['date'].dtype == 'datetime64[ns]'


class TestDataFrameNullHandling:
    """Тесты для обработки null значений"""
    
    def test_isna(self):
        """Тест: проверка на null"""
        df = pd.DataFrame({'col': [1, None, 3]})
        assert df['col'].isna().sum() == 1
    
    def test_fillna(self):
        """Тест: заполнение null"""
        df = pd.DataFrame({'col': [1, None, 3]})
        df['col'] = df['col'].fillna(0)
        assert df['col'].isna().sum() == 0
    
    def test_dropna(self):
        """Тест: удаление null"""
        df = pd.DataFrame({'col': [1, None, 3]})
        df_clean = df.dropna()
        assert len(df_clean) == 2
    
    def test_replace_nan(self):
        """Тест: замена значений"""
        df = pd.DataFrame({'col': [1, 2, 2, 3]})
        df['col'] = df['col'].replace(2, 999)
        assert (df['col'] == 999).sum() == 2


class TestDataFrameColumnOperations:
    """Тесты для операций с колонками"""
    
    def test_add_column(self):
        """Тест: добавление колонки"""
        df = pd.DataFrame({'a': [1, 2]})
        df['b'] = [3, 4]
        assert 'b' in df.columns
    
    def test_drop_column(self):
        """Тест: удаление колонки"""
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        df = df.drop('b', axis=1)
        assert 'b' not in df.columns
    
    def test_rename_column(self):
        """Тест: переименование колонки"""
        df = pd.DataFrame({'old_name': [1, 2]})
        df = df.rename(columns={'old_name': 'new_name'})
        assert 'new_name' in df.columns
    
    def test_column_calculation(self):
        """Тест: вычисление новой колонки"""
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        df['sum'] = df['a'] + df['b']
        assert df['sum'].tolist() == [4, 6]
    
    def test_column_apply(self):
        """Тест: применение функции к колонке"""
        df = pd.DataFrame({'col': [1, 2, 3]})
        df['doubled'] = df['col'].apply(lambda x: x * 2)
        assert df['doubled'].tolist() == [2, 4, 6]


class TestDataFrameSorting:
    """Тесты для сортировки DataFrame"""
    
    def test_sort_values_ascending(self):
        """Тест: сортировка по возрастанию"""
        df = pd.DataFrame({'points': [42, 48, 40]})
        sorted_df = df.sort_values('points')
        assert sorted_df['points'].iloc[0] == 40
    
    def test_sort_values_descending(self):
        """Тест: сортировка по убыванию"""
        df = pd.DataFrame({'points': [42, 48, 40]})
        sorted_df = df.sort_values('points', ascending=False)
        assert sorted_df['points'].iloc[0] == 48
    
    def test_sort_index(self):
        """Тест: сортировка по индексу"""
        df = pd.DataFrame({'col': [1, 2, 3]}, index=[2, 0, 1])
        sorted_df = df.sort_index()
        assert sorted_df.index.tolist() == [0, 1, 2]



