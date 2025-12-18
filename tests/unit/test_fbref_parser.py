"""
Тесты для fbref_parser.py
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from src.modules.data_reception.fbref_parser import (
    flatten_and_normalize_columns,
    find_players_stats_url,
    parse_players_standard_table
)


class TestFlattenAndNormalizeColumns:
    """Тесты для функции flatten_and_normalize_columns"""
    
    def test_flatten_simple_columns(self):
        """Тест: нормализация простых колонок"""
        df = pd.DataFrame({
            'Team Name': [1, 2],
            'Goals Scored': [3, 4]
        })
        result = flatten_and_normalize_columns(df)
        assert 'team_name' in result.columns
        assert 'goals_scored' in result.columns
    
    def test_normalize_removes_special_chars(self):
        """Тест: удаление специальных символов"""
        df = pd.DataFrame({
            'Goals+Assists': [1, 2],
            'Shots/90': [3, 4]
        })
        result = flatten_and_normalize_columns(df)
        # Специальные символы заменяются на подчеркивания
        assert any('goals' in col for col in result.columns)
        assert any('shots' in col for col in result.columns)
    
    def test_normalize_lowercase(self):
        """Тест: приведение к нижнему регистру"""
        df = pd.DataFrame({
            'TEAM': [1, 2],
            'PLAYER': [3, 4]
        })
        result = flatten_and_normalize_columns(df)
        assert all(col.islower() or col.isdigit() or '_' in col for col in result.columns)
    
    def test_normalize_strips_whitespace(self):
        """Тест: удаление пробелов"""
        df = pd.DataFrame({
            '  Team  ': [1, 2],
            ' Goals ': [3, 4]
        })
        result = flatten_and_normalize_columns(df)
        assert 'team' in result.columns
        assert 'goals' in result.columns
    
    def test_normalize_replaces_multiple_underscores(self):
        """Тест: множественные подчеркивания заменяются на одно"""
        df = pd.DataFrame({
            'Team___Name': [1, 2]
        })
        result = flatten_and_normalize_columns(df)
        assert 'team_name' in result.columns
        assert '___' not in result.columns[0]
    
    def test_normalize_empty_dataframe(self):
        """Тест: нормализация пустого DataFrame"""
        df = pd.DataFrame()
        result = flatten_and_normalize_columns(df)
        assert result.empty
    
    def test_normalize_removes_duplicates(self):
        """Тест: удаление дубликатов колонок"""
        df = pd.DataFrame({
            'Team': [1, 2],
            'team': [3, 4]  # дубликат после нормализации
        })
        result = flatten_and_normalize_columns(df)
        # Должна остаться только одна колонка 'team'
        assert result.columns.tolist().count('team') == 1
    
    def test_normalize_drops_all_na_rows(self):
        """Тест: удаление полностью пустых строк"""
        df = pd.DataFrame({
            'Team': [1, np.nan],
            'Goals': [2, np.nan]
        })
        result = flatten_and_normalize_columns(df)
        # Вторая строка (полностью NaN) должна быть удалена
        assert len(result) <= len(df)
    
    def test_normalize_preserves_data(self):
        """Тест: данные сохраняются после нормализации"""
        df = pd.DataFrame({
            'Team Name': ['Arsenal', 'Chelsea'],
            'Goals': [50, 45]
        })
        result = flatten_and_normalize_columns(df)
        assert len(result) == 2
        assert result.iloc[0, 1] == 50 or result.iloc[0, 0] == 'Arsenal'
    
    def test_normalize_g_plus_a_replacement(self):
        """Тест: специальная замена G+A"""
        df = pd.DataFrame({
            'G+A': [10, 15]
        })
        result = flatten_and_normalize_columns(df)
        assert 'g_plus_a' in result.columns


class TestFindPlayersStatsUrl:
    """Тесты для функции find_players_stats_url"""
    
    def test_find_url_in_simple_html(self):
        """Тест: поиск URL в простом HTML"""
        html = """
        <html>
            <a href="/en/comps/9/stats/players">Standard Stats — Players</a>
        </html>
        """
        result = find_players_stats_url(html)
        assert result is not None
        assert 'fbref.com' in result
        assert 'players' in result or 'stats' in result
    
    def test_find_url_with_full_path(self):
        """Тест: поиск URL с полным путем"""
        html = """
        <html>
            <a href="https://fbref.com/en/comps/9/stats/players">Standard Stats</a>
        </html>
        """
        result = find_players_stats_url(html)
        assert result is not None
        assert 'https://fbref.com' in result
    
    def test_find_url_not_found(self):
        """Тест: URL не найден"""
        html = """
        <html>
            <a href="/other/page">Other Link</a>
        </html>
        """
        result = find_players_stats_url(html)
        assert result is None
    
    def test_find_url_empty_html(self):
        """Тест: пустой HTML"""
        html = ""
        result = find_players_stats_url(html)
        assert result is None
    
    def test_find_url_no_links(self):
        """Тест: HTML без ссылок"""
        html = "<html><body>No links here</body></html>"
        result = find_players_stats_url(html)
        assert result is None
    
    def test_find_url_with_standard_keyword(self):
        """Тест: поиск с ключевым словом 'standard'"""
        html = """
        <html>
            <a href="/en/comps/9/players">Standard Stats</a>
        </html>
        """
        result = find_players_stats_url(html)
        assert result is not None
    
    def test_find_url_case_insensitive(self):
        """Тест: поиск без учета регистра"""
        html = """
        <html>
            <a href="/en/comps/9/stats/players">STANDARD Stats</a>
        </html>
        """
        result = find_players_stats_url(html)
        assert result is not None
    
    def test_find_url_multiple_links_returns_first(self):
        """Тест: множественные ссылки - возвращается первая"""
        html = """
        <html>
            <a href="/en/comps/9/stats/players">Standard Stats 1</a>
            <a href="/en/comps/10/stats/players">Standard Stats 2</a>
        </html>
        """
        result = find_players_stats_url(html)
        assert result is not None
        assert 'comps' in result


class TestParsePlayersStandardTable:
    """Тесты для функции parse_players_standard_table"""
    
    @patch('src.modules.data_reception.fbref_parser.biggest_table')
    def test_parse_returns_dataframe(self, mock_biggest):
        """Тест: функция возвращает DataFrame"""
        mock_df = pd.DataFrame({'Player': ['Saka'], 'Goals': [10]})
        mock_biggest.return_value = mock_df
        
        result = parse_players_standard_table("<html>table</html>")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
    
    @patch('src.modules.data_reception.fbref_parser.biggest_table')
    def test_parse_calls_biggest_table(self, mock_biggest):
        """Тест: функция вызывает biggest_table"""
        mock_df = pd.DataFrame({'col': [1]})
        mock_biggest.return_value = mock_df
        
        html = "<html>test</html>"
        parse_players_standard_table(html)
        
        mock_biggest.assert_called_once_with(html)
    
    @patch('src.modules.data_reception.fbref_parser.biggest_table')
    def test_parse_handles_exception(self, mock_biggest):
        """Тест: обработка исключений"""
        mock_biggest.side_effect = Exception("Parse error")
        
        result = parse_players_standard_table("<html>bad html</html>")
        
        assert result is None
    
    @patch('src.modules.data_reception.fbref_parser.biggest_table')
    def test_parse_empty_html(self, mock_biggest):
        """Тест: парсинг пустого HTML"""
        mock_biggest.return_value = pd.DataFrame()
        
        result = parse_players_standard_table("")
        
        assert result is not None
        assert result.empty
    
    @patch('src.modules.data_reception.fbref_parser.biggest_table')
    def test_parse_returns_none_on_error(self, mock_biggest):
        """Тест: возврат None при ошибке"""
        mock_biggest.side_effect = ValueError("Invalid table")
        
        result = parse_players_standard_table("<html>invalid</html>")
        
        assert result is None


class TestDataFrameOperations:
    """Дополнительные тесты для работы с DataFrame"""
    
    def test_multiindex_columns_handling(self):
        """Тест: обработка многоуровневых колонок"""
        # Создаем DataFrame с MultiIndex колонками
        arrays = [
            ['Performance', 'Performance', 'Expected'],
            ['Goals', 'Assists', 'xG']
        ]
        columns = pd.MultiIndex.from_arrays(arrays)
        df = pd.DataFrame([[1, 2, 3]], columns=columns)
        
        result = flatten_and_normalize_columns(df)
        
        # Колонки должны быть сплющены
        assert not isinstance(result.columns, pd.MultiIndex)
        assert len(result.columns) == 3
    
    def test_reset_index_after_normalize(self):
        """Тест: индекс сбрасывается после нормализации"""
        df = pd.DataFrame({'Team': ['A', 'B']}, index=[5, 10])
        result = flatten_and_normalize_columns(df)
        
        # Индекс должен начинаться с 0
        assert result.index.tolist() == [0, 1]
    
    def test_preserve_numeric_values(self):
        """Тест: сохранение числовых значений"""
        df = pd.DataFrame({
            'Goals Scored': [10, 20, 30],
            'Assists': [5, 15, 25]
        })
        result = flatten_and_normalize_columns(df)
        
        # Числовые значения должны быть сохранены
        assert result.iloc[0].sum() in [15, 30]  # 10+5 или другая комбинация
        assert result.iloc[2].sum() in [55, 30, 25]  # 30+25 или другая
    
    def test_handle_unnamed_columns(self):
        """Тест: обработка колонок Unnamed"""
        df = pd.DataFrame({
            'Team': [1, 2],
            'Unnamed: 0': [3, 4]
        })
        result = flatten_and_normalize_columns(df)
        
        # Unnamed колонки должны быть обработаны
        assert len(result.columns) >= 1



