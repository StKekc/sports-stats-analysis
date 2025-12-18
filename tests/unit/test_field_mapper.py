"""
Тесты для FieldMapper из etl_utils.py
"""

import pytest
import pandas as pd
from database.etl_utils import FieldMapper


@pytest.fixture
def sample_mappings():
    """Примеры маппингов для тестов"""
    return {
        'fixtures': {
            'Date': 'match_date',
            'Time': 'match_time',
            'Home': 'home_team',
            'Away': 'away_team',
            'Score': 'score'
        },
        'standings': {
            'Squad': 'team_name',
            'MP': 'matches_played',
            'W': 'wins',
            'D': 'draws',
            'L': 'losses',
            'Pts': 'points'
        },
        'player_stats': {
            'Player': 'player_name',
            'Nation': 'nation',
            'Pos': 'position',
            'Age': 'age',
            'Min': 'minutes'
        }
    }


@pytest.fixture
def mapper(sample_mappings):
    """Фикстура FieldMapper"""
    return FieldMapper(sample_mappings)


class TestMapFields:
    """Тесты для метода map_fields"""
    
    def test_map_empty_dataframe(self, mapper):
        """Тест: маппинг пустого DataFrame"""
        df = pd.DataFrame()
        result = mapper.map_fields(df, 'fixtures')
        assert result.empty
    
    def test_map_fixtures_columns(self, mapper):
        """Тест: маппинг колонок fixtures"""
        df = pd.DataFrame({
            'Date': ['2023-12-25'],
            'Time': ['20:00'],
            'Home': ['Arsenal'],
            'Away': ['Chelsea']
        })
        result = mapper.map_fields(df, 'fixtures')
        
        assert 'match_date' in result.columns
        assert 'match_time' in result.columns
        assert 'home_team' in result.columns
        assert 'away_team' in result.columns
        assert 'Date' not in result.columns
    
    def test_map_standings_columns(self, mapper):
        """Тест: маппинг колонок standings"""
        df = pd.DataFrame({
            'Squad': ['Arsenal', 'Chelsea'],
            'MP': [20, 20],
            'Pts': [45, 40]
        })
        result = mapper.map_fields(df, 'standings')
        
        assert 'team_name' in result.columns
        assert 'matches_played' in result.columns
        assert 'points' in result.columns
    
    def test_map_player_stats_columns(self, mapper):
        """Тест: маппинг колонок player_stats"""
        df = pd.DataFrame({
            'Player': ['Saka'],
            'Nation': ['ENG'],
            'Pos': ['FW']
        })
        result = mapper.map_fields(df, 'player_stats')
        
        assert 'player_name' in result.columns
        assert 'nation' in result.columns
        assert 'position' in result.columns
    
    def test_map_only_existing_columns(self, mapper):
        """Тест: маппинг только существующих колонок"""
        df = pd.DataFrame({
            'Date': ['2023-12-25'],
            'Home': ['Arsenal']
        })
        result = mapper.map_fields(df, 'fixtures')
        
        assert 'match_date' in result.columns
        assert 'home_team' in result.columns
        assert 'match_time' not in result.columns  # не было в исходном df
    
    def test_map_unknown_mapping_key_returns_unchanged(self, mapper):
        """Тест: неизвестный ключ маппинга возвращает неизмененный DataFrame"""
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        result = mapper.map_fields(df, 'unknown_key')
        
        assert list(result.columns) == ['col1', 'col2']
    
    def test_map_preserves_data(self, mapper):
        """Тест: маппинг сохраняет данные"""
        df = pd.DataFrame({
            'Squad': ['Arsenal', 'Chelsea'],
            'Pts': [45, 40]
        })
        result = mapper.map_fields(df, 'standings')
        
        assert result['team_name'].tolist() == ['Arsenal', 'Chelsea']
        assert result['points'].tolist() == [45, 40]
    
    def test_map_mixed_mapped_and_unmapped_columns(self, mapper):
        """Тест: маппинг с смешанными колонками (мапятся и не мапятся)"""
        df = pd.DataFrame({
            'Date': ['2023-12-25'],
            'Extra': ['value']
        })
        result = mapper.map_fields(df, 'fixtures')
        
        assert 'match_date' in result.columns
        assert 'Extra' in result.columns  # немаппированная колонка сохраняется


class TestGetDbFieldName:
    """Тесты для метода get_db_field_name"""
    
    def test_get_mapped_field_name(self, mapper):
        """Тест: получение маппированного имени поля"""
        assert mapper.get_db_field_name('Date', 'fixtures') == 'match_date'
        assert mapper.get_db_field_name('Squad', 'standings') == 'team_name'
    
    def test_get_unmapped_field_returns_original(self, mapper):
        """Тест: немаппированное поле возвращает оригинальное имя"""
        assert mapper.get_db_field_name('Unknown', 'fixtures') == 'Unknown'
    
    def test_get_field_with_unknown_mapping_key(self, mapper):
        """Тест: получение поля с неизвестным ключом маппинга"""
        assert mapper.get_db_field_name('Date', 'unknown') == 'Date'
    
    def test_get_all_fixtures_mappings(self, mapper):
        """Тест: проверка всех маппингов fixtures"""
        assert mapper.get_db_field_name('Time', 'fixtures') == 'match_time'
        assert mapper.get_db_field_name('Home', 'fixtures') == 'home_team'
        assert mapper.get_db_field_name('Away', 'fixtures') == 'away_team'
        assert mapper.get_db_field_name('Score', 'fixtures') == 'score'
    
    def test_get_all_standings_mappings(self, mapper):
        """Тест: проверка всех маппингов standings"""
        assert mapper.get_db_field_name('MP', 'standings') == 'matches_played'
        assert mapper.get_db_field_name('W', 'standings') == 'wins'
        assert mapper.get_db_field_name('D', 'standings') == 'draws'
        assert mapper.get_db_field_name('L', 'standings') == 'losses'
    
    def test_get_all_player_stats_mappings(self, mapper):
        """Тест: проверка всех маппингов player_stats"""
        assert mapper.get_db_field_name('Player', 'player_stats') == 'player_name'
        assert mapper.get_db_field_name('Nation', 'player_stats') == 'nation'
        assert mapper.get_db_field_name('Pos', 'player_stats') == 'position'
        assert mapper.get_db_field_name('Age', 'player_stats') == 'age'
        assert mapper.get_db_field_name('Min', 'player_stats') == 'minutes'


class TestFieldMapperInit:
    """Тесты для инициализации FieldMapper"""
    
    def test_init_with_empty_mappings(self):
        """Тест: инициализация с пустыми маппингами"""
        mapper = FieldMapper({})
        assert mapper.mappings == {}
    
    def test_init_stores_mappings(self, sample_mappings):
        """Тест: инициализация сохраняет маппинги"""
        mapper = FieldMapper(sample_mappings)
        assert mapper.mappings == sample_mappings
    
    def test_init_with_single_mapping(self):
        """Тест: инициализация с одним маппингом"""
        mapping = {'key': {'old': 'new'}}
        mapper = FieldMapper(mapping)
        assert mapper.mappings == mapping



