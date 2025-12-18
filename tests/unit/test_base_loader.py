"""
Тесты для BaseLoader из etl_loaders.py
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, mock_open
from database.etl_loaders import BaseLoader


@pytest.fixture
def mock_db_manager():
    """Мок DatabaseManager"""
    return Mock()


@pytest.fixture
def mock_cache_manager():
    """Мок CacheManager"""
    return Mock()


@pytest.fixture
def mock_config():
    """Мок конфигурации"""
    return {
        'special_values': {
            'null_values': ['N/A'],
            'replacements': {}
        },
        'validation': {
            'check_negative_stats': True
        },
        'field_mappings': {}
    }


@pytest.fixture
def base_loader(mock_db_manager, mock_cache_manager, mock_config):
    """Фикстура BaseLoader"""
    return BaseLoader(mock_db_manager, mock_cache_manager, mock_config)


class TestBaseLoaderInit:
    """Тесты для инициализации BaseLoader"""
    
    def test_init_stores_db_manager(self, base_loader, mock_db_manager):
        """Тест: сохранение DatabaseManager"""
        assert base_loader.db is mock_db_manager
    
    def test_init_stores_cache(self, base_loader, mock_cache_manager):
        """Тест: сохранение CacheManager"""
        assert base_loader.cache is mock_cache_manager
    
    def test_init_stores_config(self, base_loader, mock_config):
        """Тест: сохранение конфигурации"""
        assert base_loader.config is mock_config
    
    def test_init_creates_cleaner(self, base_loader):
        """Тест: создание DataCleaner"""
        assert base_loader.cleaner is not None
    
    def test_init_creates_field_mapper(self, base_loader):
        """Тест: создание FieldMapper"""
        assert base_loader.field_mapper is not None
    
    def test_init_creates_validator(self, base_loader):
        """Тест: создание DataValidator"""
        assert base_loader.validator is not None
    
    def test_init_creates_logger(self, base_loader):
        """Тест: создание логгера"""
        assert base_loader.logger is not None


class TestReadCsv:
    """Тесты для метода read_csv"""
    
    @patch('pandas.read_csv')
    def test_read_csv_success(self, mock_read_csv, base_loader, tmp_path):
        """Тест: успешное чтение CSV"""
        test_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        mock_read_csv.return_value = test_df
        
        csv_path = tmp_path / "test.csv"
        result = base_loader.read_csv(csv_path)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
    
    @patch('pandas.read_csv')
    def test_read_csv_file_not_found(self, mock_read_csv, base_loader, tmp_path):
        """Тест: файл не найден"""
        mock_read_csv.side_effect = FileNotFoundError()
        
        csv_path = tmp_path / "nonexistent.csv"
        result = base_loader.read_csv(csv_path)
        
        assert result is None
    
    @patch('pandas.read_csv')
    def test_read_csv_general_exception(self, mock_read_csv, base_loader, tmp_path):
        """Тест: общее исключение"""
        mock_read_csv.side_effect = Exception("Read error")
        
        csv_path = tmp_path / "error.csv"
        result = base_loader.read_csv(csv_path)
        
        assert result is None
    
    @patch('pandas.read_csv')
    def test_read_csv_empty_file(self, mock_read_csv, base_loader, tmp_path):
        """Тест: чтение пустого CSV"""
        mock_read_csv.return_value = pd.DataFrame()
        
        csv_path = tmp_path / "empty.csv"
        result = base_loader.read_csv(csv_path)
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    @patch('pandas.read_csv')
    def test_read_csv_uses_utf8_encoding(self, mock_read_csv, base_loader, tmp_path):
        """Тест: использование UTF-8 кодировки"""
        test_df = pd.DataFrame({'col': ['test']})
        mock_read_csv.return_value = test_df
        
        csv_path = tmp_path / "test.csv"
        base_loader.read_csv(csv_path)
        
        mock_read_csv.assert_called_with(csv_path, encoding='utf-8')
    
    @patch('pandas.read_csv')
    def test_read_csv_with_special_characters(self, mock_read_csv, base_loader, tmp_path):
        """Тест: чтение CSV со специальными символами"""
        test_df = pd.DataFrame({'team': ['Arsenal', 'Реал Мадрид']})
        mock_read_csv.return_value = test_df
        
        csv_path = tmp_path / "special.csv"
        result = base_loader.read_csv(csv_path)
        
        assert len(result) == 2
    
    @patch('pandas.read_csv')
    def test_read_csv_large_file(self, mock_read_csv, base_loader, tmp_path):
        """Тест: чтение большого CSV"""
        large_df = pd.DataFrame({'col': range(10000)})
        mock_read_csv.return_value = large_df
        
        csv_path = tmp_path / "large.csv"
        result = base_loader.read_csv(csv_path)
        
        assert len(result) == 10000


class TestBaseLoaderComponents:
    """Тесты для компонентов BaseLoader"""
    
    def test_cleaner_is_data_cleaner(self, base_loader):
        """Тест: cleaner является DataCleaner"""
        from database.etl_utils import DataCleaner
        assert isinstance(base_loader.cleaner, DataCleaner)
    
    def test_field_mapper_is_field_mapper(self, base_loader):
        """Тест: field_mapper является FieldMapper"""
        from database.etl_utils import FieldMapper
        assert isinstance(base_loader.field_mapper, FieldMapper)
    
    def test_validator_is_data_validator(self, base_loader):
        """Тест: validator является DataValidator"""
        from database.etl_utils import DataValidator
        assert isinstance(base_loader.validator, DataValidator)
    
    def test_logger_name_matches_class_name(self, base_loader):
        """Тест: имя логгера соответствует имени класса"""
        assert base_loader.logger.name == 'BaseLoader'


class TestBaseLoaderEdgeCases:
    """Тесты граничных случаев"""
    
    @patch('pandas.read_csv')
    def test_read_csv_with_path_object(self, mock_read_csv, base_loader, tmp_path):
        """Тест: чтение с объектом Path"""
        test_df = pd.DataFrame({'col': [1]})
        mock_read_csv.return_value = test_df
        
        csv_path = Path(tmp_path) / "test.csv"
        result = base_loader.read_csv(csv_path)
        
        assert isinstance(result, pd.DataFrame)
    
    @patch('pandas.read_csv')
    def test_read_csv_returns_exact_dataframe(self, mock_read_csv, base_loader, tmp_path):
        """Тест: возвращается точный DataFrame"""
        expected_df = pd.DataFrame({
            'team': ['Arsenal', 'Chelsea'],
            'points': [48, 42]
        })
        mock_read_csv.return_value = expected_df
        
        csv_path = tmp_path / "test.csv"
        result = base_loader.read_csv(csv_path)
        
        pd.testing.assert_frame_equal(result, expected_df)
    
    def test_config_passed_to_components(self, mock_db_manager, mock_cache_manager):
        """Тест: конфигурация передается в компоненты"""
        config = {
            'special_values': {'null_values': ['test']},
            'validation': {},
            'field_mappings': {'key': 'value'}
        }
        loader = BaseLoader(mock_db_manager, mock_cache_manager, config)
        
        assert loader.cleaner.config is config
        assert loader.validator.config is config



