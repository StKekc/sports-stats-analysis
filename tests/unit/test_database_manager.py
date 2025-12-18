"""
Тесты для database_manager.py
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from database.database_manager import DatabaseManager


@pytest.fixture
def db_config():
    """Конфигурация БД для тестов"""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_password',
        'connection_timeout': 30
    }


@pytest.fixture
def db_manager(db_config):
    """Фикстура DatabaseManager"""
    return DatabaseManager(db_config)


class TestDatabaseManagerInit:
    """Тесты для инициализации DatabaseManager"""
    
    def test_init_stores_config(self, db_config):
        """Тест: инициализация сохраняет конфигурацию"""
        manager = DatabaseManager(db_config)
        assert manager.config == db_config
    
    def test_init_connection_is_none(self, db_config):
        """Тест: начальное подключение равно None"""
        manager = DatabaseManager(db_config)
        assert manager.connection is None
    
    def test_init_creates_logger(self, db_config):
        """Тест: создается логгер"""
        manager = DatabaseManager(db_config)
        assert manager.logger is not None
    
    def test_init_with_minimal_config(self):
        """Тест: инициализация с минимальной конфигурацией"""
        config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'db',
            'user': 'user',
            'password': 'pass'
        }
        manager = DatabaseManager(config)
        assert manager.config == config


class TestConnect:
    """Тесты для метода connect"""
    
    @patch('database.database_manager.psycopg2.connect')
    def test_connect_success(self, mock_connect, db_manager):
        """Тест: успешное подключение"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        db_manager.connect()
        
        assert db_manager.connection is mock_conn
        mock_connect.assert_called_once()
    
    @patch('database.database_manager.psycopg2.connect')
    def test_connect_uses_correct_params(self, mock_connect, db_manager):
        """Тест: используются правильные параметры подключения"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        db_manager.connect()
        
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs['host'] == 'localhost'
        assert call_kwargs['port'] == 5432
        assert call_kwargs['database'] == 'test_db'
        assert call_kwargs['user'] == 'test_user'
        assert call_kwargs['password'] == 'test_password'
    
    @patch('database.database_manager.psycopg2.connect')
    def test_connect_failure_raises_exception(self, mock_connect, db_manager):
        """Тест: ошибка подключения вызывает исключение"""
        mock_connect.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception):
            db_manager.connect()
    
    @patch('database.database_manager.psycopg2.connect')
    def test_connect_uses_default_timeout(self, mock_connect, db_config):
        """Тест: используется таймаут по умолчанию"""
        db_config_no_timeout = db_config.copy()
        del db_config_no_timeout['connection_timeout']
        manager = DatabaseManager(db_config_no_timeout)
        
        mock_connect.return_value = Mock()
        manager.connect()
        
        call_kwargs = mock_connect.call_args[1]
        assert 'connect_timeout' in call_kwargs


class TestDisconnect:
    """Тесты для метода disconnect"""
    
    def test_disconnect_closes_connection(self, db_manager):
        """Тест: отключение закрывает соединение"""
        mock_conn = Mock()
        db_manager.connection = mock_conn
        
        db_manager.disconnect()
        
        mock_conn.close.assert_called_once()
        assert db_manager.connection is None
    
    def test_disconnect_when_no_connection(self, db_manager):
        """Тест: отключение когда нет соединения"""
        db_manager.connection = None
        
        # Не должно вызывать ошибку
        db_manager.disconnect()
        
        assert db_manager.connection is None
    
    def test_disconnect_sets_connection_to_none(self, db_manager):
        """Тест: после отключения connection = None"""
        db_manager.connection = Mock()
        
        db_manager.disconnect()
        
        assert db_manager.connection is None


class TestGetCursor:
    """Тесты для метода get_cursor"""
    
    @patch('database.database_manager.psycopg2.connect')
    def test_get_cursor_connects_if_no_connection(self, mock_connect, db_manager):
        """Тест: автоматическое подключение если нет соединения"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with db_manager.get_cursor() as cursor:
            assert cursor is mock_cursor
        
        mock_connect.assert_called_once()
    
    def test_get_cursor_commits_on_success(self, db_manager):
        """Тест: commit при успешном выполнении"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        db_manager.connection = mock_conn
        
        with db_manager.get_cursor() as cursor:
            pass
        
        mock_conn.commit.assert_called_once()
    
    def test_get_cursor_rollback_on_exception(self, db_manager):
        """Тест: rollback при исключении"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        db_manager.connection = mock_conn
        
        with pytest.raises(ValueError):
            with db_manager.get_cursor() as cursor:
                raise ValueError("Test error")
        
        mock_conn.rollback.assert_called_once()
    
    def test_get_cursor_closes_cursor(self, db_manager):
        """Тест: курсор закрывается после использования"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        db_manager.connection = mock_conn
        
        with db_manager.get_cursor() as cursor:
            pass
        
        mock_cursor.close.assert_called_once()
    
    def test_get_cursor_with_cursor_factory(self, db_manager):
        """Тест: использование cursor_factory"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        db_manager.connection = mock_conn
        
        factory = Mock()
        with db_manager.get_cursor(cursor_factory=factory) as cursor:
            pass
        
        mock_conn.cursor.assert_called_with(cursor_factory=factory)


class TestExecuteQuery:
    """Тесты для метода execute_query"""
    
    

class TestConfigValidation:
    """Тесты валидации конфигурации"""
    
    def test_config_has_required_fields(self, db_config):
        """Тест: конфигурация содержит обязательные поля"""
        manager = DatabaseManager(db_config)
        assert 'host' in manager.config
        assert 'port' in manager.config
        assert 'database' in manager.config
        assert 'user' in manager.config
        assert 'password' in manager.config
    
    def test_config_port_is_integer(self, db_config):
        """Тест: порт является числом"""
        manager = DatabaseManager(db_config)
        assert isinstance(manager.config['port'], int)
    
    def test_config_can_have_extra_params(self, db_config):
        """Тест: конфигурация может иметь дополнительные параметры"""
        db_config['extra_param'] = 'value'
        manager = DatabaseManager(db_config)
        assert manager.config['extra_param'] == 'value'



