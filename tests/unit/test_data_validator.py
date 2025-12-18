"""
Тесты для DataValidator из etl_utils.py
"""

import pytest
from datetime import datetime
from database.etl_utils import DataValidator


@pytest.fixture
def config():
    """Конфигурация для тестов"""
    return {
        'validation': {
            'max_goals_per_match': 20,
            'min_birth_year': 1950
        }
    }


@pytest.fixture
def validator(config):
    """Фикстура DataValidator"""
    return DataValidator(config)


class TestValidateMatchData:
    """Тесты для метода validate_match_data"""
    
    def test_validate_valid_match(self, validator):
        """Тест: валидация корректного матча"""
        match = {
            'match_date': '2023-12-25',
            'home_team_id': 1,
            'away_team_id': 2,
            'home_goals': 2,
            'away_goals': 1
        }
        assert validator.validate_match_data(match) is True
    
    def test_validate_match_without_date_fails(self, validator):
        """Тест: матч без даты невалиден"""
        match = {
            'home_team_id': 1,
            'away_team_id': 2
        }
        assert validator.validate_match_data(match) is False
    
    def test_validate_match_with_none_date_fails(self, validator):
        """Тест: матч с None датой невалиден"""
        match = {
            'match_date': None,
            'home_team_id': 1,
            'away_team_id': 2
        }
        assert validator.validate_match_data(match) is False
    
    def test_validate_match_without_home_team_fails(self, validator):
        """Тест: матч без домашней команды невалиден"""
        match = {
            'match_date': '2023-12-25',
            'away_team_id': 2
        }
        assert validator.validate_match_data(match) is False
    
    def test_validate_match_without_away_team_fails(self, validator):
        """Тест: матч без гостевой команды невалиден"""
        match = {
            'match_date': '2023-12-25',
            'home_team_id': 1
        }
        assert validator.validate_match_data(match) is False
    
    def test_validate_match_same_teams_fails(self, validator):
        """Тест: матч с одинаковыми командами невалиден"""
        match = {
            'match_date': '2023-12-25',
            'home_team_id': 1,
            'away_team_id': 1
        }
        assert validator.validate_match_data(match) is False
    
    def test_validate_match_with_normal_score(self, validator):
        """Тест: матч с нормальным счетом валиден"""
        match = {
            'match_date': '2023-12-25',
            'home_team_id': 1,
            'away_team_id': 2,
            'home_goals': 3,
            'away_goals': 2
        }
        assert validator.validate_match_data(match) is True
    
    def test_validate_match_with_zero_score(self, validator):
        """Тест: матч со счетом 0-0 валиден"""
        match = {
            'match_date': '2023-12-25',
            'home_team_id': 1,
            'away_team_id': 2,
            'home_goals': 0,
            'away_goals': 0
        }
        assert validator.validate_match_data(match) is True
    
    def test_validate_match_with_high_score_warns(self, validator):
        """Тест: матч с очень высоким счетом выдает предупреждение но проходит"""
        match = {
            'match_date': '2023-12-25',
            'home_team_id': 1,
            'away_team_id': 2,
            'home_goals': 25,
            'away_goals': 1
        }
        # Должно пройти с warning, но вернуть True
        assert validator.validate_match_data(match) is True
    
    def test_validate_match_without_goals(self, validator):
        """Тест: матч без голов (None) валиден"""
        match = {
            'match_date': '2023-12-25',
            'home_team_id': 1,
            'away_team_id': 2,
            'home_goals': None,
            'away_goals': None
        }
        assert validator.validate_match_data(match) is True
    
    def test_validate_match_minimal_valid_data(self, validator):
        """Тест: матч с минимальными данными валиден"""
        match = {
            'match_date': '2023-12-25',
            'home_team_id': 5,
            'away_team_id': 10
        }
        assert validator.validate_match_data(match) is True


class TestValidatePlayerData:
    """Тесты для метода validate_player_data"""
    
    def test_validate_valid_player(self, validator):
        """Тест: валидация корректного игрока"""
        player = {
            'player_name': 'Bukayo Saka',
            'born': 2001,
            'nation': 'ENG'
        }
        assert validator.validate_player_data(player) is True
    
    def test_validate_player_without_name_fails(self, validator):
        """Тест: игрок без имени невалиден"""
        player = {
            'born': 1995
        }
        assert validator.validate_player_data(player) is False
    
    def test_validate_player_with_empty_name_fails(self, validator):
        """Тест: игрок с пустым именем невалиден"""
        player = {
            'player_name': '',
            'born': 1995
        }
        assert validator.validate_player_data(player) is False
    
    def test_validate_player_with_none_name_fails(self, validator):
        """Тест: игрок с None именем невалиден"""
        player = {
            'player_name': None,
            'born': 1995
        }
        assert validator.validate_player_data(player) is False
    
    def test_validate_player_with_valid_birth_year(self, validator):
        """Тест: игрок с валидным годом рождения"""
        player = {
            'player_name': 'John Doe',
            'born': 1990
        }
        assert validator.validate_player_data(player) is True
    
    def test_validate_player_with_old_birth_year_fails(self, validator):
        """Тест: игрок со слишком старым годом рождения невалиден"""
        player = {
            'player_name': 'John Doe',
            'born': 1940
        }
        assert validator.validate_player_data(player) is False
    
    def test_validate_player_with_future_birth_year_fails(self, validator):
        """Тест: игрок с будущим годом рождения невалиден"""
        current_year = datetime.now().year
        player = {
            'player_name': 'John Doe',
            'born': current_year + 1
        }
        assert validator.validate_player_data(player) is False
    
    def test_validate_player_with_current_year_birth(self, validator):
        """Тест: игрок, рожденный в текущем году, валиден"""
        current_year = datetime.now().year
        player = {
            'player_name': 'Baby Player',
            'born': current_year
        }
        assert validator.validate_player_data(player) is True
    
    def test_validate_player_with_min_birth_year(self, validator):
        """Тест: игрок с минимальным годом рождения валиден"""
        player = {
            'player_name': 'Old Player',
            'born': 1950
        }
        assert validator.validate_player_data(player) is True
    
    def test_validate_player_without_birth_year(self, validator):
        """Тест: игрок без года рождения валиден"""
        player = {
            'player_name': 'John Doe'
        }
        assert validator.validate_player_data(player) is True
    
    def test_validate_player_with_none_birth_year(self, validator):
        """Тест: игрок с None годом рождения валиден"""
        player = {
            'player_name': 'John Doe',
            'born': None
        }
        assert validator.validate_player_data(player) is True
    
    def test_validate_player_minimal_data(self, validator):
        """Тест: игрок с минимальными данными валиден"""
        player = {
            'player_name': 'Minimal Player'
        }
        assert validator.validate_player_data(player) is True
    
    def test_validate_player_with_extra_fields(self, validator):
        """Тест: игрок с дополнительными полями валиден"""
        player = {
            'player_name': 'Complete Player',
            'born': 1995,
            'nation': 'ENG',
            'position': 'FW',
            'team': 'Arsenal'
        }
        assert validator.validate_player_data(player) is True


class TestDataValidatorInit:
    """Тесты для инициализации DataValidator"""
    
    def test_init_stores_config(self, config):
        """Тест: инициализация сохраняет конфигурацию"""
        validator = DataValidator(config)
        assert validator.config == config
    
    def test_init_with_empty_config(self):
        """Тест: инициализация с пустой конфигурацией"""
        validator = DataValidator({})
        assert validator.config == {}
    
    def test_init_creates_logger(self, config):
        """Тест: инициализация создает логгер"""
        validator = DataValidator(config)
        assert validator.logger is not None



