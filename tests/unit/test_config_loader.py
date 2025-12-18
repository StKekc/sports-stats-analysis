"""
Тесты для config_loader.py
"""

import pytest
import yaml
import tempfile
import pathlib
from unittest.mock import patch, mock_open
from src.modules.config_loader import load_league_config


class TestLoadLeagueConfig:
    """Тесты для функции load_league_config"""
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  epl:
    name: "Premier League"
    country: "England"
    url: "https://fbref.com/en/comps/9/"
  laliga:
    name: "La Liga"
    country: "Spain"
    url: "https://fbref.com/en/comps/12/"
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_epl_config(self, mock_file):
        """Тест: загрузка конфигурации EPL"""
        config = load_league_config('epl')
        assert config['name'] == "Premier League"
        assert config['country'] == "England"
        assert 'fbref.com' in config['url']
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  epl:
    name: "Premier League"
    country: "England"
  laliga:
    name: "La Liga"
    country: "Spain"
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_laliga_config(self, mock_file):
        """Тест: загрузка конфигурации La Liga"""
        config = load_league_config('laliga')
        assert config['name'] == "La Liga"
        assert config['country'] == "Spain"
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  epl:
    name: "Premier League"
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_nonexistent_league_raises_error(self, mock_file):
        """Тест: загрузка несуществующей лиги вызывает ошибку"""
        with pytest.raises(ValueError) as exc_info:
            load_league_config('nonexistent')
        assert 'не найдена' in str(exc_info.value) or 'not found' in str(exc_info.value).lower()
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  bundesliga:
    name: "Bundesliga"
    country: "Germany"
    seasons:
      - "2022-2023"
      - "2023-2024"
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_config_with_nested_structure(self, mock_file):
        """Тест: загрузка конфигурации с вложенной структурой"""
        config = load_league_config('bundesliga')
        assert config['name'] == "Bundesliga"
        assert 'seasons' in config
        assert len(config['seasons']) == 2
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  seriea:
    name: "Serie A"
    country: "Italy"
    tier: 1
    teams: 20
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_config_returns_dict(self, mock_file):
        """Тест: функция возвращает словарь"""
        config = load_league_config('seriea')
        assert isinstance(config, dict)
        assert config['tier'] == 1
        assert config['teams'] == 20
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  ligue1:
    name: "Ligue 1"
    country: "France"
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_config_multiple_calls(self, mock_file):
        """Тест: множественные вызовы работают корректно"""
        config1 = load_league_config('ligue1')
        config2 = load_league_config('ligue1')
        assert config1 == config2
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  test_league:
    name: "Test League"
    active: true
    founded: 1992
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_config_with_boolean_and_int(self, mock_file):
        """Тест: загрузка конфигурации с boolean и int значениями"""
        config = load_league_config('test_league')
        assert config['active'] is True
        assert config['founded'] == 1992
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  empty_league: {}
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_empty_league_config(self, mock_file):
        """Тест: загрузка пустой конфигурации лиги"""
        config = load_league_config('empty_league')
        assert config == {}
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  league_with_list:
    name: "League"
    divisions: ["A", "B", "C"]
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_config_with_list(self, mock_file):
        """Тест: загрузка конфигурации со списком"""
        config = load_league_config('league_with_list')
        assert len(config['divisions']) == 3
        assert 'A' in config['divisions']


class TestConfigLoaderEdgeCases:
    """Тесты граничных случаев"""
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  epl:
    name: "Premier League"
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_with_lowercase_key(self, mock_file):
        """Тест: загрузка с ключом в нижнем регистре"""
        config = load_league_config('epl')
        assert config['name'] == "Premier League"
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  test123:
    name: "Test League 123"
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_with_numeric_in_key(self, mock_file):
        """Тест: загрузка с числами в ключе"""
        config = load_league_config('test123')
        assert config['name'] == "Test League 123"
    
    @patch('builtins.open', new_callable=mock_open, read_data="""
leagues:
  league:
    name: "League"
    special_char: "Value with: special, characters!"
""")
    @patch('src.modules.config_loader.CONFIG_PATH', 'fake_path.yaml')
    def test_load_config_with_special_characters(self, mock_file):
        """Тест: загрузка конфигурации со специальными символами"""
        config = load_league_config('league')
        assert '!' in config['special_char']



