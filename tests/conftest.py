"""
Общие фикстуры для всех тестов
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path


@pytest.fixture
def sample_match_data():
    """Пример данных матча для тестов"""
    return {
        'match_date': '2023-12-25',
        'home_team_id': 1,
        'away_team_id': 2,
        'home_goals': 3,
        'away_goals': 1,
        'venue': 'Emirates Stadium',
        'attendance': 60000
    }


@pytest.fixture
def sample_player_data():
    """Пример данных игрока для тестов"""
    return {
        'player_name': 'Bukayo Saka',
        'born': 2001,
        'nation': 'ENG',
        'position': 'FW',
        'team_id': 1
    }


@pytest.fixture
def sample_team_data():
    """Пример данных команды для тестов"""
    return {
        'team_id': 1,
        'team_name': 'Arsenal',
        'matches_played': 20,
        'wins': 15,
        'draws': 3,
        'losses': 2,
        'points': 48
    }


@pytest.fixture
def sample_fixtures_df():
    """DataFrame с фикстурами для тестов"""
    return pd.DataFrame({
        'Date': ['2023-12-25', '2023-12-26', '2023-12-27'],
        'Time': ['20:00', '15:00', '17:30'],
        'Home': ['Arsenal', 'Chelsea', 'Liverpool'],
        'Away': ['Chelsea', 'Arsenal', 'Man City'],
        'Score': ['3-1', '0-2', '1-1']
    })


@pytest.fixture
def sample_standings_df():
    """DataFrame с турнирной таблицей для тестов"""
    return pd.DataFrame({
        'Squad': ['Arsenal', 'Man City', 'Liverpool'],
        'MP': [20, 20, 20],
        'W': [15, 14, 13],
        'D': [3, 4, 5],
        'L': [2, 2, 2],
        'Pts': [48, 46, 44]
    })


@pytest.fixture
def sample_player_stats_df():
    """DataFrame со статистикой игроков для тестов"""
    return pd.DataFrame({
        'Player': ['Saka', 'Odegaard', 'Martinelli'],
        'Nation': ['ENG', 'NOR', 'BRA'],
        'Pos': ['FW', 'MF', 'FW'],
        'Age': [22, 25, 23],
        'Min': [1800, 1700, 1600],
        'Goals': [15, 12, 10],
        'Assists': [8, 10, 6]
    })


@pytest.fixture
def temp_csv_file(tmp_path):
    """Временный CSV файл для тестов"""
    csv_path = tmp_path / "test_data.csv"
    df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def temp_yaml_file(tmp_path):
    """Временный YAML файл для тестов"""
    yaml_path = tmp_path / "test_config.yaml"
    yaml_content = """
leagues:
  test_league:
    name: "Test League"
    country: "Test Country"
"""
    yaml_path.write_text(yaml_content)
    return yaml_path


@pytest.fixture
def mock_db_config():
    """Мок конфигурации БД для тестов"""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_password',
        'connection_timeout': 30
    }


@pytest.fixture
def mock_etl_config():
    """Мок конфигурации ETL для тестов"""
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
        },
        'field_mappings': {
            'fixtures': {
                'Date': 'match_date',
                'Time': 'match_time',
                'Home': 'home_team',
                'Away': 'away_team'
            },
            'standings': {
                'Squad': 'team_name',
                'MP': 'matches_played',
                'Pts': 'points'
            }
        }
    }


@pytest.fixture
def sample_html_table():
    """Пример HTML таблицы для тестов"""
    return """
    <html>
        <table>
            <thead>
                <tr>
                    <th>Team</th>
                    <th>Points</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>Arsenal</td>
                    <td>48</td>
                </tr>
                <tr>
                    <td>Chelsea</td>
                    <td>42</td>
                </tr>
            </tbody>
        </table>
    </html>
    """


@pytest.fixture
def sample_multiindex_df():
    """DataFrame с MultiIndex колонками для тестов"""
    arrays = [
        ['Performance', 'Performance', 'Expected'],
        ['Goals', 'Assists', 'xG']
    ]
    columns = pd.MultiIndex.from_arrays(arrays)
    return pd.DataFrame([[15, 8, 12.5], [10, 6, 8.3]], columns=columns)


@pytest.fixture(autouse=True)
def reset_test_state():
    """Автоматическая фикстура для сброса состояния перед каждым тестом"""
    # Setup
    yield
    # Teardown - здесь можно добавить код очистки если нужно


@pytest.fixture
def sample_dates():
    """Различные форматы дат для тестов"""
    return [
        '2023-12-25',           # ISO format
        '25/12/2023',           # DD/MM/YYYY
        '12/25/2023',           # MM/DD/YYYY
        '25.12.2023',           # DD.MM.YYYY
        '2023.12.25',           # YYYY.MM.DD
    ]


@pytest.fixture
def sample_scores():
    """Различные форматы счетов для тестов"""
    return [
        '4-1',
        '0-0',
        '3 - 2',
        '4:1',
        '2 : 3',
        '4–1',  # en dash
        '4—1',  # em dash
    ]


@pytest.fixture
def sample_nation_codes():
    """Примеры кодов стран для тестов"""
    return [
        ('eng ENG', 'ENG'),
        ('ESP', 'ESP'),
        ('FRA France', 'FRA'),
        ('england ENG', 'ENG'),
        ('abc', 'ABC'),
    ]


@pytest.fixture
def empty_dataframe():
    """Пустой DataFrame для тестов"""
    return pd.DataFrame()


@pytest.fixture
def dataframe_with_nulls():
    """DataFrame с различными типами null значений"""
    return pd.DataFrame({
        'col1': [1, None, 3, np.nan, 5],
        'col2': ['a', '', 'c', 'N/A', 'e'],
        'col3': [1.1, 2.2, None, np.nan, 5.5]
    })


@pytest.fixture
def dataframe_with_duplicates():
    """DataFrame с дубликатами для тестов"""
    return pd.DataFrame({
        'team': ['Arsenal', 'Arsenal', 'Chelsea', 'Chelsea'],
        'season': ['2022', '2023', '2022', '2023'],
        'points': [84, 89, 74, 75]
    })



