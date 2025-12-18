"""
Тесты для работы с регулярными выражениями
"""

import pytest
import re


class TestScorePatterns:
    """Тесты для паттернов счета матчей"""
    
    def test_simple_score_pattern(self):
        """Тест: простой паттерн счета"""
        pattern = r'(\d+)-(\d+)'
        match = re.match(pattern, '4-1')
        assert match is not None
        assert match.group(1) == '4'
        assert match.group(2) == '1'
    
    def test_score_with_spaces(self):
        """Тест: счет с пробелами"""
        pattern = r'(\d+)\s*-\s*(\d+)'
        match = re.match(pattern, '4 - 1')
        assert match is not None
    
    def test_score_zero_zero(self):
        """Тест: счет 0-0"""
        pattern = r'(\d+)-(\d+)'
        match = re.match(pattern, '0-0')
        assert match is not None
        assert match.group(1) == '0'
    
    def test_high_score(self):
        """Тест: высокий счет"""
        pattern = r'(\d+)-(\d+)'
        match = re.match(pattern, '10-5')
        assert match is not None
    
    def test_score_with_colon(self):
        """Тест: счет с двоеточием"""
        pattern = r'(\d+)\s*:\s*(\d+)'
        match = re.match(pattern, '4:1')
        assert match is not None


class TestDatePatterns:
    """Тесты для паттернов дат"""
    
    def test_iso_date_pattern(self):
        """Тест: ISO формат даты"""
        pattern = r'(\d{4})-(\d{2})-(\d{2})'
        match = re.match(pattern, '2023-12-25')
        assert match is not None
        assert match.group(1) == '2023'
        assert match.group(2) == '12'
        assert match.group(3) == '25'
    
    def test_slash_date_pattern(self):
        """Тест: дата со слешами"""
        pattern = r'(\d{2})/(\d{2})/(\d{4})'
        match = re.match(pattern, '25/12/2023')
        assert match is not None
    
    def test_dot_date_pattern(self):
        """Тест: дата с точками"""
        pattern = r'(\d{2})\.(\d{2})\.(\d{4})'
        match = re.match(pattern, '25.12.2023')
        assert match is not None


class TestSeasonPatterns:
    """Тесты для паттернов сезонов"""
    
    def test_season_two_years(self):
        """Тест: сезон с двумя годами"""
        pattern = r'(\d{4})-(\d{4})'
        match = re.match(pattern, '2023-2024')
        assert match is not None
        assert match.group(1) == '2023'
        assert match.group(2) == '2024'
    
    def test_season_single_year(self):
        """Тест: сезон с одним годом"""
        pattern = r'(\d{4})'
        match = re.match(pattern, '2024')
        assert match is not None
        assert match.group(1) == '2024'
    
    def test_season_in_directory_name(self):
        """Тест: сезон в имени директории"""
        pattern = r'^([a-z0-9]+)_(.+)$'
        match = re.match(pattern, 'epl_2023-2024')
        assert match is not None
        assert match.group(1) == 'epl'
        assert match.group(2) == '2023-2024'


class TestNationCodePatterns:
    """Тесты для паттернов кодов стран"""
    
    def test_three_letter_code(self):
        """Тест: 3-буквенный код"""
        pattern = r'\b([A-Z]{3})\b'
        match = re.search(pattern, 'eng ENG')
        assert match is not None
        assert match.group(1) == 'ENG'
    
    def test_code_at_end(self):
        """Тест: код в конце строки"""
        pattern = r'\b([A-Z]{3})\b'
        match = re.search(pattern, 'england ENG')
        assert match is not None
    
    def test_standalone_code(self):
        """Тест: отдельный код"""
        pattern = r'\b([A-Z]{3})\b'
        match = re.search(pattern, 'ESP')
        assert match is not None


class TestTimePatterns:
    """Тесты для паттернов времени"""
    
    def test_simple_time(self):
        """Тест: простое время HH:MM"""
        pattern = r'(\d{2}):(\d{2})'
        match = re.match(pattern, '20:30')
        assert match is not None
        assert match.group(1) == '20'
        assert match.group(2) == '30'
    
    def test_time_with_seconds(self):
        """Тест: время с секундами"""
        pattern = r'(\d{2}):(\d{2}):(\d{2})'
        match = re.match(pattern, '20:30:45')
        assert match is not None
    
    def test_time_with_parentheses(self):
        """Тест: время с информацией в скобках"""
        text = '20:00 (21:00)'
        cleaned = re.sub(r'\s*\([^)]*\)', '', text)
        assert cleaned == '20:00'


class TestColumnNamePatterns:
    """Тесты для паттернов имен колонок"""
    
    def test_remove_special_chars(self):
        """Тест: удаление специальных символов"""
        name = 'Goals+Assists'
        cleaned = re.sub(r'[^\w]+', '_', name.lower())
        assert 'goals' in cleaned
        assert 'assists' in cleaned
    
    def test_normalize_spaces(self):
        """Тест: нормализация пробелов"""
        name = '  Multiple   Spaces  '
        cleaned = ' '.join(name.split())
        assert cleaned == 'Multiple Spaces'
    
    def test_remove_multiple_underscores(self):
        """Тест: удаление множественных подчеркиваний"""
        name = 'col___name'
        cleaned = re.sub(r'_+', '_', name)
        assert cleaned == 'col_name'
    
    def test_strip_underscores(self):
        """Тест: удаление подчеркиваний с краев"""
        name = '_col_name_'
        cleaned = name.strip('_')
        assert cleaned == 'col_name'


class TestURLPatterns:
    """Тесты для паттернов URL"""
    
    def test_find_comps_url(self):
        """Тест: поиск URL с /comps/"""
        text = 'Link: /en/comps/9/stats/players'
        pattern = r'/comps/\d+'
        match = re.search(pattern, text)
        assert match is not None
    
    def test_extract_comp_id(self):
        """Тест: извлечение comp_id"""
        url = '/en/comps/9/stats'
        pattern = r'/comps/(\d+)'
        match = re.search(pattern, url)
        assert match is not None
        assert match.group(1) == '9'
    
    def test_full_url_or_relative(self):
        """Тест: полный или относительный URL"""
        url = '/en/comps/9/stats'
        if url.startswith('/'):
            full_url = 'https://fbref.com' + url
        else:
            full_url = url
        assert full_url.startswith('https://')


class TestStringValidation:
    """Тесты для валидации строк"""
    
    def test_alphanumeric_check(self):
        """Тест: проверка на буквенно-цифровые"""
        assert re.match(r'^[a-zA-Z0-9]+$', 'Arsenal123')
        assert not re.match(r'^[a-zA-Z0-9]+$', 'Arsenal FC')
    
    def test_contains_digit(self):
        """Тест: содержит цифру"""
        assert re.search(r'\d', 'Team 1')
        assert not re.search(r'\d', 'Arsenal')
    
    def test_starts_with_letter(self):
        """Тест: начинается с буквы"""
        assert re.match(r'^[a-zA-Z]', 'Arsenal')
        assert not re.match(r'^[a-zA-Z]', '1Arsenal')
    
    def test_email_like_pattern(self):
        """Тест: email-подобный паттерн"""
        pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        assert re.match(pattern, 'test@example.com')
        assert not re.match(pattern, 'invalid-email')


class TestReplaceOperations:
    """Тесты для операций замены"""
    
    def test_replace_hyphen_with_underscore(self):
        """Тест: замена дефиса на подчеркивание"""
        text = 'team-name'
        result = text.replace('-', '_')
        assert result == 'team_name'
    
    def test_remove_whitespace(self):
        """Тест: удаление пробелов"""
        text = 'Some Text With Spaces'
        result = re.sub(r'\s+', '', text)
        assert result == 'SomeTextWithSpaces'
    
    def test_replace_multiple_patterns(self):
        """Тест: замена нескольких паттернов"""
        text = 'G+A'
        result = text.replace('G+A', 'g_plus_a')
        assert result == 'g_plus_a'
    
    def test_conditional_replace(self):
        """Тест: условная замена"""
        text = 'per 90 minutes'
        if 'per 90' in text:
            result = text.replace('per 90 minutes', 'per90')
        assert result == 'per90'



