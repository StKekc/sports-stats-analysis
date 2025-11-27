# 🚀 Быстрый старт миграции в PostgreSQL

## За 5 минут до запуска

### 1. Установите PostgreSQL

**macOS:**
```bash
brew install postgresql@15
brew services start postgresql@15
```

**Linux:**
```bash
sudo apt install postgresql
sudo systemctl start postgresql
```

### 2. Создайте БД

```bash
psql postgres -c "CREATE DATABASE sports_stats;"
psql postgres -c "CREATE USER sports_user WITH PASSWORD 'mypassword';"
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE sports_stats TO sports_user;"
```

Для PostgreSQL 15+:
```bash
psql sports_stats -c "GRANT ALL ON SCHEMA public TO sports_user;"
```

### 3. Установите зависимости

```bash
cd /path/to/sports-stats-analysis
pip3 install psycopg2-binary pandas numpy pyyaml tqdm
```

### 4. Настройте конфигурацию

Отредактируйте `database/config.yaml`:
```yaml
database:
  host: "localhost"
  port: 5432
  database: "sports_stats"
  user: "sports_user"
  password: "mypassword"  # ⚠️ Ваш пароль!
```

### 5. Запустите миграцию

```bash
python3 database/migrate_to_postgres.py
```

## ⏱️ Ожидаемое время

- Создание схемы: ~10 секунд
- Загрузка справочников: ~5 секунд
- Загрузка матчей и статистики: **20-40 минут** (зависит от железа)
- Оптимизация: ~5 минут

**Итого: ~30-45 минут для 2.5 ГБ данных**

## ✅ Проверка результата

```bash
# Подключитесь к БД
psql -U sports_user -d sports_stats

# Проверьте количество записей
SELECT 'leagues' as table_name, COUNT(*) FROM leagues
UNION ALL SELECT 'teams', COUNT(*) FROM teams
UNION ALL SELECT 'matches', COUNT(*) FROM matches
UNION ALL SELECT 'players', COUNT(*) FROM players;

# Должно быть примерно:
# leagues:   15
# teams:     ~400
# matches:   ~30,000
# players:   ~10,000
```

## 🎯 Первые запросы

```sql
-- Топ бомбардиров EPL 2023-2024
SELECT 
    p.player_name,
    t.team_name,
    pss.goals,
    pss.xg
FROM player_standard_stats pss
JOIN player_team_seasons pts ON pts.player_team_season_id = pss.player_team_season_id
JOIN players p ON p.player_id = pts.player_id
JOIN teams t ON t.team_id = pts.team_id
JOIN leagues l ON l.league_id = pts.league_id
JOIN seasons s ON s.season_id = pts.season_id
WHERE l.league_code = 'epl' AND s.season_code = '2023-2024'
ORDER BY pss.goals DESC
LIMIT 10;
```

## 🔧 Если что-то пошло не так

```bash
# Проверьте логи
tail -f logs/etl/migration_*.log

# Проверьте что PostgreSQL запущен
pg_isready

# Пересоздайте БД (если нужно)
dropdb -U postgres sports_stats
createdb -U postgres sports_stats
```

## 📚 Полная документация

См. [README.md](README.md) для подробной информации.

---

**Готово! Теперь у вас есть полная БД для анализа 🎉**

