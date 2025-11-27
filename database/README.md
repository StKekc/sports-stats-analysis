# Миграция данных в PostgreSQL

Полная система миграции футбольной статистики из CSV файлов в PostgreSQL базу данных.

## 📋 Содержание

- [Обзор](#обзор)
- [Структура проекта](#структура-проекта)
- [Требования](#требования)
- [Установка](#установка)
- [Конфигурация](#конфигурация)
- [Запуск миграции](#запуск-миграции)
- [Структура базы данных](#структура-базы-данных)
- [Использование](#использование)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Обзор

Эта система миграции предназначена для:

- **Миграции 2.5 ГБ данных** из 90 директорий (15 лиг × 6 сезонов)
- **Нормализации данных** в реляционную структуру
- **Создания индексов** для быстрых аналитических запросов
- **Обеспечения целостности данных** через foreign keys и constraints

### Что мигрируется:

- ✅ **Справочники**: Лиги, сезоны, команды, игроки
- ✅ **Матчи**: Расписание, результаты, xG статистика
- ✅ **Турнирные таблицы**: Позиции команд по сезонам
- ✅ **Командная статистика**: Владение, передачи, голы, xG
- ✅ **Игроковая статистика**: 9 типов (standard, shooting, passing, defense, possession, misc, passing_types, keepers, keepers_adv)

---

## 📁 Структура проекта

```
database/
├── README.md                   # Этот файл
├── schema.sql                  # SQL схема базы данных
├── config.yaml                 # Конфигурация ETL
├── database_manager.py         # Менеджер подключения к БД
├── etl_utils.py               # Утилиты для очистки и валидации данных
├── etl_loaders.py             # Загрузчики для команд, матчей, standings
├── player_stats_loader.py     # Загрузчик игроковой статистики
└── migrate_to_postgres.py     # Главный скрипт миграции
```

---

## 📦 Требования

### Программное обеспечение

- **Python 3.8+**
- **PostgreSQL 12+**

### Python зависимости

```bash
pip install psycopg2-binary pandas numpy pyyaml tqdm
```

Или создайте файл `requirements.txt`:

```
psycopg2-binary>=2.9.0
pandas>=1.3.0
numpy>=1.21.0
pyyaml>=5.4.0
tqdm>=4.62.0
```

И установите:

```bash
pip install -r requirements.txt
```

---

## 🚀 Установка

### 1. Установка PostgreSQL

**macOS:**
```bash
brew install postgresql@15
brew services start postgresql@15
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
```

**Windows:**
Скачайте установщик с [postgresql.org](https://www.postgresql.org/download/)

### 2. Создание базы данных и пользователя

```bash
# Подключитесь к PostgreSQL
psql postgres

# В psql выполните:
CREATE DATABASE sports_stats;
CREATE USER sports_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE sports_stats TO sports_user;

# Для PostgreSQL 15+ также нужно:
\c sports_stats
GRANT ALL ON SCHEMA public TO sports_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sports_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sports_user;

# Выход
\q
```

### 3. Установка Python зависимостей

```bash
cd /path/to/sports-stats-analysis
pip install -r database/requirements.txt
```

---

## ⚙️ Конфигурация

### 1. Настройка подключения к БД

Отредактируйте `database/config.yaml`:

```yaml
database:
  host: "localhost"
  port: 5432
  database: "sports_stats"
  user: "sports_user"
  password: "your_secure_password"  # ⚠️ ИЗМЕНИТЕ!
```

### 2. Настройки ETL (опционально)

```yaml
etl:
  batch_size: 1000              # Размер батча для вставки
  num_workers: 4                # Количество потоков
  error_mode: "strict"          # strict или continue
  truncate_before_load: false   # ⚠️ Очистить таблицы перед загрузкой
```

### 3. Проверка путей

Убедитесь что пути корректны в `config.yaml`:

```yaml
paths:
  raw_data: "data/raw/fbref"         # Путь к raw данным
  processed_data: "data/processed"
  logs: "logs/etl"
```

---

## 🏃 Запуск миграции

### Полная миграция (рекомендуется)

```bash
cd /path/to/sports-stats-analysis

# Запуск с настройками по умолчанию
python database/migrate_to_postgres.py
```

### Миграция с параметрами

```bash
# Пропустить создание схемы (если уже создана)
python database/migrate_to_postgres.py --skip-schema

# Свой конфиг
python database/migrate_to_postgres.py --config my_config.yaml

# Свои пути
python database/migrate_to_postgres.py \
  --raw-data data/raw/fbref \
  --schema database/schema.sql \
  --leagues-config src/config/leagues.yaml
```

### Пошаговая миграция (для отладки)

```bash
# 1. Создать схему
psql -U sports_user -d sports_stats -f database/schema.sql

# 2. Запустить миграцию без создания схемы
python database/migrate_to_postgres.py --skip-schema
```

---

## 🗄️ Структура базы данных

### Справочные таблицы (Dimensions)

- **leagues** - Лиги (epl, laliga, bundesliga, etc.)
- **seasons** - Сезоны (2019-2020, 2023-2024, etc.)
- **teams** - Команды
- **players** - Игроки

### Факты (Facts)

- **matches** - Матчи с результатами и xG
- **standings** - Турнирные таблицы
- **team_season_stats** - Командная статистика по сезонам

### Игроковая статистика

- **player_team_seasons** - Связь игрок-команда-сезон
- **player_standard_stats** - Основная статистика
- **player_shooting_stats** - Удары
- **player_passing_stats** - Передачи
- **player_passing_types_stats** - Типы передач
- **player_defense_stats** - Оборона
- **player_possession_stats** - Владение и дриблинг
- **player_misc_stats** - Разное (фолы, карточки)
- **player_keeper_stats** - Статистика вратарей
- **player_keeper_adv_stats** - Продвинутая статистика вратарей

### Аналитические представления (Views)

- **v_home_away_win_rate** - Процент побед дома/на выезде
- **v_team_style_metrics** - Метрики стиля игры для кластеризации
- **v_team_form_by_month** - Динамика формы команды по месяцам
- **v_head_to_head** - Статистика личных встреч

### Диаграмма связей

```
leagues (1) ----< (N) matches
    |                  |
    |                  v
    |              home_team, away_team
    |                  |
    v                  v
seasons (1) ----< (N) teams
    |
    v
player_team_seasons ----< player_*_stats
```

---

## 💡 Использование

### Примеры SQL запросов

#### 1. Топ-10 бомбардиров EPL 2023-2024

```sql
SELECT 
    p.player_name,
    t.team_name,
    pss.goals,
    pss.matches_played,
    pss.goals_per_90
FROM player_standard_stats pss
JOIN player_team_seasons pts ON pts.player_team_season_id = pss.player_team_season_id
JOIN players p ON p.player_id = pts.player_id
JOIN teams t ON t.team_id = pts.team_id
JOIN leagues l ON l.league_id = pts.league_id
JOIN seasons s ON s.season_id = pts.season_id
WHERE l.league_code = 'epl' 
  AND s.season_code = '2023-2024'
ORDER BY pss.goals DESC
LIMIT 10;
```

#### 2. Процент побед дома vs на выезде

```sql
SELECT * 
FROM v_home_away_win_rate 
WHERE league_name = 'Premier League' 
  AND season_code = '2023-2024'
ORDER BY home_win_rate_pct DESC;
```

#### 3. Кластеризация команд по стилю игры

```sql
SELECT 
    team_name,
    possession_pct,
    progressive_passes,
    progressive_carries,
    total_cards,
    xg_per_90
FROM v_team_style_metrics
WHERE league_name = 'Premier League'
  AND season_code = '2023-2024'
ORDER BY possession_pct DESC;
```

#### 4. Форма команды последние 5 матчей

```sql
SELECT 
    m.match_date,
    CASE WHEN m.home_team_id = 1 THEN 'Home' ELSE 'Away' END as venue,
    CASE 
        WHEN m.home_team_id = 1 THEN away.team_name 
        ELSE home.team_name 
    END as opponent,
    CASE 
        WHEN m.home_team_id = 1 THEN m.home_goals || '-' || m.away_goals
        ELSE m.away_goals || '-' || m.home_goals
    END as score,
    CASE 
        WHEN (m.home_team_id = 1 AND m.home_goals > m.away_goals) OR
             (m.away_team_id = 1 AND m.away_goals > m.home_goals)
        THEN 'W'
        WHEN m.home_goals = m.away_goals THEN 'D'
        ELSE 'L'
    END as result
FROM matches m
JOIN teams home ON home.team_id = m.home_team_id
JOIN teams away ON away.team_id = m.away_team_id
WHERE 1 IN (m.home_team_id, m.away_team_id)  -- ID команды
ORDER BY m.match_date DESC
LIMIT 5;
```

#### 5. Самый неудобный соперник

```sql
SELECT 
    opponent_name,
    matches_played,
    wins,
    draws,
    losses,
    win_rate_pct,
    goals_for,
    goals_against
FROM v_head_to_head
WHERE team_name = 'Liverpool'
  AND matches_played >= 5
ORDER BY win_rate_pct ASC
LIMIT 5;
```

### Python примеры

```python
import psycopg2
import pandas as pd

# Подключение
conn = psycopg2.connect(
    host="localhost",
    database="sports_stats",
    user="sports_user",
    password="your_password"
)

# Загрузка данных в pandas
query = """
    SELECT * FROM v_team_style_metrics 
    WHERE season_code = '2023-2024'
"""
df = pd.read_sql(query, conn)

# Кластеризация
from sklearn.cluster import KMeans

features = ['possession_pct', 'progressive_passes', 'xg_per_90']
X = df[features].fillna(0)

kmeans = KMeans(n_clusters=3, random_state=42)
df['cluster'] = kmeans.fit_predict(X)

print(df.groupby('cluster')['team_name'].apply(list))
```

---

## 🔧 Troubleshooting

### Проблема: "could not connect to server"

**Решение:**
```bash
# Проверьте что PostgreSQL запущен
brew services list  # macOS
sudo systemctl status postgresql  # Linux

# Запустите если не запущен
brew services start postgresql@15  # macOS
sudo systemctl start postgresql  # Linux
```

### Проблема: "permission denied for schema public"

**Решение:**
```sql
-- Подключитесь к БД и выполните:
\c sports_stats
GRANT ALL ON SCHEMA public TO sports_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sports_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sports_user;
```

### Проблема: "relation already exists"

**Решение:**
```bash
# Используйте --skip-schema если таблицы уже созданы
python database/migrate_to_postgres.py --skip-schema
```

### Проблема: Миграция слишком долгая

**Решение:**

1. Увеличьте `batch_size` в `config.yaml`:
   ```yaml
   etl:
     batch_size: 5000  # Вместо 1000
   ```

2. Отключите индексы на время загрузки (редактируйте `schema.sql`):
   - Закомментируйте CREATE INDEX команды
   - Создайте индексы после загрузки вручную

3. Настройте PostgreSQL для bulk loading (`postgresql.conf`):
   ```
   maintenance_work_mem = 1GB
   checkpoint_completion_target = 0.9
   wal_buffers = 16MB
   ```

### Проблема: "File not found" для некоторых CSV

**Решение:**

Это нормально - не все лиги/сезоны имеют все типы статистики. Миграция пропустит отсутствующие файлы с warning в логе.

---

## 📊 Ожидаемые результаты

После успешной миграции вы должны увидеть примерно:

```
MIGRATION STATISTICS
================================================================================
Leagues:              15
Seasons:               6
Teams:             ~400
Players:         ~10,000
Matches:        ~30,000
Standings:         ~120
Team Stats:        ~120
Player Stats:
  Standard:      ~10,000
  Shooting:       ~8,000
  Passing:        ~8,000
  Defense:        ~8,000
  Possession:     ~8,000
================================================================================
```

---

## 🎓 Для ваших аналитических задач

### 1. Кластеризация команд по стилю игры

```python
# Используйте v_team_style_metrics
# Фичи: possession_pct, progressive_passes, total_cards, xg_per_90
```

### 2. Процент побед дома/на выезде

```sql
SELECT * FROM v_home_away_win_rate;
```

### 3. Динамика результатов по месяцам

```sql
SELECT * FROM v_team_form_by_month 
WHERE team_name = 'Manchester City'
ORDER BY month;
```

### 4. Самый неудобный соперник

```sql
SELECT * FROM v_head_to_head
WHERE team_name = 'Arsenal'
ORDER BY win_rate_pct ASC;
```

### 5. Прогноз исхода матча

```python
# Соберите фичи из:
# - v_team_form_by_month (последние N месяцев)
# - team_season_stats (текущие показатели)
# - v_head_to_head (история встреч)
# - v_home_away_win_rate (фактор дома/выезда)

# Используйте ML модель (RandomForest, XGBoost)
```

---

## 📝 Дополнительные ресурсы

- **Schema ERD**: См. комментарии в `schema.sql`
- **Логи**: Проверьте `logs/etl/migration_*.log` при ошибках
- **Performance**: Используйте `EXPLAIN ANALYZE` для оптимизации запросов

---

## 🤝 Поддержка

При проблемах:

1. Проверьте логи в `logs/etl/`
2. Убедитесь что все пути в `config.yaml` корректны
3. Проверьте что PostgreSQL запущен и доступен
4. Убедитесь что установлены все Python зависимости

---

## 📄 Лицензия

Этот код является частью проекта sports-stats-analysis.

---

**Удачи с миграцией! 🚀**

