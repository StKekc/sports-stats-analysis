DROP TABLE IF EXISTS player_keeper_adv_stats CASCADE;
DROP TABLE IF EXISTS player_keeper_stats CASCADE;
DROP TABLE IF EXISTS player_misc_stats CASCADE;
DROP TABLE IF EXISTS player_passing_types_stats CASCADE;
DROP TABLE IF EXISTS player_possession_stats CASCADE;
DROP TABLE IF EXISTS player_defense_stats CASCADE;
DROP TABLE IF EXISTS player_passing_stats CASCADE;
DROP TABLE IF EXISTS player_shooting_stats CASCADE;
DROP TABLE IF EXISTS player_standard_stats CASCADE;
DROP TABLE IF EXISTS player_team_seasons CASCADE;
DROP TABLE IF EXISTS team_season_stats CASCADE;
DROP TABLE IF EXISTS standings CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS seasons CASCADE;
DROP TABLE IF EXISTS leagues CASCADE;

-- Лиги
CREATE TABLE leagues (
    league_id SERIAL PRIMARY KEY,
    league_code VARCHAR(20) UNIQUE NOT NULL,
    league_name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    comp_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE leagues IS 'Справочник футбольных лиг';
COMMENT ON COLUMN leagues.league_code IS 'Код лиги (epl, laliga, bundesliga и т.д.)';
COMMENT ON COLUMN leagues.comp_id IS 'ID лиги в системе FBref';

-- Сезоны
CREATE TABLE seasons (
    season_id SERIAL PRIMARY KEY,
    season_code VARCHAR(20) UNIQUE NOT NULL,
    start_year INTEGER,
    end_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE seasons IS 'Справочник сезонов';
COMMENT ON COLUMN seasons.season_code IS 'Код сезона (2019-2020, 2023-2024 и т.д.)';

-- Команды
CREATE TABLE teams (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    normalized_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_teams_normalized_name ON teams(normalized_name);

COMMENT ON TABLE teams IS 'Справочник команд';
COMMENT ON COLUMN teams.normalized_name IS 'Нормализованное имя для избежания дубликатов (lowercase, trimmed)';

-- Игроки
CREATE TABLE players (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(200) NOT NULL,
    nation VARCHAR(10),
    born INTEGER,
    position VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_players_name ON players(player_name);
CREATE INDEX idx_players_nation ON players(nation);
CREATE UNIQUE INDEX idx_players_unique ON players(player_name, COALESCE(born, 0));

COMMENT ON TABLE players IS 'Справочник игроков';
COMMENT ON COLUMN players.nation IS 'Код страны (3 буквы, например: ENG, BRA, ARG)';
COMMENT ON COLUMN players.born IS 'Год рождения';

-- ============================================================================
-- FACT TABLES (Факты - Матчи и результаты)
-- ============================================================================

-- Матчи
CREATE TABLE matches (
    match_id SERIAL PRIMARY KEY,
    league_id INTEGER NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
    season_id INTEGER NOT NULL REFERENCES seasons(season_id) ON DELETE CASCADE,
    
    -- Основная информация
    match_week INTEGER,
    match_date DATE NOT NULL,
    match_time TIME,
    day_of_week VARCHAR(10),
    
    -- Команды
    home_team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    away_team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    
    -- Результат
    score VARCHAR(20),
    home_goals INTEGER,
    away_goals INTEGER,
    home_xg DECIMAL(5,2),
    away_xg DECIMAL(5,2),
    
    -- Дополнительная информация
    venue VARCHAR(200),
    referee VARCHAR(100),
    attendance INTEGER,
    match_report_url TEXT,
    notes TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraint для предотвращения дубликатов
    CONSTRAINT unique_match UNIQUE(league_id, season_id, match_date, home_team_id, away_team_id),
    -- Constraint что команда не может играть сама с собой
    CONSTRAINT check_different_teams CHECK (home_team_id != away_team_id)
);

-- Индексы для оптимизации запросов
CREATE INDEX idx_matches_league_season ON matches(league_id, season_id);
CREATE INDEX idx_matches_date ON matches(match_date);
CREATE INDEX idx_matches_home_team ON matches(home_team_id);
CREATE INDEX idx_matches_away_team ON matches(away_team_id);
CREATE INDEX idx_matches_week ON matches(match_week);
CREATE INDEX idx_matches_season_week ON matches(season_id, match_week);

COMMENT ON TABLE matches IS 'Матчи - основная таблица фактов для анализа';
COMMENT ON COLUMN matches.match_week IS 'Номер тура';
COMMENT ON COLUMN matches.home_xg IS 'Expected Goals домашней команды';
COMMENT ON COLUMN matches.away_xg IS 'Expected Goals гостевой команды';


CREATE TABLE standings (
    standing_id SERIAL PRIMARY KEY,
    league_id INTEGER NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
    season_id INTEGER NOT NULL REFERENCES seasons(season_id) ON DELETE CASCADE,
    team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    
    -- Позиция
    rank INTEGER NOT NULL,
    
    -- Статистика
    matches_played INTEGER,
    wins INTEGER,
    draws INTEGER,
    losses INTEGER,
    goals_for INTEGER,
    goals_against INTEGER,
    goal_difference INTEGER,
    points INTEGER,
    points_per_match DECIMAL(4,2),
    
    -- Expected Goals
    xg DECIMAL(6,2),
    xga DECIMAL(6,2),
    xgd DECIMAL(6,2),
    xgd_per_90 DECIMAL(4,2),
    
    -- Дополнительно
    attendance INTEGER,
    top_scorer VARCHAR(100),
    goalkeeper VARCHAR(100),
    notes TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_standing UNIQUE(league_id, season_id, team_id)
);

CREATE INDEX idx_standings_league_season ON standings(league_id, season_id);
CREATE INDEX idx_standings_team ON standings(team_id);
CREATE INDEX idx_standings_rank ON standings(league_id, season_id, rank);

COMMENT ON TABLE standings IS 'Турнирная таблица на конец сезона';
COMMENT ON COLUMN standings.xg IS 'Ожидаемые забитые голы за сезон';
COMMENT ON COLUMN standings.xga IS 'Ожидаемые пропущенные голы за сезон';
COMMENT ON COLUMN standings.xgd IS 'Разница expected goals';


CREATE TABLE team_season_stats (
    team_season_id SERIAL PRIMARY KEY,
    league_id INTEGER NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
    season_id INTEGER NOT NULL REFERENCES seasons(season_id) ON DELETE CASCADE,
    team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    
    -- Основная статистика
    players_used INTEGER,
    avg_age DECIMAL(4,1),
    possession_pct DECIMAL(4,1),
    matches_played INTEGER,
    starts INTEGER,
    minutes INTEGER,
    ninety_s DECIMAL(5,1),
    
    -- Атака
    goals INTEGER,
    assists INTEGER,
    goals_assists INTEGER,
    goals_non_penalty INTEGER,
    penalties INTEGER,
    penalty_attempts INTEGER,
    
    -- xG метрики
    xg DECIMAL(6,2),
    npxg DECIMAL(6,2),
    xag DECIMAL(6,2),
    npxg_xag DECIMAL(6,2),
    
    -- Дисциплина
    yellow_cards INTEGER,
    red_cards INTEGER,
    
    -- Прогрессивная игра
    progressive_carries INTEGER,
    progressive_passes INTEGER,
    
    -- Метрики на 90 мин
    goals_per_90 DECIMAL(4,2),
    assists_per_90 DECIMAL(4,2),
    goals_assists_per_90 DECIMAL(4,2),
    goals_non_penalty_per_90 DECIMAL(4,2),
    goals_assists_non_penalty_per_90 DECIMAL(4,2),
    xg_per_90 DECIMAL(4,2),
    xag_per_90 DECIMAL(4,2),
    xg_xag_per_90 DECIMAL(4,2),
    npxg_per_90 DECIMAL(4,2),
    npxg_xag_per_90 DECIMAL(4,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_team_season UNIQUE(league_id, season_id, team_id)
);

CREATE INDEX idx_team_season_league_season ON team_season_stats(league_id, season_id);
CREATE INDEX idx_team_season_team ON team_season_stats(team_id);

COMMENT ON TABLE team_season_stats IS 'Агрегированная статистика команды за сезон';
COMMENT ON COLUMN team_season_stats.ninety_s IS 'Количество 90-минутных отрезков';


CREATE TABLE player_team_seasons (
    player_team_season_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
    team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    league_id INTEGER NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
    season_id INTEGER NOT NULL REFERENCES seasons(season_id) ON DELETE CASCADE,
    
    age DECIMAL(4,1),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_team_season UNIQUE(player_id, team_id, league_id, season_id)
);

CREATE INDEX idx_pts_player ON player_team_seasons(player_id);
CREATE INDEX idx_pts_team_season ON player_team_seasons(team_id, season_id);
CREATE INDEX idx_pts_league_season ON player_team_seasons(league_id, season_id);

COMMENT ON TABLE player_team_seasons IS 'Связующая таблица: за какую команду играл игрок в каком сезоне';

-- ============================================================================
-- PLAYER STATISTICS (Статистика игроков)
-- ============================================================================

-- 1. Стандартная статистика
CREATE TABLE player_standard_stats (
    stat_id SERIAL PRIMARY KEY,
    player_team_season_id INTEGER NOT NULL REFERENCES player_team_seasons(player_team_season_id) ON DELETE CASCADE,
    
    -- Игровое время
    matches_played INTEGER,
    starts INTEGER,
    minutes INTEGER,
    ninety_s DECIMAL(5,1),
    
    -- Голы и ассисты
    goals INTEGER,
    assists INTEGER,
    goals_assists INTEGER,
    goals_non_penalty INTEGER,
    penalties INTEGER,
    penalty_attempts INTEGER,
    
    -- xG
    xg DECIMAL(6,2),
    npxg DECIMAL(6,2),
    xag DECIMAL(6,2),
    npxg_xag DECIMAL(6,2),
    
    -- Прогрессия
    progressive_carries INTEGER,
    progressive_passes INTEGER,
    progressive_receptions INTEGER,
    
    -- Дисциплина
    yellow_cards INTEGER,
    red_cards INTEGER,
    
    -- Метрики на 90
    goals_per_90 DECIMAL(4,2),
    assists_per_90 DECIMAL(4,2),
    goals_assists_per_90 DECIMAL(4,2),
    goals_non_penalty_per_90 DECIMAL(4,2),
    goals_assists_non_penalty_per_90 DECIMAL(4,2),
    xg_per_90 DECIMAL(4,2),
    xag_per_90 DECIMAL(4,2),
    xg_xag_per_90 DECIMAL(4,2),
    npxg_per_90 DECIMAL(4,2),
    npxg_xag_per_90 DECIMAL(4,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_standard UNIQUE(player_team_season_id)
);

CREATE INDEX idx_player_standard_pts ON player_standard_stats(player_team_season_id);

COMMENT ON TABLE player_standard_stats IS 'Стандартная статистика игроков';

-- 2. Статистика ударов
CREATE TABLE player_shooting_stats (
    stat_id SERIAL PRIMARY KEY,
    player_team_season_id INTEGER NOT NULL REFERENCES player_team_seasons(player_team_season_id) ON DELETE CASCADE,
    
    ninety_s DECIMAL(5,1),
    goals INTEGER,
    shots INTEGER,
    shots_on_target INTEGER,
    sot_pct DECIMAL(5,2),
    shots_per_90 DECIMAL(6,2),
    sot_per_90 DECIMAL(6,2),
    goals_per_shot DECIMAL(5,2),
    goals_per_sot DECIMAL(5,2),
    avg_distance DECIMAL(5,1),
    free_kicks INTEGER,
    penalties INTEGER,
    penalty_attempts INTEGER,
    xg DECIMAL(6,2),
    npxg DECIMAL(6,2),
    npxg_per_shot DECIMAL(4,2),
    goals_minus_xg DECIMAL(5,2),
    np_goals_minus_xg DECIMAL(5,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_shooting UNIQUE(player_team_season_id)
);

CREATE INDEX idx_player_shooting_pts ON player_shooting_stats(player_team_season_id);

COMMENT ON TABLE player_shooting_stats IS 'Статистика ударов и завершения атак';
COMMENT ON COLUMN player_shooting_stats.sot_pct IS 'Процент ударов в створ';

-- 3. Статистика передач
CREATE TABLE player_passing_stats (
    stat_id SERIAL PRIMARY KEY,
    player_team_season_id INTEGER NOT NULL REFERENCES player_team_seasons(player_team_season_id) ON DELETE CASCADE,
    
    ninety_s DECIMAL(5,1),
    
    -- Передачи
    passes_completed INTEGER,
    passes_attempted INTEGER,
    passes_pct DECIMAL(4,1),
    total_distance INTEGER,
    progressive_distance INTEGER,
    
    -- Короткие передачи (5-15 yards)
    short_completed INTEGER,
    short_attempted INTEGER,
    short_pct DECIMAL(4,1),
    
    -- Средние (15-30 yards)
    medium_completed INTEGER,
    medium_attempted INTEGER,
    medium_pct DECIMAL(4,1),
    
    -- Длинные (30+ yards)
    long_completed INTEGER,
    long_attempted INTEGER,
    long_pct DECIMAL(4,1),
    
    -- Креативность
    assists INTEGER,
    xag DECIMAL(6,2),
    xa DECIMAL(6,2),
    assists_minus_xag DECIMAL(5,2),
    key_passes INTEGER,
    passes_into_final_third INTEGER,
    passes_into_penalty_area INTEGER,
    crosses_into_penalty_area INTEGER,
    progressive_passes INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_passing UNIQUE(player_team_season_id)
);

CREATE INDEX idx_player_passing_pts ON player_passing_stats(player_team_season_id);

COMMENT ON TABLE player_passing_stats IS 'Статистика передач';
COMMENT ON COLUMN player_passing_stats.xa IS 'Expected Assisted Goals';

-- 4. Статистика типов передач
CREATE TABLE player_passing_types_stats (
    stat_id SERIAL PRIMARY KEY,
    player_team_season_id INTEGER NOT NULL REFERENCES player_team_seasons(player_team_season_id) ON DELETE CASCADE,
    
    ninety_s DECIMAL(5,1),
    passes_attempted INTEGER,
    passes_live INTEGER,
    passes_dead INTEGER,
    free_kicks INTEGER,
    through_balls INTEGER,
    switches INTEGER,
    crosses INTEGER,
    throw_ins INTEGER,
    corner_kicks INTEGER,
    inswinging INTEGER,
    outswinging INTEGER,
    straight INTEGER,
    passes_completed INTEGER,
    passes_offsides INTEGER,
    passes_blocked INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_passing_types UNIQUE(player_team_season_id)
);

CREATE INDEX idx_player_passing_types_pts ON player_passing_types_stats(player_team_season_id);

COMMENT ON TABLE player_passing_types_stats IS 'Детальная статистика типов передач';

-- 5. Оборонительная статистика
CREATE TABLE player_defense_stats (
    stat_id SERIAL PRIMARY KEY,
    player_team_season_id INTEGER NOT NULL REFERENCES player_team_seasons(player_team_season_id) ON DELETE CASCADE,
    
    ninety_s DECIMAL(5,1),
    
    -- Отборы
    tackles INTEGER,
    tackles_won INTEGER,
    tackles_def_3rd INTEGER,
    tackles_mid_3rd INTEGER,
    tackles_att_3rd INTEGER,
    
    -- Единоборства
    challenges INTEGER,
    challenges_pct DECIMAL(4,1),
    challenges_lost INTEGER,
    
    -- Блоки
    blocks INTEGER,
    shots_blocked INTEGER,
    passes_blocked INTEGER,
    
    -- Перехваты и прочее
    interceptions INTEGER,
    tackles_interceptions INTEGER,
    clearances INTEGER,
    errors INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_defense UNIQUE(player_team_season_id)
);

CREATE INDEX idx_player_defense_pts ON player_defense_stats(player_team_season_id);

COMMENT ON TABLE player_defense_stats IS 'Оборонительная статистика';

-- 6. Статистика владения
CREATE TABLE player_possession_stats (
    stat_id SERIAL PRIMARY KEY,
    player_team_season_id INTEGER NOT NULL REFERENCES player_team_seasons(player_team_season_id) ON DELETE CASCADE,
    
    ninety_s DECIMAL(5,1),
    
    -- Касания
    touches INTEGER,
    touches_def_pen INTEGER,
    touches_def_3rd INTEGER,
    touches_mid_3rd INTEGER,
    touches_att_3rd INTEGER,
    touches_att_pen INTEGER,
    touches_live INTEGER,
    
    -- Дриблинг
    take_ons_attempted INTEGER,
    take_ons_successful INTEGER,
    take_ons_pct DECIMAL(4,1),
    tackled_during_take_on INTEGER,
    tackled_during_take_on_pct DECIMAL(4,1),
    
    -- Проносы мяча
    carries INTEGER,
    total_carrying_distance INTEGER,
    progressive_carrying_distance INTEGER,
    progressive_carries INTEGER,
    carries_into_final_third INTEGER,
    carries_into_penalty_area INTEGER,
    
    -- Потери и приемы
    miscontrols INTEGER,
    dispossessed INTEGER,
    passes_received INTEGER,
    progressive_receptions INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_possession UNIQUE(player_team_season_id)
);

CREATE INDEX idx_player_possession_pts ON player_possession_stats(player_team_season_id);

COMMENT ON TABLE player_possession_stats IS 'Статистика владения мячом и дриблинга';

-- 7. Разная статистика
CREATE TABLE player_misc_stats (
    stat_id SERIAL PRIMARY KEY,
    player_team_season_id INTEGER NOT NULL REFERENCES player_team_seasons(player_team_season_id) ON DELETE CASCADE,
    
    ninety_s DECIMAL(5,1),
    yellow_cards INTEGER,
    red_cards INTEGER,
    second_yellow INTEGER,
    fouls_committed INTEGER,
    fouls_drawn INTEGER,
    offsides INTEGER,
    crosses INTEGER,
    interceptions INTEGER,
    tackles_won INTEGER,
    penalties_won INTEGER,
    penalties_conceded INTEGER,
    own_goals INTEGER,
    ball_recoveries INTEGER,
    aerials_won INTEGER,
    aerials_lost INTEGER,
    aerials_won_pct DECIMAL(4,1),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_misc UNIQUE(player_team_season_id)
);

CREATE INDEX idx_player_misc_pts ON player_misc_stats(player_team_season_id);

COMMENT ON TABLE player_misc_stats IS 'Разнообразная статистика игроков';

-- 8. Статистика вратарей
CREATE TABLE player_keeper_stats (
    stat_id SERIAL PRIMARY KEY,
    player_team_season_id INTEGER NOT NULL REFERENCES player_team_seasons(player_team_season_id) ON DELETE CASCADE,
    
    ninety_s DECIMAL(5,1),
    shots_on_target_against INTEGER,
    goals_against INTEGER,
    saves INTEGER,
    save_pct DECIMAL(4,1),
    wins INTEGER,
    draws INTEGER,
    losses INTEGER,
    clean_sheets INTEGER,
    clean_sheet_pct DECIMAL(4,1),
    penalties_attempted INTEGER,
    penalties_allowed INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    penalties_save_pct DECIMAL(4,1),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_keeper UNIQUE(player_team_season_id)
);

CREATE INDEX idx_player_keeper_pts ON player_keeper_stats(player_team_season_id);

COMMENT ON TABLE player_keeper_stats IS 'Статистика вратарей';

-- 9. Продвинутая статистика вратарей
CREATE TABLE player_keeper_adv_stats (
    stat_id SERIAL PRIMARY KEY,
    player_team_season_id INTEGER NOT NULL REFERENCES player_team_seasons(player_team_season_id) ON DELETE CASCADE,
    
    ninety_s DECIMAL(5,1),
    goals_against INTEGER,
    penalties_allowed INTEGER,
    free_kick_goals_against INTEGER,
    corner_kick_goals_against INTEGER,
    own_goals_against INTEGER,
    post_shot_xg DECIMAL(6,2),
    post_shot_xg_per_shot DECIMAL(4,2),
    post_shot_xg_minus_goals DECIMAL(5,2),
    launched_completed INTEGER,
    launched_attempted INTEGER,
    launched_pct DECIMAL(4,1),
    passes_attempted INTEGER,
    throws_attempted INTEGER,
    launch_pct DECIMAL(4,1),
    avg_pass_length DECIMAL(4,1),
    goal_kicks INTEGER,
    goal_kick_launch_pct DECIMAL(4,1),
    avg_goal_kick_length DECIMAL(4,1),
    crosses_faced INTEGER,
    crosses_stopped INTEGER,
    crosses_stopped_pct DECIMAL(4,1),
    def_actions_outside_pen INTEGER,
    def_actions_outside_pen_per_90 DECIMAL(4,2),
    avg_distance_def_actions DECIMAL(4,1),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_player_keeper_adv UNIQUE(player_team_season_id)
);

CREATE INDEX idx_player_keeper_adv_pts ON player_keeper_adv_stats(player_team_season_id);

COMMENT ON TABLE player_keeper_adv_stats IS 'Продвинутая статистика вратарей (PSxG и игра ногами)';
COMMENT ON COLUMN player_keeper_adv_stats.post_shot_xg IS 'Post-Shot Expected Goals - качество спасений';

-- ============================================================================
-- VIEWS FOR ANALYTICS (Представления для аналитики)
-- ============================================================================

-- Процент побед дома и на выезде
CREATE VIEW v_home_away_win_rate AS
SELECT 
    t.team_id,
    t.team_name,
    l.league_name,
    s.season_code,
    COUNT(CASE WHEN m.home_team_id = t.team_id THEN 1 END) as home_matches,
    COUNT(CASE WHEN m.away_team_id = t.team_id THEN 1 END) as away_matches,
    SUM(CASE WHEN m.home_team_id = t.team_id AND m.home_goals > m.away_goals THEN 1 ELSE 0 END) as home_wins,
    SUM(CASE WHEN m.away_team_id = t.team_id AND m.away_goals > m.home_goals THEN 1 ELSE 0 END) as away_wins,
    ROUND(
        SUM(CASE WHEN m.home_team_id = t.team_id AND m.home_goals > m.away_goals THEN 1 ELSE 0 END)::NUMERIC /
        NULLIF(COUNT(CASE WHEN m.home_team_id = t.team_id THEN 1 END), 0) * 100, 2
    ) as home_win_rate_pct,
    ROUND(
        SUM(CASE WHEN m.away_team_id = t.team_id AND m.away_goals > m.home_goals THEN 1 ELSE 0 END)::NUMERIC /
        NULLIF(COUNT(CASE WHEN m.away_team_id = t.team_id THEN 1 END), 0) * 100, 2
    ) as away_win_rate_pct
FROM teams t
JOIN matches m ON t.team_id IN (m.home_team_id, m.away_team_id)
JOIN leagues l ON m.league_id = l.league_id
JOIN seasons s ON m.season_id = s.season_id
WHERE m.home_goals IS NOT NULL AND m.away_goals IS NOT NULL
GROUP BY t.team_id, t.team_name, l.league_name, s.season_code;

COMMENT ON VIEW v_home_away_win_rate IS 'Статистика побед дома и на выезде для каждой команды';

-- Метрики для кластеризации команд по стилю игры
CREATE VIEW v_team_style_metrics AS
SELECT 
    t.team_id,
    t.team_name,
    l.league_name,
    s.season_code,
    tss.possession_pct,
    tss.goals,
    tss.assists,
    tss.progressive_passes,
    tss.progressive_carries,
    tss.yellow_cards + tss.red_cards as total_cards,
    tss.xg,
    tss.npxg,
    tss.matches_played,
    ROUND(tss.goals::NUMERIC / NULLIF(tss.matches_played, 0), 2) as goals_per_match,
    ROUND(tss.xg::NUMERIC / NULLIF(tss.ninety_s, 0), 2) as xg_per_90
FROM team_season_stats tss
JOIN teams t ON t.team_id = tss.team_id
JOIN leagues l ON l.league_id = tss.league_id
JOIN seasons s ON s.season_id = tss.season_id;

COMMENT ON VIEW v_team_style_metrics IS 'Метрики стиля игры для кластеризации команд';

-- Форма команды по месяцам
CREATE VIEW v_team_form_by_month AS
SELECT 
    t.team_id,
    t.team_name,
    l.league_name,
    s.season_code,
    DATE_TRUNC('month', m.match_date)::DATE as month,
    COUNT(*) as matches_played,
    SUM(CASE 
        WHEN (m.home_team_id = t.team_id AND m.home_goals > m.away_goals) OR
             (m.away_team_id = t.team_id AND m.away_goals > m.home_goals)
        THEN 1 ELSE 0 END) as wins,
    SUM(CASE 
        WHEN m.home_goals = m.away_goals
        THEN 1 ELSE 0 END) as draws,
    SUM(CASE 
        WHEN (m.home_team_id = t.team_id AND m.home_goals < m.away_goals) OR
             (m.away_team_id = t.team_id AND m.away_goals < m.home_goals)
        THEN 1 ELSE 0 END) as losses,
    SUM(CASE WHEN m.home_team_id = t.team_id THEN m.home_goals ELSE m.away_goals END) as goals_for,
    SUM(CASE WHEN m.home_team_id = t.team_id THEN m.away_goals ELSE m.home_goals END) as goals_against,
    ROUND(
        SUM(CASE 
            WHEN (m.home_team_id = t.team_id AND m.home_goals > m.away_goals) OR
                 (m.away_team_id = t.team_id AND m.away_goals > m.home_goals)
            THEN 3
            WHEN m.home_goals = m.away_goals THEN 1
            ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0), 2
    ) as points_per_match
FROM teams t
JOIN matches m ON t.team_id IN (m.home_team_id, m.away_team_id)
JOIN leagues l ON m.league_id = l.league_id
JOIN seasons s ON m.season_id = s.season_id
WHERE m.home_goals IS NOT NULL AND m.away_goals IS NOT NULL
GROUP BY t.team_id, t.team_name, l.league_name, s.season_code, DATE_TRUNC('month', m.match_date);

COMMENT ON VIEW v_team_form_by_month IS 'Динамика формы команды по месяцам';

-- Статистика против конкретных соперников (для определения "неудобных")
CREATE VIEW v_head_to_head AS
SELECT 
    t1.team_id as team_id,
    t1.team_name as team_name,
    t2.team_id as opponent_id,
    t2.team_name as opponent_name,
    l.league_name,
    COUNT(*) as matches_played,
    SUM(CASE 
        WHEN (m.home_team_id = t1.team_id AND m.home_goals > m.away_goals) OR
             (m.away_team_id = t1.team_id AND m.away_goals > m.home_goals)
        THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN m.home_goals = m.away_goals THEN 1 ELSE 0 END) as draws,
    SUM(CASE 
        WHEN (m.home_team_id = t1.team_id AND m.home_goals < m.away_goals) OR
             (m.away_team_id = t1.team_id AND m.away_goals < m.home_goals)
        THEN 1 ELSE 0 END) as losses,
    SUM(CASE WHEN m.home_team_id = t1.team_id THEN m.home_goals ELSE m.away_goals END) as goals_for,
    SUM(CASE WHEN m.home_team_id = t1.team_id THEN m.away_goals ELSE m.home_goals END) as goals_against,
    ROUND(
        SUM(CASE 
            WHEN (m.home_team_id = t1.team_id AND m.home_goals > m.away_goals) OR
                 (m.away_team_id = t1.team_id AND m.away_goals > m.home_goals)
            THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2
    ) as win_rate_pct
FROM teams t1
JOIN matches m ON t1.team_id IN (m.home_team_id, m.away_team_id)
JOIN teams t2 ON t2.team_id = CASE 
    WHEN m.home_team_id = t1.team_id THEN m.away_team_id
    ELSE m.home_team_id END
JOIN leagues l ON m.league_id = l.league_id
WHERE m.home_goals IS NOT NULL AND m.away_goals IS NOT NULL
GROUP BY t1.team_id, t1.team_name, t2.team_id, t2.team_name, l.league_name;

COMMENT ON VIEW v_head_to_head IS 'Статистика личных встреч для определения неудобных соперников';

-- ============================================================================
-- FUNCTIONS (Функции)
-- ============================================================================

-- Функция для обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггеры для автоматического обновления updated_at
CREATE TRIGGER update_leagues_updated_at BEFORE UPDATE ON leagues
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_teams_updated_at BEFORE UPDATE ON teams
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_players_updated_at BEFORE UPDATE ON players
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- Composite indexes для часто используемых комбинаций
CREATE INDEX idx_matches_teams_date ON matches(home_team_id, away_team_id, match_date);
CREATE INDEX idx_standings_league_season_rank ON standings(league_id, season_id, rank);

-- Indexes для поиска игроков
CREATE INDEX idx_players_born ON players(born);
CREATE INDEX idx_players_position ON players(position);

-- ============================================================================
-- GRANTS (Права доступа)
-- ============================================================================

-- Раскомментируйте и настройте для production
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst_role;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO etl_role;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO etl_role;

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================

-- Вывести статистику по созданным объектам
DO $$
DECLARE
    table_count INTEGER;
    view_count INTEGER;
    index_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    
    SELECT COUNT(*) INTO view_count FROM information_schema.views 
    WHERE table_schema = 'public';
    
    SELECT COUNT(*) INTO index_count FROM pg_indexes 
    WHERE schemaname = 'public';
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Schema created successfully!';
    RAISE NOTICE 'Tables created: %', table_count;
    RAISE NOTICE 'Views created: %', view_count;
    RAISE NOTICE 'Indexes created: %', index_count;
    RAISE NOTICE '========================================';
END $$;

