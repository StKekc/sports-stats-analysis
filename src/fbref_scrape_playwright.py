#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FBref сбор через реальный браузер (Playwright):
- Scores & Fixtures
- Season Stats (standings + командные стандартные метрики)

Теперь можно запускать для любой лиги:
    PYTHONPATH=src python3 src/fbref_scrape_playwright.py epl
    PYTHONPATH=src python3 src/fbref_scrape_playwright.py laliga
    PYTHONPATH=src python3 src/fbref_scrape_playwright.py bundesliga
"""

import sys
import pathlib
import pandas as pd
from playwright.sync_api import TimeoutError as PWTimeout

from modules.data_extraction.fbref_scraper import FBrefScraper
from modules.data_reception.fbref_parser import biggest_table, detect_standings_and_teamstats
from modules.config_loader import load_league_config
from modules.data_reception.fbref_parser import (
    biggest_table, detect_standings_and_teamstats,
    find_players_stats_url, parse_players_standard_table
)

PLAYER_CATEGORIES = {
    "standard":      ("stats",           "Standard-Stats"),
    "shooting":      ("shooting",        "Shooting"),
    "passing":       ("passing",         "Passing"),
    "passing_types": ("passing_types",   "Passing-Types"),
    "defense":       ("defense",         "Defensive-Actions"),
    "possession":    ("possession",      "Possession"),
    "misc":          ("misc",            "Misc"),
    "keepers":       ("keepers",         "Goalkeeping"),
    "keepers_adv":   ("keepersadv",     "Advanced-Goalkeeping"),
}

def save_csv(df: pd.DataFrame, path: pathlib.Path):
    """Сохранение CSV с логом размера"""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[OK] saved {path.relative_to(BASE_DIR)}  rows={len(df)}  cols={len(df.columns)}")

def urls_for_season(comp_id: int, league_name: str, season: str) -> tuple[str, str]:
    """Вернёт (fixtures_url, season_stats_url) для заданного сезона"""
    fixtures = f"https://fbref.com/en/comps/{comp_id}/{season}/schedule/{season}-{league_name}-Scores-and-Fixtures"
    season_stats = f"https://fbref.com/en/comps/{comp_id}/{season}/{season}-{league_name}-Stats"
    return fixtures, season_stats

def out_dir_for(league_code: str, season: str) -> pathlib.Path:
    """Папка для выгрузки CSV конкретного сезона"""
    d = BASE_DIR / "data" / "raw" / "fbref" / f"{league_code}_{season}"
    d.mkdir(parents=True, exist_ok=True)
    return d

def main():
    # === аргумент командной строки ===
    if len(sys.argv) < 2:
        print("❌ Укажите код лиги (epl | laliga | bundesliga)")
        sys.exit(1)
    league_code = sys.argv[1]

    # === загрузка конфига ===
    league = load_league_config(league_code)
    comp_id = league["comp_id"]
    name = league["name"]
    seasons = league["seasons"]

    print(f"[START] Сбор данных для {name} ({league_code})")
    print(f"[CFG] comp_id={comp_id}  seasons={seasons}")

    with FBrefScraper(headless=False) as scraper:
        for season in seasons:
            print(f"\n=== SEASON {season} ===")
            fixtures_url, season_url = urls_for_season(comp_id, name.replace(' ', '-'), season)
            out_dir = out_dir_for(league_code, season)

            # 1️⃣ Fixtures
            try:
                print(f"[EXTRACTION] fixtures: {fixtures_url}")
                html_fixtures = scraper.get_page_html(fixtures_url)
                (out_dir / "fixtures_page.html").write_text(html_fixtures, encoding="utf-8")

                print("[RECEPTION] parse fixtures table…")
                tbl_fixtures = biggest_table(html_fixtures)
                save_csv(tbl_fixtures, out_dir / "schedule_results.csv")
            except Exception as e:
                print(f"[ERROR] fixtures failed ({season}): {e}")

            # 2️⃣ Season stats
            try:
                print(f"[EXTRACTION] season stats: {season_url}")
                html_season = scraper.get_page_html(season_url)
                (out_dir / "season_page.html").write_text(html_season, encoding="utf-8")

                print("[RECEPTION] parse standings & team stats…")
                standings, teamstats = detect_standings_and_teamstats(html_season)

                if standings is not None and len(standings):
                    save_csv(standings, out_dir / "standings.csv")
                else:
                    print("[WARN] standings not detected")

                if teamstats is not None and len(teamstats):
                    save_csv(teamstats, out_dir / "team_standard_stats.csv")
                else:
                    print("[WARN] team standard stats not detected")

                # 2️⃣a) Players — стандартная статистика игроков
                try:
                    players_url = find_players_stats_url(html_season)
                    if players_url:
                        print(f"[EXTRACTION] players stats: {players_url}")
                        html_players = scraper.get_page_html(players_url)
                        (out_dir / "players_page.html").write_text(html_players, encoding="utf-8")

                        print("[RECEPTION] parse players standard stats…")
                        players_df = parse_players_standard_table(html_players)
                        if players_df is not None and len(players_df):
                            save_csv(players_df, out_dir / "player_standard_stats.csv")
                        else:
                            print("[WARN] players standard stats not detected")
                    else:
                        print("[WARN] players stats url not found on season page")
                except Exception as e:
                    print(f"[ERROR] players page failed ({season}): {e}")

                # 2️⃣b) Расширенные статы игроков (shooting, passing, defense, и т.д.)
                try:
                    # Нам нужно сгенерировать URLы для каждой категории
                    # Логика такая же, как в fbref_scrape_players_extended.py:
                    # https://fbref.com/en/comps/{comp_id}/{season}/{slug}/{season}-{league-slug}-Stats

                    league_slug = name.replace(" ", "-")  # как ты уже делал для fixtures_url раньше

                    for cat, (slug, nice_name) in PLAYER_CATEGORIES.items():
                        # пример для shooting:
                        # https://fbref.com/en/comps/9/2023-2024/shooting/2023-2024-Premier-League-Stats
                        cat_url = (
                            f"https://fbref.com/en/comps/{comp_id}/{season}/{slug}/"
                            f"{season}-{league_slug}-Stats"
                        )

                        print(f"[EXTRACT EXT] {cat}: {cat_url}")
                        try:
                            html_cat = scraper.get_page_html(cat_url)
                            (out_dir / f"players_{cat}_page.html").write_text(html_cat, encoding="utf-8")

                            # Парсим таблицу
                            df_cat = biggest_table(html_cat)

                            if df_cat is None or len(df_cat) == 0:
                                print(f"[WARN] {cat}: empty or not parsed")
                                continue

                            # Сохраняем
                            save_csv(df_cat, out_dir / f"player_{cat}_stats.csv")

                        except Exception as e:
                            print(f"[ERROR] failed extended {cat} for {season}: {e}")

                except Exception as e:
                    print(f"[ERROR] extended player stats block failed ({season}): {e}")

            except Exception as e:
                print(f"[ERROR] season page failed ({season}): {e}")

    print(f"\n[DONE] Сбор {name} ({league_code}) завершён!")

if __name__ == "__main__":
    BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
    main()