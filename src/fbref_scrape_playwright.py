#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FBref сбор через реальный браузер (Playwright):
- Scores & Fixtures
- Season Stats (standings + командные стандартные метрики)
"""

import pathlib
import pandas as pd
from playwright.sync_api import TimeoutError as PWTimeout

from modules.data_extraction.fbref_scraper import FBrefScraper
from modules.data_reception.fbref_parser import biggest_table, detect_standings_and_teamstats
from modules.config_loader import load_league_config

# === Загружаем конфиг лиги ===
league = load_league_config("epl")
LEAGUE_CODE = "epl"
COMP_ID = league["comp_id"]
SEASONS = league["seasons"]

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]


def save_csv(df: pd.DataFrame, path: pathlib.Path):
    """Сохранение CSV с логом размера"""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[OK] saved {path.relative_to(BASE_DIR)}  rows={len(df)}  cols={len(df.columns)}")


def urls_for_season(season: str) -> tuple[str, str]:
    """Вернёт (fixtures_url, season_stats_url) для заданного сезона"""
    fixtures = f"https://fbref.com/en/comps/{COMP_ID}/{season}/schedule/{season}-Premier-League-Scores-and-Fixtures"
    season_stats = f"https://fbref.com/en/comps/{COMP_ID}/{season}/{season}-Premier-League-Stats"
    return fixtures, season_stats


def out_dir_for(season: str) -> pathlib.Path:
    """Папка для выгрузки CSV конкретного сезона"""
    d = BASE_DIR / "data" / "raw" / "fbref" / f"{LEAGUE_CODE}_{season}"
    d.mkdir(parents=True, exist_ok=True)
    return d

def main():
    print("[START] Этап: Сбор и получение информации")
    print(f"[CFG] {LEAGUE_CODE=} {COMP_ID=} seasons={SEASONS}")
    with FBrefScraper(headless=False) as scraper:
        for season in SEASONS:
            print(f"\n=== SEASON {season} ===")
            fixtures_url, season_url = urls_for_season(season)
            out_dir = out_dir_for(season)

            # 1) Fixtures
            try:
                print(f"[EXTRACTION] fixtures: {fixtures_url}")
                html_fixtures = scraper.get_page_html(fixtures_url)
                (out_dir / "fixtures_page.html").write_text(html_fixtures, encoding="utf-8")

                print("[RECEPTION] parse fixtures table…")
                tbl_fixtures = biggest_table(html_fixtures)
                save_csv(tbl_fixtures, out_dir / "schedule_results.csv")
            except Exception as e:
                print(f"[ERROR] fixtures failed ({season}): {e}")

            # 2) Season stats (standings + team standard)
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
            except Exception as e:
                print(f"[ERROR] season page failed ({season}): {e}")

    print("\n[DONE] Этап 'Сбор и получение информации' завершён!")


if __name__ == "__main__":
    main()
