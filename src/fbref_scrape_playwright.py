#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FBref (EPL 2024/25) сбор через реальный браузер (Playwright):
- Scores & Fixtures (расписание/результаты)
- Season Stats (standings + командные стандартные метрики)

Сохраняет CSV в: data/raw/fbref/epl_2024-2025/
"""

import pathlib
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from modules.data_extraction.fbref_scraper import FBrefScraper, get_page_html
from modules.data_reception.fbref_parser import (
    biggest_table,
    detect_standings_and_teamstats
)

import pandas as pd

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "data" / "raw" / "fbref" / "epl_2024-2025"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def save_csv(df: pd.DataFrame, path: pathlib.Path):
    # временное сохранение, потом будет через БД
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[OK] saved {path.relative_to(BASE_DIR)}  rows={len(df)}  cols={len(df.columns)}")


def main():
    # главная функция - загружаем страницы, парсим, сохраняем
    print(f"[OUT] {OUT_DIR.relative_to(BASE_DIR)}")
    print("[START] Этап: Сбор и получение информации")

    # Используем класс FBrefScraper через контекстный менеджер
    with FBrefScraper(headless=False) as scraper:
        
        # ===== 1) FIXTURES: Расписание и результаты матчей =====
        try:
            print("\n[EXTRACTION] Загрузка страницы fixtures...")
            html_fixtures = scraper.get_page_html(FBrefScraper.FIXTURES)
            
            # Сохраняем сырой HTML (для отладки/анализа)
            (OUT_DIR / "fixtures_page.html").write_text(html_fixtures, encoding="utf-8")
            
            print("[RECEPTION] Парсинг таблицы fixtures...")
            tbl_fixtures = biggest_table(html_fixtures)
            
            # Временное сохранение
            save_csv(tbl_fixtures, OUT_DIR / "schedule_results.csv")
            
        except Exception as e:
            print(f"[ERROR] fixtures failed: {e}")

        # ===== 2) SEASON: Турнирная таблица и командная статистика =====
        try:
            print("\n[EXTRACTION] Загрузка страницы season stats...")
            html_season = scraper.get_page_html(FBrefScraper.SEASON)
            
            # Сохраняем сырой HTML (для отладки/анализа)
            (OUT_DIR / "season_page.html").write_text(html_season, encoding="utf-8")
            
            print("[RECEPTION] Парсинг таблиц standings и team stats...")
            standings, teamstats = detect_standings_and_teamstats(html_season)
            
            # Временное сохранение standings
            if standings is not None and len(standings):
                save_csv(standings, OUT_DIR / "standings.csv")
            else:
                print("[WARN] standings not detected")
            
            # Временное сохранение team stats
            if teamstats is not None and len(teamstats):
                save_csv(teamstats, OUT_DIR / "team_standard_stats.csv")
            else:
                print("[WARN] team standard stats not detected")
                
        except Exception as e:
            print(f"[ERROR] season page failed: {e}")

    print("\n[DONE] Этап 'Сбор и получение информации' завершён!")


if __name__ == "__main__":
    main()
