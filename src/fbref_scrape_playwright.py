#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FBref (EPL 2024/25) сбор через реальный браузер (Playwright):
- Scores & Fixtures (расписание/результаты)
- Season Stats (standings + командные стандартные метрики)

Сохраняет CSV в: data/raw/fbref/epl_2024-2025/
"""

import pathlib
import time
from typing import List, Optional, Tuple
from io import StringIO

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "data" / "raw" / "fbref" / "epl_2024-2025"
OUT_DIR.mkdir(parents=True, exist_ok=True)

HOME = "https://fbref.com/en/"
FIXTURES = "https://fbref.com/en/comps/9/2024-2025/schedule/2024-2025-Premier-League-Scores-and-Fixtures"
SEASON   = "https://fbref.com/en/comps/9/2024-2025/2024-2025-Premier-League-Stats"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15"
)

def save_csv(df: pd.DataFrame, path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[OK] saved {path.relative_to(BASE_DIR)}  rows={len(df)}  cols={len(df.columns)}")

def biggest_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html), flavor="lxml")  # <-- просто tables =
    tbl = max(tables, key=lambda t: (t.shape[0], t.shape[1]))
    tbl = tbl.loc[:, ~tbl.columns.duplicated()].dropna(how="all").reset_index(drop=True)
    tbl.columns = (
        pd.Index(tbl.columns)
        .map(str).str.strip().str.lower()
        .str.replace(" ", "_")
        .str.replace("%", "pct")
        .str.replace("/", "_")
        .str.replace("-", "_", regex=False)
    )
    tbl.columns = ["".join(ch for ch in c if ch.isalnum() or ch == "_") for c in tbl.columns]
    return tbl

def detect_standings_and_teamstats(html: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    dfs = pd.read_html(StringIO(html), flavor="lxml")
    standings, teamstats = None, None

    # standings — ищем W/D/L/PTS
    for df in dfs:
        cols = set(map(lambda x: str(x).lower(), df.columns))
        if {"w", "d", "l"}.issubset(cols) or {"wins", "draws", "losses"}.issubset(cols) or "pts" in cols or "points" in cols:
            standings = df.loc[:, ~df.columns.duplicated()].dropna(how="all").reset_index(drop=True)
            break

    # team standard stats — ищем ключевые метрики
    keys = {"sh", "sot", "g", "ast", "xg", "xga", "cmp", "att", "tkl", "int"}
    best_score, best_df = -1, None
    for df in dfs:
        cols = set(map(lambda x: str(x).lower(), df.columns))
        score = len(cols & keys)
        if score > best_score and df.shape[1] >= 6:
            best_score, best_df = score, df
    teamstats = best_df

    def tidy(x: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if x is None:
            return None
        x = x.loc[:, ~x.columns.duplicated()].dropna(how="all").reset_index(drop=True)
        x.columns = (
            pd.Index(x.columns)
            .map(str).str.strip().str.lower()
            .str.replace(" ", "_").str.replace("%","pct").str.replace("/","_").str.replace("-", "_", regex=False)
        )
        x.columns = ["".join(ch for ch in c if ch.isalnum() or ch == "_") for c in x.columns]
        return x

    return tidy(standings), tidy(teamstats)

def get_page_html(p, url: str, wait_selector: str = "table") -> str:
    p.goto(url, wait_until="domcontentloaded", timeout=90_000)
    # Явное ожидание таблиц с классом 'stats_table' (FBref их так размечает)
    try:
        p.wait_for_selector("table.stats_table", timeout=90_000)
    except PWTimeout:
        print(f"[WARN] Timeout waiting for tables on {url}, saving anyway")
    time.sleep(2.5)  # небольшая "человеческая" пауза
    return p.content()

def main():
    print(f"[OUT] {OUT_DIR.relative_to(BASE_DIR)}")

    with sync_playwright() as pw:
        # ВАЖНО: запустим не в headless (так меньше шансов попасть под детект)
        browser = pw.chromium.launch(headless=False)  # можно True, но хуже для антибота
        ctx = browser.new_context(
            user_agent=UA,
            viewport={"width": 1400, "height": 900},
            locale="en-US",
            timezone_id="Europe/Prague",  # ставим реальный TZ
        )
        page = ctx.new_page()

        # тёплый заход — получим куки
        try:
            page.goto(HOME, wait_until="domcontentloaded", timeout=45_000)
            time.sleep(1.0)
        except PWTimeout:
            print("[WARN] warmup timeout — продолжим")

        # 1) Fixtures
        try:
            print("[LOAD] fixtures…")
            html = get_page_html(page, FIXTURES)
            (OUT_DIR / "fixtures_page.html").write_text(html, encoding="utf-8")
            tbl = biggest_table(html)
            save_csv(tbl, OUT_DIR / "schedule_results.csv")
        except Exception as e:
            print(f"[WARN] fixtures failed: {e}")

        # 2) Season page (standings + team standard)
        try:
            print("[LOAD] season stats…")
            html = get_page_html(page, SEASON)
            (OUT_DIR / "season_page.html").write_text(html, encoding="utf-8")
            standings, teamstats = detect_standings_and_teamstats(html)
            if standings is not None and len(standings):
                save_csv(standings, OUT_DIR / "standings.csv")
            else:
                print("[WARN] standings not detected")
            if teamstats is not None and len(teamstats):
                save_csv(teamstats, OUT_DIR / "team_standard_stats.csv")
            else:
                print("[WARN] team standard stats not detected")
        except Exception as e:
            print(f"[WARN] season page failed: {e}")

        # аккуратно закрываем
        ctx.close()
        browser.close()

    print("[DONE] FBref via Playwright complete")
    
if __name__ == "__main__":
    main()