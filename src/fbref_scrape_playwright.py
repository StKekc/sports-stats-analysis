#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FBref (EPL 2024/25) сбор через реальный браузер (Playwright):
- Scores & Fixtures (расписание/результаты)
- Season Stats (standings + командные стандартные метрики)

Сохраняет CSV в: data/raw/fbref/epl_2024-2025/
"""
import re
import pathlib
import time
from typing import List, Optional, Tuple
from io import StringIO
from bs4 import BeautifulSoup

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
    tables = pd.read_html(StringIO(html), flavor="lxml")
    tbl = max(tables, key=lambda t: (t.shape[0], t.shape[1]))
    tbl = flatten_and_normalize_columns(tbl)
    return tbl

def flatten_and_normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Сплющивает MultiIndex-колонки FBref.
    - Берём нижний ярлык +, при необходимости, метку секции.
    - Для секции 'Per 90 Minutes' добавляем суффикс _per90.
    - Гарантируем уникальность имён (без потери per90-колонок).
    """
    df = df.copy()

    def norm(s: str) -> str:
        s = s.strip().lower()
        s = s.replace("g+a", "g_plus_a").replace("g+apk", "g_plus_a_pk")
        s = s.replace("per 90 minutes", "per90")
        s = re.sub(r"[^\w]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s

    names = []
    if isinstance(df.columns, pd.MultiIndex):
        # сформируем «базовые» и «секционные» метки
        raw_names = []
        sect_tags  = []
        for tup in df.columns:
            parts = [str(x) for x in tup]
            # последний непустой как базовый
            base = ""
            for x in reversed(parts):
                if x and str(x).strip() and not str(x).lower().startswith("unnamed"):
                    base = x
                    break
            # первый непустой "верхний" как секция
            sect = ""
            for x in parts:
                if x and str(x).strip() and not str(x).lower().startswith("unnamed"):
                    sect = x
                    break
            raw_names.append(base)
            sect_tags.append(sect)

        # нормализация и первичная разметка
        prim = [norm(n) for n in raw_names]
        sect_norm = [norm(s) for s in sect_tags]

        # для per90 делаем суффикс, для дубликатов — добавляем метку секции
        seen = {}
        for i, (pname, sname) in enumerate(zip(prim, sect_norm)):
            candidate = pname
            if "per90" in sname:
                candidate = f"{pname}_per90"
            # если имя уже встречалось, добавим секцию как префикс/суффикс
            if candidate in seen:
                # компактные теги секций
                tag_map = {
                    "playing_time": "pt",
                    "performance": "perf",
                    "expected": "exp",
                    "progression": "prog",
                    "per90": "per90",
                }
                tag = tag_map.get(sname, sname[:6] or "sec")
                candidate = f"{pname}_{tag}"
            seen[candidate] = True
            names.append(candidate)

        df.columns = names
    else:
        df.columns = [norm(str(c)) for c in df.columns]

    # убираем дубликаты (теперь их быть не должно) и пустые строки
    df = df.loc[:, ~df.columns.duplicated()].dropna(how="all").reset_index(drop=True)
    return df

def detect_standings_and_teamstats(html: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    def tidy(x: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if x is None:
            return None
        x = flatten_and_normalize_columns(x)
        return x

    def table_to_df(tag) -> pd.DataFrame:
        return pd.read_html(StringIO(str(tag)), flavor="lxml")[0]

    soup = BeautifulSoup(html, "lxml")

    # 1) Standings по id (надёжно), иначе — по заголовкам (W/D/L/PTS)
    standings_df = None
    st_tag = soup.find("table", id=lambda x: x and "standings" in x.lower())
    if st_tag is None:
        for t in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in t.select("thead th")]
            if {"w","d","l"}.issubset(set(headers)) and ("pts" in headers or "points" in headers):
                st_tag = t
                break
    if st_tag is not None:
        standings_df = table_to_df(st_tag)

    # 2) Team Standard Stats по точному id
    teamstats_df = None
    ts_ids = ["stats_squads_standard_for", "stats_squads_standard"]  # запасной вариант
    ts_tag = None
    for tid in ts_ids:
        ts_tag = soup.find("table", id=tid)
        if ts_tag:
            break
    if ts_tag is None:
        # Фоллбэк: ищем таблицу с метриками Sh/SoT/xG и без W/D/L
        keys = {"sh", "sot", "xg", "xga", "g", "ast", "cmp", "att", "tkl", "int"}
        for t in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in t.select("thead th")]
            if len(set(headers) & keys) >= 3 and not {"w","d","l"}.issubset(set(headers)):
                ts_tag = t
                break
    if ts_tag is not None:
        teamstats_df = table_to_df(ts_tag)

    return tidy(standings_df), tidy(teamstats_df)

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
        time.sleep(5)
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