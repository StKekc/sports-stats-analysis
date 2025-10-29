#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
from typing import Optional, Tuple
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup

def _extract_biggest_table_from_html(html: str) -> pd.DataFrame | None:
    """
    Собирает все <table> из переданного html (как строки),
    парсит их через read_html и возвращает ту, у которой максимум строк.
    Возвращает уже приведённую flatten_and_normalize_columns(df).
    """
    soup = BeautifulSoup(html, "lxml")

    candidates = []
    for tbl in soup.find_all("table"):
        try:
            df = pd.read_html(StringIO(str(tbl)), flavor="lxml")[0]
        except Exception:
            continue
        if df is not None and len(df):
            df = flatten_and_normalize_columns(df)
            candidates.append(df)

    if not candidates:
        return None

    candidates.sort(key=lambda d: (d.shape[0], d.shape[1]), reverse=True)
    return candidates[0]


def _extract_biggest_table_from_html_comments(html: str) -> pd.DataFrame | None:
    """
    FBref иногда прячет гигантские таблицы внутри <!-- ... -->,
    и браузер потом уже на странице их 'раскрывает' JS-ом.
    Playwright .content() может вернуть исходник с этими комментариями,
    и обычный парс этого не увидит.

    Здесь мы вручную перебираем все <!-- ... --> блоки,
    ищем внутри <table> и вытаскиваем самую большую таблицу так же,
    как в _extract_biggest_table_from_html.
    """
    comment_tables = []

    # вытащим все куски внутри <!-- ... -->
    for m in re.finditer(r"<!--(.*?)-->", html, flags=re.DOTALL):
        block = m.group(1)
        if "<table" not in block.lower():
            continue
        try:
            df = _extract_biggest_table_from_html(block)
        except Exception:
            df = None
        if df is not None and len(df):
            comment_tables.append(df)

    if not comment_tables:
        return None

    comment_tables.sort(key=lambda d: (d.shape[0], d.shape[1]), reverse=True)
    return comment_tables[0]


# Меняю на новое временно
# def biggest_table(html: str) -> pd.DataFrame:
#     # берем самую большую таблицу из HTML
#     tables = pd.read_html(StringIO(html), flavor="lxml")
#     tbl = max(tables, key=lambda t: (t.shape[0], t.shape[1]))
#     tbl = flatten_and_normalize_columns(tbl)
#     return tbl

def biggest_table(html: str) -> pd.DataFrame:
    """
    Возвращает основную (самую большую по числу строк) таблицу со страницы.

    Логика:
    1. Пытаемся достать таблицу из обычного HTML.
    2. Если нашли, но она подозрительно маленькая (<100 строк),
       пробуем достать таблицу из скрытых <!-- ... --> блоков и,
       если она больше — берём её.
    3. Если вообще не нашли обычную таблицу — сразу пытаемся взять из комментариев.

    В конце обязательно прогоняем flatten_and_normalize_columns.
    """
    # 1. обычные таблицы
    main_df = None
    try:
        main_df = _extract_biggest_table_from_html(html)
    except Exception:
        main_df = None

    # 2. таблицы внутри комментариев
    comment_df = None
    try:
        comment_df = _extract_biggest_table_from_html_comments(html)
    except Exception:
        comment_df = None

    # кейс: нет вообще ничего обычного, но есть коммент-таблица
    if (main_df is None or len(main_df) == 0) and comment_df is not None and len(comment_df):
        return comment_df.reset_index(drop=True)

    # кейс: обычная таблица есть, но короткая (типа MLS 2025: 30 строк)
    if main_df is not None and len(main_df) and len(main_df) < 100:
        if comment_df is not None and len(comment_df) > len(main_df):
            return comment_df.reset_index(drop=True)

    # дефолт: возвращаем то, что нашли первым способом
    if main_df is not None and len(main_df):
        return main_df.reset_index(drop=True)

    # если вообще пусто — вернуть пустой df (чтобы не падать)
    return pd.DataFrame()


def flatten_and_normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # приводим многоуровневые колонки FBref к нормальному виду
    df = df.copy()

    def norm(s: str) -> str:
        # чистим название колонки
        s = s.strip().lower()
        s = s.replace("g+a", "g_plus_a").replace("g+apk", "g_plus_a_pk")
        s = s.replace("per 90 minutes", "per90")
        s = re.sub(r"[^\w]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s

    names = []
    if isinstance(df.columns, pd.MultiIndex):
        # разбираем многоуровневые заголовки
        raw_names = []
        sect_tags = []
        for tup in df.columns:
            parts = [str(x) for x in tup]
            # последний непустой берем как основное название
            base = ""
            for x in reversed(parts):
                if x and str(x).strip() and not str(x).lower().startswith("unnamed"):
                    base = x
                    break
            # первый непустой - это секция таблицы
            sect = ""
            for x in parts:
                if x and str(x).strip() and not str(x).lower().startswith("unnamed"):
                    sect = x
                    break
            raw_names.append(base)
            sect_tags.append(sect)

        prim = [norm(n) for n in raw_names]
        sect_norm = [norm(s) for s in sect_tags]

        # делаем уникальные имена
        seen = {}
        for i, (pname, sname) in enumerate(zip(prim, sect_norm)):
            candidate = pname
            if "per90" in sname:
                candidate = f"{pname}_per90"
            if candidate in seen:
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

    df = df.loc[:, ~df.columns.duplicated()].dropna(how="all").reset_index(drop=True)
    return df


def detect_standings_and_teamstats(html: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    # находим турнирную таблицу и статистику команд
    
    def tidy(x: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if x is None:
            return None
        x = flatten_and_normalize_columns(x)
        return x

    def table_to_df(tag) -> pd.DataFrame:
        return pd.read_html(StringIO(str(tag)), flavor="lxml")[0]

    soup = BeautifulSoup(html, "lxml")

    # ищем standings - по id или по заголовкам W/D/L/PTS
    standings_df = None
    st_tag = soup.find("table", id=lambda x: x and "standings" in x.lower())
    if st_tag is None:
        for t in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in t.select("thead th")]
            if {"w", "d", "l"}.issubset(set(headers)) and ("pts" in headers or "points" in headers):
                st_tag = t
                break
    if st_tag is not None:
        standings_df = table_to_df(st_tag)

    # ищем team stats - сначала по id, потом по метрикам
    teamstats_df = None
    ts_ids = ["stats_squads_standard_for", "stats_squads_standard"]
    ts_tag = None
    for tid in ts_ids:
        ts_tag = soup.find("table", id=tid)
        if ts_tag:
            break
    if ts_tag is None:
        # если по id не нашли - ищем по наличию метрик
        keys = {"sh", "sot", "xg", "xga", "g", "ast", "cmp", "att", "tkl", "int"}
        for t in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in t.select("thead th")]
            if len(set(headers) & keys) >= 3 and not {"w", "d", "l"}.issubset(set(headers)):
                ts_tag = t
                break
    if ts_tag is not None:
        teamstats_df = table_to_df(ts_tag)

    return tidy(standings_df), tidy(teamstats_df)


def find_players_stats_url(html: str) -> str | None:
    """Ищет на странице сезона ссылку на таблицу 'Standard Stats — Players'."""
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").strip().lower()
        if "/comps/" in href and ("players" in href or "stats" in href) and "standard" in text:
            return "https://fbref.com" + href if href.startswith("/") else href
    return None


def parse_players_standard_table(html: str) -> pd.DataFrame | None:
    """Возвращает таблицу со стандартной статистикой игроков."""
    try:
        df = biggest_table(html)
        return df
    except Exception:
        return None
