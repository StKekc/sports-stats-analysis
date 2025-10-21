#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Загрузка конфигурации лиг из config/leagues.yaml
"""

import yaml
import pathlib

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config" / "leagues.yaml"

def load_league_config(league_code: str) -> dict:
    """Возвращает словарь с параметрами заданной лиги."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if league_code not in data["leagues"]:
        raise ValueError(f"Лига '{league_code}' не найдена в {CONFIG_PATH}")
    return data["leagues"][league_code]

if __name__ == "__main__":
    epl = load_league_config("epl")
    print(epl)