#!/usr/bin/env python3
import subprocess

leagues = [
    "epl", "laliga", "bundesliga", "ligue1", "seriea",
    "primeira", "eredivisie", "superlig", "championship",
    "mls", "brasileirao", "argentina", "ligamx", "j1", "saudiprol"
]

for lg in leagues:
    print(f"\n==============================")
    print(f"=== STARTING {lg.upper()} ===")
    print(f"==============================\n")
    subprocess.run(["python3", "src/fbref_scrape_playwright.py", lg], check=False)