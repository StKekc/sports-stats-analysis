#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from playwright.sync_api import Page, sync_playwright, TimeoutError as PWTimeout


class FBrefScraper:
    # класс для загрузки страниц FBref через Playwright
    
    HOME = "https://fbref.com/en/"
    FIXTURES = "https://fbref.com/en/comps/9/2024-2025/schedule/2024-2025-Premier-League-Scores-and-Fixtures"
    SEASON = "https://fbref.com/en/comps/9/2024-2025/2024-2025-Premier-League-Stats"
    
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15"
    )
    
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
    
    def __enter__(self):
        # запуск браузера
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            user_agent=self.USER_AGENT,
            viewport={"width": 1400, "height": 900},
            locale="en-US",
            timezone_id="Europe/Prague",
        )
        self.page = self.context.new_page()
        
        # заходим на главную, чтобы получить cookies
        try:
            self.page.goto(self.HOME, wait_until="domcontentloaded", timeout=45_000)
            time.sleep(1.0)
        except PWTimeout:
            print("[WARN] warmup timeout — продолжим")
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # закрываем браузер
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
    
    def get_page_html(self, url: str, wait_selector: str = "table") -> str:
        # загружаем страницу и ждем пока таблицы подгрузятся
        if not self.page:
            raise RuntimeError("Браузер не инициализирован. Используйте контекстный менеджер.")
        
        self.page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        
        # ждем таблицы stats_table
        try:
            self.page.wait_for_selector("table.stats_table", timeout=90_000)
            time.sleep(5)
        except PWTimeout:
            print(f"[WARN] Timeout waiting for tables on {url}, saving anyway")
        
        time.sleep(2.5)  # пауза чтобы не забанили
        return self.page.content()


def get_page_html(page: Page, url: str, wait_selector: str = "table") -> str:
    # вспомогательная функция для загрузки
    page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    
    try:
        page.wait_for_selector("table.stats_table", timeout=90_000)
        time.sleep(5)
    except PWTimeout:
        print(f"[WARN] Timeout waiting for tables on {url}, saving anyway")
    
    time.sleep(2.5)
    return page.content()

