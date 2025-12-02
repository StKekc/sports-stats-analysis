"""
Модуль визуализации процента побед дома и в гостях
"""

import logging
from pathlib import Path
from typing import Optional
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from plotly.subplots import make_subplots


logger = logging.getLogger(__name__)


class WinRateVisualizer:
    """
    Класс для визуализации статистики побед команд дома и в гостях
    """
    
    def __init__(self, output_dir: str = "outputs"):
        """
        Инициализация визуализатора
        
        Args:
            output_dir: Директория для сохранения графиков
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Настройка стиля для matplotlib
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
    
    def plot_home_away_comparison_bar(
        self, 
        df: pd.DataFrame, 
        save_path: Optional[str] = None
    ) -> None:
        """
        Создает столбчатую диаграмму сравнения процента побед дома и в гостях
        
        Args:
            df: DataFrame с колонками team_name, home_win_pct, away_win_pct
            save_path: Путь для сохранения графика (если None, используется output_dir)
        """
        logger.info("Создание столбчатой диаграммы...")
        
        # Проверка наличия необходимых колонок
        required_cols = ['team_name', 'home_win_pct', 'away_win_pct']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"DataFrame должен содержать колонки: {required_cols}")
        
        # Конвертируем Decimal в float для совместимости с matplotlib
        df = df.copy()
        df['home_win_pct'] = pd.to_numeric(df['home_win_pct'], errors='coerce')
        df['away_win_pct'] = pd.to_numeric(df['away_win_pct'], errors='coerce')
        if 'total_win_pct' in df.columns:
            df['total_win_pct'] = pd.to_numeric(df['total_win_pct'], errors='coerce')
        
        # Сортируем по общему проценту побед (если есть колонка)
        if 'total_win_pct' in df.columns:
            df = df.sort_values('total_win_pct', ascending=False)
        
        # Создание графика
        fig, ax = plt.subplots(figsize=(14, 8))
        
        x = range(len(df))
        width = 0.35
        
        # Столбцы для домашних и выездных побед
        bars1 = ax.bar(
            [i - width/2 for i in x], 
            df['home_win_pct'], 
            width, 
            label='Домашние матчи',
            color='#2ecc71',
            alpha=0.8,
            edgecolor='black',
            linewidth=0.5
        )
        
        bars2 = ax.bar(
            [i + width/2 for i in x], 
            df['away_win_pct'], 
            width, 
            label='Выездные матчи',
            color='#3498db',
            alpha=0.8,
            edgecolor='black',
            linewidth=0.5
        )
        
        # Добавление значений на столбцы
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width() / 2.,
                    height,
                    f'{height:.1f}%',
                    ha='center',
                    va='bottom',
                    fontsize=9,
                    fontweight='bold'
                )
        
        # Настройка осей и заголовков
        ax.set_xlabel('Команда', fontsize=12, fontweight='bold')
        ax.set_ylabel('Процент побед (%)', fontsize=12, fontweight='bold')
        ax.set_title(
            'Процент побед дома и в гостях - Топ команд\n(Задача 2: Анализ через Spark SQL)',
            fontsize=14,
            fontweight='bold',
            pad=20
        )
        ax.set_xticks(x)
        ax.set_xticklabels(df['team_name'], rotation=45, ha='right', fontsize=10)
        ax.legend(loc='upper right', fontsize=11, framealpha=0.9)
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        ax.set_ylim(0, max(df['home_win_pct'].max(), df['away_win_pct'].max()) * 1.15)
        
        plt.tight_layout()
        
        # Сохранение графика
        if save_path is None:
            save_path = self.output_dir / "task2_home_away_win_rate_bar.png"
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"✅ График сохранен: {save_path}")
        
        plt.show()
        plt.close()
    
    def plot_home_away_difference(
        self, 
        df: pd.DataFrame, 
        save_path: Optional[str] = None
    ) -> None:
        """
        Создает график разницы между процентом побед дома и в гостях
        
        Args:
            df: DataFrame с колонками team_name, home_win_pct, away_win_pct
            save_path: Путь для сохранения графика
        """
        logger.info("Создание графика разницы побед...")
        
        # Конвертируем Decimal в float
        df = df.copy()
        df['home_win_pct'] = pd.to_numeric(df['home_win_pct'], errors='coerce')
        df['away_win_pct'] = pd.to_numeric(df['away_win_pct'], errors='coerce')
        
        # Рассчитываем разницу
        df['win_diff'] = df['home_win_pct'] - df['away_win_pct']
        df = df.sort_values('win_diff', ascending=True)
        
        # Создание графика
        fig, ax = plt.subplots(figsize=(12, 8))
        
        colors = ['red' if x < 0 else 'green' for x in df['win_diff']]
        
        ax.barh(
            df['team_name'], 
            df['win_diff'], 
            color=colors,
            alpha=0.7,
            edgecolor='black',
            linewidth=0.5
        )
        
        # Добавление значений
        for i, (idx, row) in enumerate(df.iterrows()):
            ax.text(
                row['win_diff'],
                i,
                f' {row["win_diff"]:.1f}%',
                va='center',
                ha='left' if row['win_diff'] > 0 else 'right',
                fontsize=9,
                fontweight='bold'
            )
        
        ax.axvline(x=0, color='black', linestyle='-', linewidth=1.5)
        ax.set_xlabel('Разница: Дома % - В гостях %', fontsize=12, fontweight='bold')
        ax.set_ylabel('Команда', fontsize=12, fontweight='bold')
        ax.set_title(
            'Разница в проценте побед: Дома vs В гостях\n(Положительное значение = лучше играют дома)',
            fontsize=14,
            fontweight='bold',
            pad=20
        )
        ax.grid(axis='x', alpha=0.3, linestyle='--')
        
        plt.tight_layout()
        
        if save_path is None:
            save_path = self.output_dir / "task2_home_away_difference.png"
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"✅ График сохранен: {save_path}")
        
        plt.show()
        plt.close()
    
    def plot_interactive_plotly(
        self, 
        df: pd.DataFrame, 
        save_path: Optional[str] = None
    ) -> None:
        """
        Создает интерактивный график через Plotly
        
        Args:
            df: DataFrame с колонками team_name, home_win_pct, away_win_pct
            save_path: Путь для сохранения HTML файла
        """
        logger.info("Создание интерактивного графика Plotly...")
        
        # Конвертируем Decimal в float
        df = df.copy()
        df['home_win_pct'] = pd.to_numeric(df['home_win_pct'], errors='coerce')
        df['away_win_pct'] = pd.to_numeric(df['away_win_pct'], errors='coerce')
        if 'total_win_pct' in df.columns:
            df['total_win_pct'] = pd.to_numeric(df['total_win_pct'], errors='coerce')
        
        # Сортировка по общему проценту
        if 'total_win_pct' in df.columns:
            df = df.sort_values('total_win_pct', ascending=False)
        
        # Создание графика
        fig = go.Figure()
        
        # Домашние матчи
        fig.add_trace(go.Bar(
            name='Домашние матчи',
            x=df['team_name'],
            y=df['home_win_pct'],
            text=df['home_win_pct'].apply(lambda x: f'{x:.1f}%'),
            textposition='outside',
            marker_color='#2ecc71',
            hovertemplate='<b>%{x}</b><br>Дома: %{y:.1f}%<extra></extra>'
        ))
        
        # Выездные матчи
        fig.add_trace(go.Bar(
            name='Выездные матчи',
            x=df['team_name'],
            y=df['away_win_pct'],
            text=df['away_win_pct'].apply(lambda x: f'{x:.1f}%'),
            textposition='outside',
            marker_color='#3498db',
            hovertemplate='<b>%{x}</b><br>В гостях: %{y:.1f}%<extra></extra>'
        ))
        
        # Настройка layout
        fig.update_layout(
            title={
                'text': 'Процент побед дома и в гостях - Топ команд<br><sub>Интерактивный график (Задача 2: Spark SQL)</sub>',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18}
            },
            xaxis_title='Команда',
            yaxis_title='Процент побед (%)',
            barmode='group',
            hovermode='x unified',
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1
            ),
            template='plotly_white',
            height=600,
            font=dict(size=12)
        )
        
        if save_path is None:
            save_path = self.output_dir / "task2_home_away_win_rate_interactive.html"
        
        fig.write_html(save_path)
        logger.info(f"✅ Интерактивный график сохранен: {save_path}")
        
        fig.show()
    
    def plot_comprehensive_dashboard(
        self, 
        df: pd.DataFrame, 
        save_path: Optional[str] = None
    ) -> None:
        """
        Создает комплексный dashboard с несколькими графиками
        
        Args:
            df: DataFrame с результатами анализа
            save_path: Путь для сохранения
        """
        logger.info("Создание комплексного dashboard...")
        
        df = df.copy()
        # Конвертируем Decimal в float
        numeric_cols = ['home_win_pct', 'away_win_pct', 'home_matches', 'home_wins', 
                       'away_matches', 'away_wins']
        if 'total_win_pct' in df.columns:
            numeric_cols.append('total_win_pct')
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['win_diff'] = df['home_win_pct'] - df['away_win_pct']
        
        # Создание subplot с 2x2 графиками
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Процент побед дома и в гостях',
                'Количество матчей',
                'Разница: Дома - В гостях',
                'Общий процент побед'
            ),
            specs=[
                [{'type': 'bar'}, {'type': 'bar'}],
                [{'type': 'bar'}, {'type': 'bar'}]
            ],
            vertical_spacing=0.12,
            horizontal_spacing=0.1
        )
        
        # График 1: Процент побед дома и в гостях
        fig.add_trace(
            go.Bar(name='Дома', x=df['team_name'], y=df['home_win_pct'], 
                   marker_color='#2ecc71', showlegend=True),
            row=1, col=1
        )
        fig.add_trace(
            go.Bar(name='В гостях', x=df['team_name'], y=df['away_win_pct'], 
                   marker_color='#3498db', showlegend=True),
            row=1, col=1
        )
        
        # График 2: Количество матчей
        fig.add_trace(
            go.Bar(name='Дома (кол-во)', x=df['team_name'], y=df['home_matches'], 
                   marker_color='#27ae60', showlegend=False),
            row=1, col=2
        )
        fig.add_trace(
            go.Bar(name='В гостях (кол-во)', x=df['team_name'], y=df['away_matches'], 
                   marker_color='#2980b9', showlegend=False),
            row=1, col=2
        )
        
        # График 3: Разница
        colors_diff = ['red' if x < 0 else 'green' for x in df['win_diff']]
        fig.add_trace(
            go.Bar(x=df['team_name'], y=df['win_diff'], 
                   marker_color=colors_diff, showlegend=False),
            row=2, col=1
        )
        
        # График 4: Общий процент побед
        if 'total_win_pct' in df.columns:
            fig.add_trace(
                go.Bar(x=df['team_name'], y=df['total_win_pct'], 
                       marker_color='#9b59b6', showlegend=False),
                row=2, col=2
            )
        
        # Обновление layout
        fig.update_xaxes(tickangle=-45, row=1, col=1)
        fig.update_xaxes(tickangle=-45, row=1, col=2)
        fig.update_xaxes(tickangle=-45, row=2, col=1)
        fig.update_xaxes(tickangle=-45, row=2, col=2)
        
        fig.update_layout(
            title_text="Комплексный анализ побед: Дома vs В гостях (Задача 2: Spark SQL)",
            title_x=0.5,
            height=900,
            showlegend=True,
            template='plotly_white',
            font=dict(size=11)
        )
        
        if save_path is None:
            save_path = self.output_dir / "task2_comprehensive_dashboard.html"
        
        fig.write_html(save_path)
        logger.info(f"✅ Dashboard сохранен: {save_path}")
        
        fig.show()
    
    def generate_summary_table(
        self, 
        df: pd.DataFrame, 
        save_path: Optional[str] = None
    ) -> None:
        """
        Создает и сохраняет таблицу с результатами в формате изображения
        
        Args:
            df: DataFrame с результатами
            save_path: Путь для сохранения
        """
        logger.info("Создание таблицы результатов...")
        
        # Определяем какие колонки есть в DataFrame
        available_cols = ['team_name']
        optional_cols = {
            'league_name': 'Лига',
            'home_matches': 'Матчей\nдома',
            'home_wins': 'Побед\nдома',
            'home_win_pct': '% побед\nдома',
            'away_matches': 'Матчей\nв гостях',
            'away_wins': 'Побед\nв гостях',
            'away_win_pct': '% побед\nв гостях',
            'total_win_pct': 'Общий\n% побед'
        }
        
        # Отбираем только те колонки, которые есть в DataFrame
        selected_cols = ['team_name']
        for col in optional_cols.keys():
            if col in df.columns:
                selected_cols.append(col)
        
        # Подготовка данных для таблицы
        table_df = df[selected_cols].copy()
        
        # Конвертируем все числовые колонки в float/int
        numeric_cols = ['home_matches', 'home_wins', 'home_win_pct',
                       'away_matches', 'away_wins', 'away_win_pct', 'total_win_pct']
        for col in numeric_cols:
            if col in table_df.columns:
                table_df[col] = pd.to_numeric(table_df[col], errors='coerce')
        
        # Названия колонок на русском
        col_names = ['Команда']
        for col in selected_cols[1:]:
            col_names.append(optional_cols.get(col, col))
        
        # Названия колонок на русском
        col_names = ['Команда']
        for col in selected_cols[1:]:
            col_names.append(optional_cols.get(col, col))
        
        # Создание графика с таблицей
        fig, ax = plt.subplots(figsize=(16, len(df) * 0.5 + 2))
        ax.axis('tight')
        ax.axis('off')
        
        table = ax.table(
            cellText=table_df.values,
            colLabels=col_names,
            cellLoc='center',
            loc='center',
            colColours=['#f0f0f0'] * len(col_names)
        )
        
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        
        # Стилизация заголовков
        for i in range(len(col_names)):
            table[(0, i)].set_facecolor('#3498db')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        plt.title(
            'Детальная статистика побед команд (Задача 2: Spark SQL)',
            fontsize=14,
            fontweight='bold',
            pad=20
        )
        
        if save_path is None:
            save_path = self.output_dir / "task2_results_table.png"
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"✅ Таблица сохранена: {save_path}")
        
        plt.close()

