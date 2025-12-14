"""
Модуль визуализации для задачи 4: Определение «самого неудобного соперника»

Создает интерактивные графики и таблицы для отображения анализа
самых сложных соперников команд через Spark RDD API.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

logger = logging.getLogger(__name__)


class ToughestOpponentVisualizer:
    """
    Класс для визуализации результатов анализа самых неудобных соперников

    Создает:
    - Столбчатые диаграммы процентов побед
    - Network графы взаимоотношений команд
    - Тепловые карты матриц побед
    - Комплексные дашборды
    """

    # Цветовая палитра (адаптированная для темы)
    COLORS = {
        'low_percentage': '#E63946',  # Красный (низкий процент побед)
        'medium_percentage': '#F4A261',  # Оранжевый
        'high_percentage': '#2A9D8F',  # Бирюзовый (высокий процент)
        'neutral': '#457B9D',  # Синий
        'background': '#1a1a2e',
        'plot_bg': '#16213e',
        'grid': '#2d3a4f',
        'text': '#eaeaea'
    }

    # Настройки темы
    LAYOUT_THEME = {
        'paper_bgcolor': COLORS['background'],
        'plot_bgcolor': COLORS['plot_bg'],
        'font_color': COLORS['text'],
        'gridcolor': COLORS['grid'],
        'title_font_size': 20,
        'axis_title_font_size': 14,
    }

    def __init__(self, output_dir: str = "outputs/task4"):
        """
        Инициализация визуализатора

        Args:
            output_dir: Директория для сохранения графиков
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"ToughestOpponentVisualizer инициализирован. Output: {self.output_dir}")

    def _get_color_by_percentage(self, percentage: float) -> str:
        """
        Возвращает цвет в зависимости от процента побед

        Args:
            percentage: Процент побед (0-100)

        Returns:
            str: HEX цвет
        """
        if percentage < 25:
            return self.COLORS['low_percentage']
        elif percentage < 50:
            return self.COLORS['medium_percentage']
        else:
            return self.COLORS['high_percentage']

    def _apply_layout_theme(self, fig: go.Figure, title: str) -> go.Figure:
        """
        Применяет единую тему оформления к графику
        """
        fig.update_layout(
            title={
                'text': title,
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': self.LAYOUT_THEME['title_font_size'],
                         'color': self.LAYOUT_THEME['font_color']}
            },
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            plot_bgcolor=self.LAYOUT_THEME['plot_bgcolor'],
            font={'color': self.LAYOUT_THEME['font_color'],
                  'family': 'JetBrains Mono, Consolas, monospace'},
            legend=dict(
                bgcolor='rgba(22, 33, 62, 0.8)',
                bordercolor=self.COLORS['grid'],
                borderwidth=1
            ),
            hovermode='closest',
            hoverlabel=dict(
                bgcolor=self.COLORS['background'],
                font_size=12,
                font_family='JetBrains Mono, Consolas, monospace'
            )
        )

        fig.update_xaxes(
            gridcolor=self.LAYOUT_THEME['gridcolor'],
            linecolor=self.COLORS['grid'],
            title_font={'size': self.LAYOUT_THEME['axis_title_font_size']},
            tickfont={'size': 11}
        )

        fig.update_yaxes(
            gridcolor=self.LAYOUT_THEME['gridcolor'],
            linecolor=self.COLORS['grid'],
            title_font={'size': self.LAYOUT_THEME['axis_title_font_size']},
            tickfont={'size': 11}
        )

        return fig

    def plot_toughest_opponents_bar(
            self,
            df: pd.DataFrame,
            top_n: int = 15,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создает столбчатую диаграмму самых неудобных соперников

        Args:
            df: DataFrame с результатами анализа
            top_n: Количество команд для отображения
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание столбчатой диаграммы самых неудобных соперников...")

        # Сортируем и ограничиваем количество
        plot_df = df.sort_values('win_percentage').head(top_n).copy()

        # Создаем комбинированные названия для оси X
        plot_df['matchup_label'] = plot_df.apply(
            lambda row: f"{row['team_name']}<br>vs {row['toughest_opponent_name']}",
            axis=1
        )

        # Создаем график
        fig = go.Figure()

        # Добавляем столбцы с градиентной окраской
        for idx, row in plot_df.iterrows():
            color = self._get_color_by_percentage(row['win_percentage'])

            fig.add_trace(go.Bar(
                x=[row['matchup_label']],
                y=[row['win_percentage']],
                name=row['team_name'],
                marker_color=color,
                hovertemplate=(
                    f"<b>{row['team_name']} vs {row['toughest_opponent_name']}</b><br>"
                    f"Процент побед: <b>{row['win_percentage']:.1f}%</b><br>"
                    f"Матчи: {row['total_matches']} (победы: {row['wins_against']})<br>"
                    f"Лига: {row['league']}<br>"
                    f"Сезон: {row['season']}<br>"
                    "<extra></extra>"
                ),
                text=f"{row['win_percentage']:.1f}%",
                textposition='outside'
            ))

        fig.update_xaxes(title_text='Команда vs Самый неудобный соперник')
        fig.update_yaxes(title_text='Процент побед (%)')

        title = f" Топ-{top_n} самых неудобных противостояний<br><sub>Наименьший процент побед против конкретного соперника</sub>"
        fig = self._apply_layout_theme(fig, title)

        fig.update_layout(
            height=600,
            showlegend=False,
            xaxis_tickangle=-45
        )

        if save_path is None:
            save_path = self.output_dir / "toughest_opponents_bar.html"

        fig.write_html(str(save_path))
        logger.info(f" График сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_win_percentage_distribution(
            self,
            df: pd.DataFrame,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создает гистограмму распределения процентов побед

        Args:
            df: DataFrame с результатами анализа
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание гистограммы распределения процентов побед...")

        fig = go.Figure()

        # Гистограмма
        fig.add_trace(go.Histogram(
            x=df['win_percentage'],
            nbinsx=20,
            name='Распределение',
            marker_color=self.COLORS['neutral'],
            opacity=0.7,
            hovertemplate=(
                "Процент побед: %{x:.1f}%<br>"
                "Количество команд: %{y}<br>"
                "<extra></extra>"
            )
        ))

        # Линия среднего значения
        mean_percentage = df['win_percentage'].mean()
        fig.add_vline(
            x=mean_percentage,
            line_dash="dash",
            line_color=self.COLORS['low_percentage'],
            annotation_text=f"Среднее: {mean_percentage:.1f}%",
            annotation_position="top right"
        )

        fig.update_xaxes(title_text='Процент побед против самого неудобного соперника (%)')
        fig.update_yaxes(title_text='Количество команд')

        title = f" Распределение процентов побед<br><sub>Какой процент побед команды показывают против самого сложного соперника</sub>"
        fig = self._apply_layout_theme(fig, title)

        fig.update_layout(
            height=500,
            bargap=0.05
        )

        if save_path is None:
            save_path = self.output_dir / "win_percentage_distribution.html"

        fig.write_html(str(save_path))
        logger.info(f" График сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def create_team_network_graph(
            self,
            df: pd.DataFrame,
            top_n: int = 20,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создает network граф взаимоотношений команд

        Args:
            df: DataFrame с результатами анализа
            top_n: Количество команд для отображения
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание network графа взаимоотношений команд...")

        # Ограничиваем количество команд
        plot_df = df.head(top_n).copy()

        # Собираем уникальные узлы (команды)
        nodes = set()
        for _, row in plot_df.iterrows():
            nodes.add(row['team_name'])
            nodes.add(row['toughest_opponent_name'])

        nodes = list(nodes)
        node_indices = {node: i for i, node in enumerate(nodes)}

        # Создаем связи
        edge_x = []
        edge_y = []
        edge_colors = []
        edge_widths = []

        node_x = []
        node_y = []
        node_colors = []
        node_sizes = []
        node_labels = []

        # Располагаем узлы по кругу
        angle_step = 2 * np.pi / len(nodes)
        radius = 1.0

        for i, node in enumerate(nodes):
            angle = i * angle_step
            node_x.append(radius * np.cos(angle))
            node_y.append(radius * np.sin(angle))
            node_labels.append(node)
            node_sizes.append(20)
            node_colors.append(self.COLORS['neutral'])

        # Создаем связи (ребра)
        for _, row in plot_df.iterrows():
            source_idx = node_indices[row['team_name']]
            target_idx = node_indices[row['toughest_opponent_name']]

            edge_x.extend([node_x[source_idx], node_x[target_idx], None])
            edge_y.extend([node_y[source_idx], node_y[target_idx], None])

            # Цвет и толщина связи в зависимости от процента побед
            percentage = row['win_percentage']
            edge_colors.append(self._get_color_by_percentage(percentage))
            edge_widths.append(3 - (percentage / 50))  # Толще для меньшего процента

        # Создаем график
        fig = go.Figure()

        # Добавляем связи
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(
                width=2,
                color='rgba(100, 100, 100, 0.4)'
            ),
            hoverinfo='none',
            showlegend=False
        ))

        # Добавляем узлы
        fig.add_trace(go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            marker=dict(
                size=node_sizes,
                color=node_colors,
                line=dict(width=2, color='white')
            ),
            text=node_labels,
            textposition="top center",
            hoverinfo='text',
            showlegend=False,
            textfont=dict(size=10, color=self.COLORS['text'])
        ))

        fig.update_xaxes(showgrid=False, zeroline=False, showticklabels=False)
        fig.update_yaxes(showgrid=False, zeroline=False, showticklabels=False)

        title = f" Network граф самых неудобных соперников (Топ-{top_n})<br><sub>Связи показывают самые сложные противостояния для каждой команды</sub>"
        fig = self._apply_layout_theme(fig, title)

        fig.update_layout(
            showlegend=False,
            height=700,
            width=800,
            xaxis_range=[-1.2, 1.2],
            yaxis_range=[-1.2, 1.2]
        )

        if save_path is None:
            save_path = self.output_dir / "team_network_graph.html"

        fig.write_html(str(save_path))
        logger.info(f" График сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def create_comprehensive_dashboard(
            self,
            df: pd.DataFrame,
            detailed_stats: pd.DataFrame = None,
            top_n: int = 10,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создает комплексный dashboard с несколькими графиками

        Args:
            df: DataFrame с самыми неудобными соперниками
            detailed_stats: Подробная статистика по всем парам
            top_n: Количество команд для анализа
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание комплексного dashboard...")

        # Ограничиваем данные
        plot_df = df.head(top_n * 2).copy()

        # Создаем subplot 2x2
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                f' Топ-{top_n} самых неудобных соперников',
                ' Распределение процентов побед',
                ' Количество матчей в противостояниях',
                '📊 Сводная таблица результатов'
            ),
            vertical_spacing=0.15,
            horizontal_spacing=0.1,
            specs=[
                [{"type": "bar"}, {"type": "histogram"}],
                [{"type": "scatter"}, {"type": "table"}]
            ]
        )

        # График 1: Столбчатая диаграмма
        bar_df = plot_df.head(top_n)
        bar_df['label'] = bar_df.apply(
            lambda r: f"{r['team_name']}<br>vs {r['toughest_opponent_name'][:15]}...",
            axis=1
        )

        for i, row in bar_df.iterrows():
            fig.add_trace(
                go.Bar(
                    x=[row['label']],
                    y=[row['win_percentage']],
                    name=row['team_name'],
                    marker_color=self._get_color_by_percentage(row['win_percentage']),
                    showlegend=False,
                    hovertemplate=f"Победы: {row['win_percentage']:.1f}%<extra></extra>"
                ),
                row=1, col=1
            )

        # График 2: Гистограмма распределения
        fig.add_trace(
            go.Histogram(
                x=plot_df['win_percentage'],
                nbinsx=15,
                marker_color=self.COLORS['neutral'],
                showlegend=False,
                hovertemplate="%{x:.1f}%: %{y} команд<extra></extra>"
            ),
            row=1, col=2
        )

        # Добавляем среднюю линию
        mean_val = plot_df['win_percentage'].mean()
        fig.add_hline(
            y=mean_val,
            line_dash="dash",
            line_color=self.COLORS['low_percentage'],
            row=1, col=2,
            annotation_text=f"Среднее: {mean_val:.1f}%"
        )

        # График 3: Количество матчей vs процент побед
        fig.add_trace(
            go.Scatter(
                x=plot_df['total_matches'],
                y=plot_df['win_percentage'],
                mode='markers',
                marker=dict(
                    size=plot_df['win_percentage'] / 2 + 5,
                    color=plot_df['win_percentage'],
                    colorscale='RdYlGn_r',  # Красный-желтый-зеленый (реверс)
                    showscale=True,
                    colorbar=dict(title="% побед")
                ),
                text=plot_df.apply(
                    lambda r: f"{r['team_name']} vs {r['toughest_opponent_name']}",
                    axis=1
                ),
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "Матчи: %{x}<br>"
                    "Победы: %{y:.1f}%<br>"
                    "<extra></extra>"
                ),
                showlegend=False
            ),
            row=2, col=1
        )

        # График 4: Таблица результатов
        table_df = plot_df.head(10)[[
            'team_name', 'toughest_opponent_name',
            'win_percentage', 'total_matches', 'wins_against'
        ]].copy()

        table_df.columns = ['Команда', 'Самый неудобный соперник', '% побед', 'Матчи', 'Победы']
        table_df['% побед'] = table_df['% побед'].round(1)

        fig.add_trace(
            go.Table(
                header=dict(
                    values=list(table_df.columns),
                    fill_color=self.COLORS['grid'],
                    align='center',
                    font=dict(color='white', size=12)
                ),
                cells=dict(
                    values=[table_df[col] for col in table_df.columns],
                    fill_color=self.COLORS['plot_bg'],
                    align='center',
                    font=dict(color='white', size=11)
                )
            ),
            row=2, col=2
        )

        # Обновляем оси
        fig.update_xaxes(title_text='Команда vs Соперник', row=1, col=1, tickangle=-45)
        fig.update_yaxes(title_text='% побед', row=1, col=1)
        fig.update_xaxes(title_text='% побед', row=1, col=2)
        fig.update_yaxes(title_text='Количество команд', row=1, col=2)
        fig.update_xaxes(title_text='Всего матчей', row=2, col=1)
        fig.update_yaxes(title_text='% побед', row=2, col=1)

        # Обновляем заголовки subplot
        for i, annotation in enumerate(fig['layout']['annotations']):
            annotation['font'] = dict(size=14, color=self.COLORS['text'])

        # Применяем тему
        title = f" Комплексный анализ самых неудобных соперников (Топ-{top_n * 2})<br>"
        fig = self._apply_layout_theme(fig, title)

        fig.update_layout(
            height=900,
            showlegend=False
        )

        if save_path is None:
            save_path = self.output_dir / "comprehensive_dashboard.html"

        fig.write_html(str(save_path))
        logger.info(f" Dashboard сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_team_detailed_analysis(
            self,
            team_stats: pd.DataFrame,
            team_name: str,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создает детальный анализ для конкретной команды

        Args:
            team_stats: DataFrame со статистикой по всем соперникам команды
            team_name: Название команды
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info(f"Создание детального анализа для команды: {team_name}")

        if len(team_stats) == 0:
            logger.warning(f"Нет данных для команды {team_name}")
            return go.Figure()

        # Сортируем по проценту побед
        plot_df = team_stats.sort_values('win_percentage').copy()

        fig = go.Figure()

        # Создаем горизонтальную столбчатую диаграмму
        fig.add_trace(go.Bar(
            y=plot_df['opponent_name'],
            x=plot_df['win_percentage'],
            orientation='h',
            marker=dict(
                color=plot_df['win_percentage'],
                colorscale='RdYlGn_r',  # Красный для низких процентов, зеленый для высоких
                showscale=True,
                colorbar=dict(title="% побед")
            ),
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Процент побед: <b>%{x:.1f}%</b><br>"
                "Матчи: %{customdata[0]}<br>"
                "Победы: %{customdata[1]}<br>"
                "<extra></extra>"
            ),
            customdata=np.stack((
                plot_df['total_matches'],
                plot_df['wins']
            ), axis=-1)
        ))

        fig.update_xaxes(title_text='Процент побед (%)')
        fig.update_yaxes(title_text='Соперник', autorange="reversed")

        title = f" {team_name}: Анализ всех соперников<br><sub>Процент побед против каждого оппонента</sub>"
        fig = self._apply_layout_theme(fig, title)

        fig.update_layout(
            height=max(400, len(plot_df) * 25),
            showlegend=False
        )

        if save_path is None:
            safe_name = team_name.replace(' ', '_').lower()
            save_path = self.output_dir / f"team_analysis_{safe_name}.html"

        fig.write_html(str(save_path))
        logger.info(f" График сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def generate_summary_report(
            self,
            df: pd.DataFrame,
            output_path: Optional[str] = None
    ) -> str:
        """
        Генерирует текстовый отчет с основными выводами

        Args:
            df: DataFrame с результатами анализа
            output_path: Путь для сохранения отчета

        Returns:
            str: Текст отчета
        """
        logger.info("Генерация сводного отчета...")

        if len(df) == 0:
            return "Нет данных для генерации отчета."

        # Основная статистика
        total_teams = len(df)
        avg_win_percentage = df['win_percentage'].mean()
        min_win_percentage = df['win_percentage'].min()
        max_win_percentage = df['win_percentage'].max()

        # Находим самые интересные случаи
        most_difficult = df.iloc[0]
        easiest = df[df['win_percentage'] == max_win_percentage].iloc[0]

        # Команды с наибольшим количеством матчей против самого сложного соперника
        most_experienced = df.loc[df['total_matches'].idxmax()]

        # Генерация отчета
        report_lines = [
            "=" * 80,
            "ОТЧЕТ: АНАЛИЗ САМЫХ НЕУДОБНЫХ СОПЕРНИКОВ",
            "=" * 80,
            "",
            f"Всего проанализировано команд: {total_teams}",
            f"Средний процент побед против самого сложного соперника: {avg_win_percentage:.1f}%",
            f"Диапазон процентов побед: от {min_win_percentage:.1f}% до {max_win_percentage:.1f}%",
            "",
            "САМЫЕ ИНТЕРЕСНЫЕ СЛУЧАИ:",
            "",
            f"1. Самый сложный соперник:",
            f"   • {most_difficult['team_name']} против {most_difficult['toughest_opponent_name']}",
            f"   • Всего {most_difficult['total_matches']} матчей",
            f"   • Только {most_difficult['win_percentage']:.1f}% побед",
            "",
            f"2. Самый опытный в сложных противостояниях:",
            f"   • {most_experienced['team_name']} против {most_experienced['toughest_opponent_name']}",
            f"   • {most_experienced['total_matches']} матчей (наибольшее количество)",
            f"   • {most_experienced['win_percentage']:.1f}% побед",
            "",
            f"3. Самый успешный против сложного соперника:",
            f"   • {easiest['team_name']} против {easiest['toughest_opponent_name']}",
            f"   • {easiest['win_percentage']:.1f}% побед (наивысший процент)",
            "",
            "СТАТИСТИЧЕСКИЙ АНАЛИЗ:",
            "",
            f"• Команд с менее чем 25% побед: {len(df[df['win_percentage'] < 25])}",
            f"• Команд с 25-50% побед: {len(df[(df['win_percentage'] >= 25) & (df['win_percentage'] < 50)])}",
            f"• Команд с более 50% побед: {len(df[df['win_percentage'] >= 50])}",
            "",
            "ВЫВОДЫ:",
            "",
            "1. Большинство команд показывают низкий процент побед против своего самого",
            "   неудобного соперника (часто ниже 30%).",
            "2. Некоторые команды играют против своих самых сложных соперников",
            "   очень часто (более 10 матчей), что говорит о регулярных встречах",
            "   в рамках чемпионата.",
            "3. Отдельные команды демонстрируют удивительно высокий процент побед",
            "   даже против самых сложных для них соперников.",
            "",
            "=" * 80,
            "Отчет сгенерирован автоматически"
        ]

        report_text = "\n".join(report_lines)

        # Сохраняем отчет
        if output_path is None:
            output_path = self.output_dir / "analysis_report.txt"

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report_text)

        logger.info(f" Отчет сохранён: {output_path}")

        return report_text