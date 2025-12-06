"""
Модуль визуализации анализа игровых стилей команд

Задача 1: Визуализация кластеризации игровых стилей команд
и анализа характеристик кластеров.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from plotly.colors import qualitative, sequential

logger = logging.getLogger(__name__)


class TeamStylesVisualizer:
    """
    Класс для визуализации результатов анализа игровых стилей команд

    Создает интерактивные графики через Plotly:
    - Визуализация кластеров в 2D/3D пространстве
    - Радиарные диаграммы характеристик стилей
    - Heatmap сравнения кластеров
    - Анализ распределения стилей по лигам
    """

    # Цветовая палитра для кластеров
    CLUSTER_COLORS = [
        '#FF6B6B',  # Красный
        '#4ECDC4',  # Бирюзовый
        '#FFD166',  # Жёлтый
        '#06D6A0',  # Зелёный
        '#118AB2',  # Синий
        '#073B4C',  # Тёмно-синий
        '#EF476F',  # Розовый
        '#7209B7',  # Фиолетовый
        '#F15BB5',  # Фуксия
        '#00BBF9',  # Голубой
        '#00F5D4',  # Циан
        '#FB5607',  # Оранжевый
        '#8338EC',  # Пурпурный
        '#3A86FF',  # Ярко-синий
        '#FF006E',  # Маджента
    ]

    # Цветовая палитра для лиг
    LEAGUE_COLORS = {
        'EPL': '#3D195B',  # Премьер-лига
        'La Liga': '#C41E3A',  # Ла Лига
        'Serie A': '#008C45',  # Серия А
        'Bundesliga': '#DA291C',  # Бундеслига
        'Ligue 1': '#091C3F',  # Лига 1
        'UEFA': '#1E88E5',  # Еврокубки
        'Other': '#757575'  # Другие
    }

    # Настройки темы графиков
    LAYOUT_THEME = {
        'paper_bgcolor': '#1a1a2e',
        'plot_bgcolor': '#16213e',
        'font_color': '#eaeaea',
        'gridcolor': '#2d3a4f',
        'title_font_size': 20,
        'axis_title_font_size': 14,
    }

    def __init__(self, output_dir: str = "outputs/team_styles"):
        """
        Инициализация визуализатора

        Args:
            output_dir: Директория для сохранения графиков
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"TeamStylesVisualizer инициализирован. Output: {self.output_dir}")

    def _get_cluster_color(self, cluster_id: int) -> str:
        """Возвращает цвет для кластера"""
        return self.CLUSTER_COLORS[cluster_id % len(self.CLUSTER_COLORS)]

    def _get_league_color(self, league_name: str) -> str:
        """Возвращает цвет для лиги"""
        for key in self.LEAGUE_COLORS:
            if key in league_name:
                return self.LEAGUE_COLORS[key]
        return self.LEAGUE_COLORS['Other']

    def _apply_layout_theme(self, fig: go.Figure, title: str) -> go.Figure:
        """
        Применяет единую тему оформления к графику
        """
        fig.update_layout(
            title={
                'text': title,
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': self.LAYOUT_THEME['title_font_size'], 'color': self.LAYOUT_THEME['font_color']}
            },
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            plot_bgcolor=self.LAYOUT_THEME['plot_bgcolor'],
            font={'color': self.LAYOUT_THEME['font_color'], 'family': 'JetBrains Mono, Consolas, monospace'},
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1,
                bgcolor='rgba(22, 33, 62, 0.8)',
                bordercolor='#2d3a4f',
                borderwidth=1
            ),
            hovermode='closest',
            hoverlabel=dict(
                bgcolor='#1a1a2e',
                font_size=12,
                font_family='JetBrains Mono, Consolas, monospace'
            )
        )

        fig.update_xaxes(
            gridcolor=self.LAYOUT_THEME['gridcolor'],
            linecolor='#2d3a4f',
            title_font={'size': self.LAYOUT_THEME['axis_title_font_size']},
            tickfont={'size': 11}
        )

        fig.update_yaxes(
            gridcolor=self.LAYOUT_THEME['gridcolor'],
            linecolor='#2d3a4f',
            title_font={'size': self.LAYOUT_THEME['axis_title_font_size']},
            tickfont={'size': 11}
        )

        return fig

    def plot_clusters_2d(
            self,
            teams_df: pd.DataFrame,
            feature_x: str = "attacking_power",
            feature_y: str = "possession_control",
            size_col: str = "attack_efficiency",
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Визуализация кластеров в 2D пространстве

        Args:
            teams_df: DataFrame с данными команд и кластерами
            feature_x: Признак для оси X
            feature_y: Признак для оси Y
            size_col: Признак для размера точек
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info(f"Создание 2D визуализации кластеров: {feature_x} vs {feature_y}")

        fig = go.Figure()

        # Группируем по кластерам
        clusters = sorted(teams_df['cluster'].unique())

        for cluster_id in clusters:
            cluster_data = teams_df[teams_df['cluster'] == cluster_id]
            style_name = cluster_data['playing_style'].iloc[
                0] if 'playing_style' in cluster_data.columns else f"Кластер {cluster_id}"

            fig.add_trace(go.Scatter(
                x=cluster_data[feature_x],
                y=cluster_data[feature_y],
                mode='markers',
                name=f"{style_name}",
                marker=dict(
                    size=cluster_data[size_col] * 20 if size_col in cluster_data.columns else 10,
                    color=self._get_cluster_color(cluster_id),
                    line=dict(width=1, color='#1a1a2e'),
                    opacity=0.8,
                    sizemode='area',
                    sizeref=0.1,
                ),
                text=cluster_data['team_name'] + '<br>' + cluster_data.get('league_name', ''),
                hovertemplate=(
                    '<b>%{text}</b><br>'
                    f'{feature_x}: %{{x:.2f}}<br>'
                    f'{feature_y}: %{{y:.2f}}<br>'
                    '<extra></extra>'
                )
            ))

        # Добавляем центроиды если есть данные
        if 'centers' in locals():
            for cluster_id, center in enumerate(centers):
                fig.add_trace(go.Scatter(
                    x=[center[feature_x_idx]],
                    y=[center[feature_y_idx]],
                    mode='markers',
                    name=f"Центроид {cluster_id}",
                    marker=dict(
                        symbol='x',
                        size=15,
                        color=self._get_cluster_color(cluster_id),
                        line=dict(width=2)
                    ),
                    showlegend=False
                ))

        fig.update_xaxes(title_text=feature_x.replace('_', ' ').title())
        fig.update_yaxes(title_text=feature_y.replace('_', ' ').title())

        title = f"🔍 Кластеризация игровых стилей<br><sub>{feature_x.replace('_', ' ')} vs {feature_y.replace('_', ' ')}</sub>"
        fig = self._apply_layout_theme(fig, title)

        fig.update_layout(
            height=700,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        if save_path is None:
            save_path = self.output_dir / "clusters_2d_scatter.html"

        fig.write_html(str(save_path))
        logger.info(f"✅ График сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_clusters_3d(
            self,
            teams_df: pd.DataFrame,
            feature_x: str = "attacking_power",
            feature_y: str = "possession_control",
            feature_z: str = "attack_efficiency",
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        3D визуализация кластеров

        Args:
            teams_df: DataFrame с данными команд и кластерами
            feature_x: Признак для оси X
            feature_y: Признак для оси Y
            feature_z: Признак для оси Z
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info(f"Создание 3D визуализации кластеров")

        fig = go.Figure()

        clusters = sorted(teams_df['cluster'].unique())

        for cluster_id in clusters:
            cluster_data = teams_df[teams_df['cluster'] == cluster_id]
            style_name = cluster_data['playing_style'].iloc[
                0] if 'playing_style' in cluster_data.columns else f"Кластер {cluster_id}"

            fig.add_trace(go.Scatter3d(
                x=cluster_data[feature_x],
                y=cluster_data[feature_y],
                z=cluster_data[feature_z],
                mode='markers',
                name=f"{style_name}",
                marker=dict(
                    size=6,
                    color=self._get_cluster_color(cluster_id),
                    opacity=0.8,
                    line=dict(width=0.5, color='#1a1a2e')
                ),
                text=cluster_data['team_name'],
                hovertemplate=(
                    '<b>%{text}</b><br>'
                    f'{feature_x}: %{{x:.2f}}<br>'
                    f'{feature_y}: %{{y:.2f}}<br>'
                    f'{feature_z}: %{{z:.2f}}<br>'
                    '<extra></extra>'
                )
            ))

        fig.update_layout(
            scene=dict(
                xaxis_title=feature_x.replace('_', ' ').title(),
                yaxis_title=feature_y.replace('_', ' ').title(),
                zaxis_title=feature_z.replace('_', ' ').title(),
                bgcolor=self.LAYOUT_THEME['plot_bgcolor'],
                xaxis=dict(
                    backgroundcolor=self.LAYOUT_THEME['plot_bgcolor'],
                    gridcolor=self.LAYOUT_THEME['gridcolor'],
                    showbackground=True
                ),
                yaxis=dict(
                    backgroundcolor=self.LAYOUT_THEME['plot_bgcolor'],
                    gridcolor=self.LAYOUT_THEME['gridcolor'],
                    showbackground=True
                ),
                zaxis=dict(
                    backgroundcolor=self.LAYOUT_THEME['plot_bgcolor'],
                    gridcolor=self.LAYOUT_THEME['gridcolor'],
                    showbackground=True
                )
            ),
            title=dict(
                text="🔬 3D Визуализация игровых стилей<br><sub>Интерактивное исследование кластеров</sub>",
                x=0.5,
                font=dict(size=20, color=self.LAYOUT_THEME['font_color'])
            ),
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            font=dict(color=self.LAYOUT_THEME['font_color']),
            height=800,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )

        if save_path is None:
            save_path = self.output_dir / "clusters_3d_scatter.html"

        fig.write_html(str(save_path))
        logger.info(f"✅ 3D график сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_radar_chart_clusters(
            self,
            cluster_analysis: pd.DataFrame,
            metrics: List[str] = None,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Радиарные диаграммы для сравнения характеристик кластеров

        Args:
            cluster_analysis: DataFrame с анализом кластеров
            metrics: Список метрик для сравнения
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание радиарных диаграмм кластеров")

        if metrics is None:
            metrics = [
                'avg_attacking', 'avg_possession', 'avg_efficiency',
                'avg_creativity', 'avg_aggressiveness', 'avg_age'
            ]

        fig = go.Figure()

        for _, row in cluster_analysis.iterrows():
            values = []
            for metric in metrics:
                values.append(row[metric])

            # Замыкаем круг
            values.append(values[0])
            metrics_circle = metrics + [metrics[0]]

            fig.add_trace(go.Scatterpolar(
                r=values,
                theta=[m.replace('avg_', '').replace('_', ' ').title() for m in metrics_circle],
                name=row['style_name'],
                line_color=self._get_cluster_color(row['cluster_id']),
                fill='toself',
                opacity=0.6
            ))

        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, max([max(cluster_analysis[m]) for m in metrics]) * 1.2]
                ),
                bgcolor='rgba(22, 33, 62, 0.5)'
            ),
            title=dict(
                text="📊 Сравнение характеристик стилей<br><sub>Радиарная диаграмма средних значений</sub>",
                x=0.5,
                font=dict(size=20, color=self.LAYOUT_THEME['font_color'])
            ),
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            font=dict(color=self.LAYOUT_THEME['font_color']),
            height=700,
            showlegend=True
        )

        if save_path is None:
            save_path = self.output_dir / "clusters_radar_chart.html"

        fig.write_html(str(save_path))
        logger.info(f"✅ Радиарная диаграмма сохранена: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_cluster_heatmap(
            self,
            cluster_analysis: pd.DataFrame,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Heatmap характеристик кластеров

        Args:
            cluster_analysis: DataFrame с анализом кластеров
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание heatmap характеристик кластеров")

        # Выбираем числовые колонки для heatmap
        numeric_cols = cluster_analysis.select_dtypes(include=[np.number]).columns
        exclude_cols = ['cluster_id', 'team_count']
        heatmap_cols = [col for col in numeric_cols if col not in exclude_cols and col.startswith('avg_')]

        # Подготавливаем данные
        heatmap_data = cluster_analysis[['style_name'] + heatmap_cols].copy()
        heatmap_data = heatmap_data.set_index('style_name')

        # Транспонируем для лучшей визуализации
        heatmap_data = heatmap_data.T

        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data.values,
            x=heatmap_data.columns,
            y=[col.replace('avg_', '').replace('_', ' ').title() for col in heatmap_data.index],
            colorscale='Viridis',
            hoverongaps=False,
            text=heatmap_data.values.round(2),
            texttemplate='%{text}',
            textfont={"size": 10},
            colorbar=dict(
                title="Значение"
            )
        ))

        fig.update_layout(
            title=dict(
                text="🔥 Heatmap характеристик игровых стилей<br><sub>Сравнение средних значений по кластерам</sub>",
                x=0.5,
                font=dict(size=20, color=self.LAYOUT_THEME['font_color'])
            ),
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            font=dict(color=self.LAYOUT_THEME['font_color']),
            height=600,
            xaxis_title="Стили игры",
            yaxis_title="Характеристики"
        )

        if save_path is None:
            save_path = self.output_dir / "clusters_heatmap.html"

        fig.write_html(str(save_path))
        logger.info(f"✅ Heatmap сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_league_distribution(
            self,
            league_distribution: pd.DataFrame,
            top_n_leagues: int = 10,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Визуализация распределения стилей по лигам

        Args:
            league_distribution: DataFrame с распределением по лигам
            top_n_leagues: Количество топ лиг для отображения
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание графика распределения стилей по лигам")

        # Выбираем топ лиги по общему количеству команд
        top_leagues = league_distribution.sum(axis=1).nlargest(top_n_leagues).index

        # Фильтруем данные
        plot_data = league_distribution.loc[top_leagues]

        fig = go.Figure()

        for style in plot_data.columns:
            fig.add_trace(go.Bar(
                x=plot_data.index,
                y=plot_data[style],
                name=style,
                text=plot_data[style],
                textposition='auto',
                hovertemplate=(
                    '<b>%{x}</b><br>'
                    'Стиль: %{fullData.name}<br>'
                    'Команд: %{y}<br>'
                    '<extra></extra>'
                )
            ))

        fig.update_layout(
            barmode='stack',
            title=dict(
                text="🌍 Распределение стилей по лигам<br><sub>Доминирующие тактические подходы в разных чемпионатах</sub>",
                x=0.5,
                font=dict(size=20, color=self.LAYOUT_THEME['font_color'])
            ),
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            font=dict(color=self.LAYOUT_THEME['font_color']),
            height=600,
            xaxis_title="Лига",
            yaxis_title="Количество команд",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        if save_path is None:
            save_path = self.output_dir / "league_styles_distribution.html"

        fig.write_html(str(save_path))
        logger.info(f"✅ График распределения сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_style_changes(
            self,
            style_changes: pd.DataFrame,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Визуализация изменений стилей команд

        Args:
            style_changes: DataFrame с изменениями стилей
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        if style_changes.empty:
            logger.warning("Нет данных об изменениях стилей")
            return go.Figure()

        logger.info("Создание графика изменений стилей")

        # Подсчитываем частоту переходов
        transition_counts = style_changes['change_description'].value_counts().head(15)

        fig = go.Figure(data=[
            go.Bar(
                x=transition_counts.values,
                y=transition_counts.index,
                orientation='h',
                marker_color='#4ECDC4',
                text=transition_counts.values,
                textposition='auto',
                hovertemplate=(
                    '<b>%{y}</b><br>'
                    'Количество команд: %{x}<br>'
                    '<extra></extra>'
                )
            )
        ])

        fig.update_layout(
            title=dict(
                text="🔄 Трансформации игровых стилей<br><sub>Самые частые тактические изменения команд</sub>",
                x=0.5,
                font=dict(size=20, color=self.LAYOUT_THEME['font_color'])
            ),
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            font=dict(color=self.LAYOUT_THEME['font_color']),
            height=600,
            xaxis_title="Количество команд",
            yaxis_title="Изменение стиля",
            showlegend=False
        )

        if save_path is None:
            save_path = self.output_dir / "style_changes_bar.html"

        fig.write_html(str(save_path))
        logger.info(f"✅ График изменений сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_team_style_analysis(
            self,
            team_row: pd.Series,
            cluster_means: pd.DataFrame,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Детальный анализ стиля конкретной команды

        Args:
            team_row: Данные команды
            cluster_means: Средние значения по кластерам
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info(f"Создание детального анализа для команды: {team_row['team_name']}")

        # Определяем метрики для сравнения
        metrics = [
            'attacking_power', 'possession_control', 'attack_efficiency',
            'creativity', 'aggressiveness', 'team_age_profile',
            'squad_rotation', 'attack_variety', 'attack_intensity'
        ]

        # Получаем значения команды
        team_values = []
        cluster_values = []

        for metric in metrics:
            if metric in team_row:
                team_values.append(team_row[metric])
            else:
                team_values.append(0)

            # Находим среднее значение по кластеру команды
            if metric.replace('_', ' ') in cluster_means.columns:
                cluster_col = metric.replace('_', ' ')
            elif f'avg_{metric}' in cluster_means.columns:
                cluster_col = f'avg_{metric}'
            else:
                cluster_col = None

            if cluster_col and cluster_col in cluster_means.columns:
                cluster_mean = cluster_means.loc[
                    cluster_means['cluster_id'] == team_row['cluster'],
                    cluster_col
                ].values[0]
                cluster_values.append(cluster_mean)
            else:
                cluster_values.append(0)

        # Создаём subplot
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                f'📊 {team_row["team_name"]}: Профиль команды',
                '📈 Сравнение со средним по стилю',
                '🎯 Ключевые показатели',
                '⚖️ Баланс характеристик'
            ),
            specs=[[{"type": "polar"}, {"type": "bar"}],
                   [{"type": "scatter"}, {"type": "funnelarea"}]],
            vertical_spacing=0.15,
            horizontal_spacing=0.1
        )

        # 1. Радиарная диаграмма
        fig.add_trace(
            go.Scatterpolar(
                r=team_values[:6],
                theta=[m.replace('_', ' ').title() for m in metrics[:6]],
                fill='toself',
                name='Команда',
                line_color='#FF6B6B',
                opacity=0.8
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatterpolar(
                r=cluster_values[:6],
                theta=[m.replace('_', ' ').title() for m in metrics[:6]],
                fill='toself',
                name='Среднее по стилю',
                line_color='#4ECDC4',
                opacity=0.5
            ),
            row=1, col=1
        )

        # 2. Bar chart сравнения
        fig.add_trace(
            go.Bar(
                x=[m.replace('_', ' ').title() for m in metrics],
                y=team_values,
                name='Команда',
                marker_color='#FF6B6B',
                opacity=0.8
            ),
            row=1, col=2
        )

        fig.add_trace(
            go.Bar(
                x=[m.replace('_', ' ').title() for m in metrics],
                y=cluster_values,
                name='Среднее по стилю',
                marker_color='#4ECDC4',
                opacity=0.5
            ),
            row=1, col=2
        )

        # 3. Scatter plot ключевых показателей
        fig.add_trace(
            go.Scatter(
                x=['Атака', 'Владение', 'Эффективность'],
                y=[team_values[0], team_values[1], team_values[2]],
                mode='markers+text',
                name='Ключевые показатели',
                marker=dict(
                    size=[v * 10 for v in [team_values[0], team_values[1], team_values[2]]],
                    color=['#FF6B6B', '#4ECDC4', '#FFD166'],
                    line=dict(width=2, color='#1a1a2e')
                ),
                text=[f'{v:.2f}' for v in [team_values[0], team_values[1], team_values[2]]],
                textposition='top center',
                hovertemplate='<b>%{x}</b><br>Значение: %{y:.2f}<extra></extra>'
            ),
            row=2, col=1
        )

        # 4. Funnel area для баланса
        balance_labels = ['Атака', 'Контроль', 'Агрессия', 'Креативность']
        balance_values = [
            team_values[0] / max(team_values) if max(team_values) > 0 else 0,
            team_values[1] / 100,  # Проценты владения
            min(team_values[4] / 100, 1),  # Агрессивность
            team_values[3] / max(team_values[3], 1)
        ]

        fig.add_trace(
            go.Funnelarea(
                values=balance_values,
                text=balance_labels,
                marker=dict(
                    colors=['#FF6B6B', '#4ECDC4', '#FFD166', '#06D6A0']
                ),
                hovertemplate='<b>%{text}</b><br>Баланс: %{value:.2f}<extra></extra>'
            ),
            row=2, col=2
        )

        # Обновляем layout
        fig.update_layout(
            title=dict(
                text=f"🔍 Детальный анализ: {team_row['team_name']}<br><sub>Стиль: {team_row.get('playing_style', 'Не определен')} | Лига: {team_row.get('league_name', '')}</sub>",
                x=0.5,
                font=dict(size=22, color=self.LAYOUT_THEME['font_color'])
            ),
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            font=dict(color=self.LAYOUT_THEME['font_color']),
            height=900,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5
            )
        )

        # Обновляем subplot titles
        for annotation in fig['layout']['annotations']:
            annotation['font'] = dict(size=14, color=self.LAYOUT_THEME['font_color'])

        # Настраиваем оси
        fig.update_xaxes(title_text="Метрики", row=1, col=2)
        fig.update_yaxes(title_text="Значение", row=1, col=2)
        fig.update_xaxes(title_text="Показатели", row=2, col=1)
        fig.update_yaxes(title_text="Значение", row=2, col=1)

        if save_path is None:
            safe_name = team_row['team_name'].replace(' ', '_').lower()
            save_path = self.output_dir / f"team_analysis_{safe_name}.html"

        fig.write_html(str(save_path))
        logger.info(f"✅ Детальный анализ сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def create_comprehensive_dashboard(
            self,
            analysis_results: Dict,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создание комплексного dashboard анализа стилей

        Args:
            analysis_results: Результаты анализа из analyze_team_playing_styles
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание комплексного dashboard анализа стилей")

        teams_df = analysis_results['teams_with_styles']
        cluster_analysis = analysis_results['cluster_analysis']
        league_distribution = analysis_results['league_distribution']

        # Создаём subplot 3x3
        fig = make_subplots(
            rows=3, cols=3,
            subplot_titles=(
                '🔍 2D Визуализация кластеров',
                '📊 Радиарная диаграмма стилей',
                '🌍 Распределение по лигам',
                '🔥 Heatmap характеристик',
                '📈 Размеры кластеров',
                '🏆 Топ команды по стилям',
                '📅 Эволюция стилей',
                '⚖️ Баланс атаки и контроля',
                '🎯 Сводная статистика'
            ),
            specs=[
                [{"type": "scatter"}, {"type": "scatterpolar"}, {"type": "bar"}],
                [{"type": "heatmap"}, {"type": "pie"}, {"type": "bar"}],
                [{"type": "scatter"}, {"type": "scatter"}, {"type": "table"}]
            ],
            vertical_spacing=0.08,
            horizontal_spacing=0.05
        )

        # 1. 2D Scatter plot (row=1, col=1)
        clusters = sorted(teams_df['cluster'].unique())
        for cluster_id in clusters:
            cluster_data = teams_df[teams_df['cluster'] == cluster_id]
            fig.add_trace(
                go.Scatter(
                    x=cluster_data['attacking_power'],
                    y=cluster_data['possession_control'],
                    mode='markers',
                    name=f"Кластер {cluster_id}",
                    marker=dict(
                        color=self._get_cluster_color(cluster_id),
                        size=8,
                        opacity=0.7
                    ),
                    showlegend=False
                ),
                row=1, col=1
            )

        # 2. Radar chart (row=1, col=2)
        metrics = ['avg_attacking', 'avg_possession', 'avg_efficiency',
                   'avg_creativity', 'avg_aggressiveness']
        for _, row in cluster_analysis.iterrows():
            values = [row[m] for m in metrics]
            values.append(values[0])
            fig.add_trace(
                go.Scatterpolar(
                    r=values,
                    theta=[m.replace('avg_', '').replace('_', ' ').title() for m in metrics] +
                          [metrics[0].replace('avg_', '').replace('_', ' ').title()],
                    name=row['style_name'],
                    line_color=self._get_cluster_color(row['cluster_id']),
                    fill='toself',
                    opacity=0.6,
                    showlegend=False
                ),
                row=1, col=2
            )

        # 3. League distribution (row=1, col=3)
        top_leagues = league_distribution.sum(axis=1).nlargest(8).index
        plot_data = league_distribution.loc[top_leagues]
        for i, style in enumerate(plot_data.columns[:5]):  # Ограничиваем 5 стилями
            fig.add_trace(
                go.Bar(
                    x=plot_data.index,
                    y=plot_data[style],
                    name=style,
                    marker_color=self.CLUSTER_COLORS[i],
                    showlegend=False
                ),
                row=1, col=3
            )

        # 4. Heatmap (row=2, col=1)
        heatmap_cols = [col for col in cluster_analysis.columns
                        if col.startswith('avg_') and col not in ['avg_age', 'avg_aggressiveness']]
        heatmap_data = cluster_analysis[['style_name'] + heatmap_cols].set_index('style_name').T
        fig.add_trace(
            go.Heatmap(
                z=heatmap_data.values,
                x=heatmap_data.columns,
                y=[col.replace('avg_', '').replace('_', ' ').title() for col in heatmap_data.index],
                colorscale='Viridis',
                showscale=False,
                showlegend=False
            ),
            row=2, col=1
        )

        # 5. Pie chart sizes (row=2, col=2)
        fig.add_trace(
            go.Pie(
                labels=cluster_analysis['style_name'],
                values=cluster_analysis['team_count'],
                marker=dict(colors=[self._get_cluster_color(i) for i in range(len(cluster_analysis))]),
                hole=0.4,
                showlegend=False,
                textinfo='label+percent'
            ),
            row=2, col=2
        )

        # 6. Top teams bar (row=2, col=3)
        top_teams = teams_df.nlargest(10, 'attacking_power')
        fig.add_trace(
            go.Bar(
                x=top_teams['team_name'],
                y=top_teams['attacking_power'],
                marker_color=[self._get_cluster_color(c) for c in top_teams['cluster']],
                text=top_teams['playing_style'],
                textposition='auto'
            ),
            row=2, col=3
        )

        # 7. Style evolution scatter (row=3, col=1)
        if 'style_changes' in analysis_results and not analysis_results['style_changes'].empty:
            changes_df = analysis_results['style_changes']
            fig.add_trace(
                go.Scatter(
                    x=changes_df.index,
                    y=[1] * len(changes_df),
                    mode='markers',
                    marker=dict(
                        size=10,
                        color='#FFD166'
                    ),
                    text=changes_df['team_name'] + ': ' + changes_df['change_description']
                ),
                row=3, col=1
            )

        # 8. Attack vs Control scatter (row=3, col=2)
        fig.add_trace(
            go.Scatter(
                x=teams_df['attacking_power'],
                y=teams_df['possession_control'],
                mode='markers',
                marker=dict(
                    color=teams_df['attack_efficiency'],
                    colorscale='RdYlGn',
                    size=8,
                    showscale=True,
                    colorbar=dict(
                        title="Эффективность",
                        x=1.02
                    )
                ),
                text=teams_df['team_name']
            ),
            row=3, col=2
        )

        # 9. Summary table (row=3, col=3)
        summary_stats = [
            ['Метрика', 'Значение'],
            ['Всего команд', len(teams_df)],
            ['Количество стилей', len(cluster_analysis)],
            ['Силуэтный коэффициент', f"{analysis_results.get('silhouette_score', 0):.3f}"],
            ['Самый частый стиль', cluster_analysis.loc[cluster_analysis['team_count'].idxmax(), 'style_name']],
            ['Самый редкий стиль', cluster_analysis.loc[cluster_analysis['team_count'].idxmin(), 'style_name']]
        ]

        fig.add_trace(
            go.Table(
                header=dict(
                    values=[  # ← передаем оба столбца как список списков
                        list(zip(*summary_stats))[0][1:],  # Столбец 1: Метрики
                        list(zip(*summary_stats))[1][1:]  # Столбец 2: Значения
                    ],
                    fill_color='#2d3a4f',
                    align='left',
                    font=dict(color='white', size=12)
                ),
                cells=dict(
                    values=[  # ← передаем оба столбца как список списков
                        list(zip(*summary_stats))[0][1:],  # Столбец 1: Метрики
                        list(zip(*summary_stats))[1][1:]  # Столбец 2: Значения
                    ],
                    fill_color='#16213e',
                    align='left',
                    font=dict(color='white', size=11)
                ),
            ),
            row=3, col=3
        )

        # Обновляем layout
        fig.update_layout(
            title=dict(
                text="🏆 Комплексный Dashboard Анализа Игровых Стилей<br><sub>Многоаспектная визуализация тактических профилей команд</sub>",
                x=0.5,
                font=dict(size=24, color=self.LAYOUT_THEME['font_color'])
            ),
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            font=dict(color=self.LAYOUT_THEME['font_color']),
            height=1200,
            showlegend=False
        )

        # Обновляем subplot titles
        for annotation in fig['layout']['annotations']:
            annotation['font'] = dict(size=11, color=self.LAYOUT_THEME['font_color'])

        # Настраиваем оси
        fig.update_xaxes(title_text="Сила атаки", row=1, col=1)
        fig.update_yaxes(title_text="Владение мячом %", row=1, col=1)
        fig.update_xaxes(title_text="Лиги", row=1, col=3)
        fig.update_yaxes(title_text="Количество команд", row=1, col=3)
        fig.update_xaxes(title_text="Команды", row=2, col=3)
        fig.update_yaxes(title_text="Сила атаки", row=2, col=3)
        fig.update_xaxes(title_text="Сила атаки", row=3, col=2)
        fig.update_yaxes(title_text="Владение мячом %", row=3, col=2)

        if save_path is None:
            save_path = self.output_dir / "comprehensive_styles_dashboard.html"

        fig.write_html(str(save_path))
        logger.info(f"✅ Комплексный dashboard сохранён: {save_path}")

        if show:
            fig.show()

        return fig

    def generate_all_visualizations(self, analysis_results: Dict):
        """
        Генерация всех визуализаций для анализа стилей

        Args:
            analysis_results: Результаты анализа
        """
        logger.info("Запуск генерации всех визуализаций...")

        teams_df = analysis_results['teams_with_styles']
        cluster_analysis = analysis_results['cluster_analysis']
        league_distribution = analysis_results['league_distribution']
        style_changes = analysis_results.get('style_changes', pd.DataFrame())

        # 1. Основные графики кластеризации
        self.plot_clusters_2d(teams_df, show=False)
        self.plot_clusters_3d(teams_df, show=False)
        self.plot_cluster_heatmap(cluster_analysis, show=False)

        # 2. Анализ распределения
        if not league_distribution.empty:
            self.plot_league_distribution(league_distribution, show=False)

        # 3. Радиарные диаграммы
        self.plot_radar_chart_clusters(cluster_analysis, show=False)

        # 4. Анализ изменений
        if not style_changes.empty:
            self.plot_style_changes(style_changes, show=False)

        # 5. Комплексный dashboard
        self.create_comprehensive_dashboard(analysis_results, show=False)

        # 6. Анализ топ команд
        top_teams = teams_df.nlargest(5, 'attacking_power')
        for _, team_row in top_teams.iterrows():
            self.plot_team_style_analysis(team_row, cluster_analysis, show=False)

        logger.info(f"✅ Все визуализации сохранены в: {self.output_dir}")

        return {
            'output_dir': str(self.output_dir),
            'visualizations': [
                'clusters_2d_scatter.html',
                'clusters_3d_scatter.html',
                'clusters_radar_chart.html',
                'clusters_heatmap.html',
                'league_styles_distribution.html',
                'comprehensive_styles_dashboard.html'
            ]
        }