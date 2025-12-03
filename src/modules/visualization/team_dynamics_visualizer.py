"""
Модуль визуализации динамики результатов команд по сезонам

Задача 3: Создание интерактивных линейных графиков (time series)
для отображения кумулятивных метрик команд по ходу сезона.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px


logger = logging.getLogger(__name__)


class TeamDynamicsVisualizer:
    """
    Класс для визуализации динамики результатов команд
    
    Создает интерактивные линейные графики через Plotly:
    - Накопленные очки по ходу сезона
    - Накопленная разница мячей
    - Сравнение нескольких команд
    - Комплексные dashboards
    """
    
    # Цветовая палитра для команд (вдохновлена IDE темами)
    TEAM_COLORS = [
        '#E63946',  # Красный (насыщенный)
        '#457B9D',  # Синий (приглушённый)
        '#2A9D8F',  # Бирюзовый
        '#E9C46A',  # Золотой
        '#F4A261',  # Оранжевый
        '#9B59B6',  # Фиолетовый
        '#1ABC9C',  # Изумрудный
        '#E74C3C',  # Алый
        '#3498DB',  # Небесный
        '#F39C12',  # Янтарный
        '#8E44AD',  # Аметистовый
        '#16A085',  # Морской
        '#D35400',  # Тыквенный
        '#2980B9',  # Кобальт
        '#C0392B',  # Гранатовый
    ]
    
    # Настройки темы графиков
    LAYOUT_THEME = {
        'paper_bgcolor': '#1a1a2e',
        'plot_bgcolor': '#16213e',
        'font_color': '#eaeaea',
        'gridcolor': '#2d3a4f',
        'title_font_size': 20,
        'axis_title_font_size': 14,
    }
    
    def __init__(self, output_dir: str = "outputs/task3"):
        """
        Инициализация визуализатора
        
        Args:
            output_dir: Директория для сохранения графиков
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"TeamDynamicsVisualizer инициализирован. Output: {self.output_dir}")
    
    def _get_team_color(self, index: int) -> str:
        """Возвращает цвет для команды по индексу"""
        return self.TEAM_COLORS[index % len(self.TEAM_COLORS)]
    
    def _apply_layout_theme(self, fig: go.Figure, title: str) -> go.Figure:
        """
        Применяет единую тему оформления к графику
        
        Args:
            fig: Plotly Figure
            title: Заголовок графика
        
        Returns:
            go.Figure с примененной темой
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
            hovermode='x unified',
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
            tickfont={'size': 11},
            zeroline=True,
            zerolinecolor='#4a5568',
            zerolinewidth=1
        )
        
        return fig
    
    def plot_cumulative_points(
        self,
        df: pd.DataFrame,
        team_names: Optional[List[str]] = None,
        season_filter: Optional[str] = None,
        save_path: Optional[str] = None,
        show: bool = True
    ) -> go.Figure:
        """
        Строит линейный график накопленных очков по ходу сезона
        
        Args:
            df: DataFrame с данными динамики
            team_names: Список команд для отображения (если None — топ-6)
            season_filter: Фильтр по сезону
            save_path: Путь для сохранения HTML
            show: Показать график
        
        Returns:
            go.Figure
        """
        logger.info("Создание графика накопленных очков...")
        
        # Фильтрация данных
        plot_df = df.copy()
        
        if season_filter:
            plot_df = plot_df[plot_df['season_code'] == season_filter]
        
        # Если команды не указаны, берём топ-6 по итоговым очкам
        if team_names is None:
            # Находим максимальные очки для каждой команды в каждом сезоне
            max_points = plot_df.groupby(['team_name', 'season_code'])['cumulative_points'].max().reset_index()
            top_teams = max_points.groupby('team_name')['cumulative_points'].max().nlargest(6).index.tolist()
            team_names = top_teams
            logger.info(f"Автоматически выбраны топ-6 команд: {team_names}")
        
        plot_df = plot_df[plot_df['team_name'].isin(team_names)]
        
        # Создаём график
        fig = go.Figure()
        
        for i, team in enumerate(team_names):
            team_data = plot_df[plot_df['team_name'] == team].sort_values('match_number')
            
            fig.add_trace(go.Scatter(
                x=team_data['match_number'],
                y=team_data['cumulative_points'],
                mode='lines+markers',
                name=team,
                line=dict(
                    color=self._get_team_color(i),
                    width=3
                ),
                marker=dict(
                    size=6,
                    color=self._get_team_color(i),
                    line=dict(width=1, color='#1a1a2e')
                ),
                hovertemplate=(
                    f'<b>{team}</b><br>'
                    'Матч: %{x}<br>'
                    'Очки: %{y}<br>'
                    '<extra></extra>'
                )
            ))
        
        # Настройка осей
        fig.update_xaxes(title_text='Номер матча в сезоне')
        fig.update_yaxes(title_text='Накопленные очки')
        
        # Применяем тему
        season_text = f" ({season_filter})" if season_filter else ""
        title = f"📈 Динамика накопления очков{season_text}<br>"
        fig = self._apply_layout_theme(fig, title)
        
        fig.update_layout(height=600)
        
        # Сохранение
        if save_path is None:
            save_path = self.output_dir / "task3_cumulative_points.html"
        
        fig.write_html(str(save_path))
        logger.info(f"✅ График сохранён: {save_path}")
        
        if show:
            fig.show()
        
        return fig
    
    def plot_cumulative_goal_diff(
        self,
        df: pd.DataFrame,
        team_names: Optional[List[str]] = None,
        season_filter: Optional[str] = None,
        save_path: Optional[str] = None,
        show: bool = True
    ) -> go.Figure:
        """
        Строит линейный график накопленной разницы мячей
        
        Args:
            df: DataFrame с данными динамики
            team_names: Список команд для отображения
            season_filter: Фильтр по сезону
            save_path: Путь для сохранения HTML
            show: Показать график
        
        Returns:
            go.Figure
        """
        logger.info("Создание графика накопленной разницы мячей...")
        
        plot_df = df.copy()
        
        if season_filter:
            plot_df = plot_df[plot_df['season_code'] == season_filter]
        
        # Если команды не указаны — топ-6
        if team_names is None:
            max_gd = plot_df.groupby('team_name')['cumulative_goal_diff'].max()
            team_names = max_gd.nlargest(6).index.tolist()
        
        plot_df = plot_df[plot_df['team_name'].isin(team_names)]
        
        fig = go.Figure()
        
        for i, team in enumerate(team_names):
            team_data = plot_df[plot_df['team_name'] == team].sort_values('match_number')
            
            fig.add_trace(go.Scatter(
                x=team_data['match_number'],
                y=team_data['cumulative_goal_diff'],
                mode='lines+markers',
                name=team,
                line=dict(
                    color=self._get_team_color(i),
                    width=3
                ),
                marker=dict(size=5),
                hovertemplate=(
                    f'<b>{team}</b><br>'
                    'Матч: %{x}<br>'
                    'Разница мячей: %{y:+d}<br>'
                    '<extra></extra>'
                )
            ))
        
        # Добавляем нулевую линию
        fig.add_hline(y=0, line_dash="dash", line_color="#6c757d", line_width=2)
        
        fig.update_xaxes(title_text='Номер матча в сезоне')
        fig.update_yaxes(title_text='Накопленная разница мячей')
        
        season_text = f" ({season_filter})" if season_filter else ""
        title = f"⚽ Динамика разницы мячей{season_text}<br><sub>Положительное значение = забито больше, чем пропущено</sub>"
        fig = self._apply_layout_theme(fig, title)
        
        fig.update_layout(height=600)
        
        if save_path is None:
            save_path = self.output_dir / "task3_goal_diff_dynamics.html"
        
        fig.write_html(str(save_path))
        logger.info(f"✅ График сохранён: {save_path}")
        
        if show:
            fig.show()
        
        return fig
    
    def plot_monthly_aggregation(
        self,
        df: pd.DataFrame,
        team_names: Optional[List[str]] = None,
        season_filter: Optional[str] = None,
        save_path: Optional[str] = None,
        show: bool = True
    ) -> go.Figure:
        """
        Строит график очков по месяцам (агрегация)
        
        Args:
            df: DataFrame с данными динамики
            team_names: Список команд
            season_filter: Фильтр по сезону
            save_path: Путь для сохранения
            show: Показать график
        
        Returns:
            go.Figure
        """
        logger.info("Создание графика очков по месяцам...")
        
        plot_df = df.copy()
        
        # Преобразуем дату в месяц
        plot_df['match_date'] = pd.to_datetime(plot_df['match_date'])
        plot_df['month'] = plot_df['match_date'].dt.to_period('M').astype(str)
        
        if season_filter:
            plot_df = plot_df[plot_df['season_code'] == season_filter]
        
        if team_names is None:
            max_points = plot_df.groupby('team_name')['cumulative_points'].max()
            team_names = max_points.nlargest(6).index.tolist()
        
        plot_df = plot_df[plot_df['team_name'].isin(team_names)]
        
        # Агрегация по месяцам
        monthly_df = plot_df.groupby(['team_name', 'month']).agg({
            'points': 'sum',
            'goal_diff': 'sum',
            'goals_for': 'sum',
            'goals_against': 'sum'
        }).reset_index()
        
        fig = go.Figure()
        
        for i, team in enumerate(team_names):
            team_data = monthly_df[monthly_df['team_name'] == team].sort_values('month')
            
            fig.add_trace(go.Bar(
                x=team_data['month'],
                y=team_data['points'],
                name=team,
                marker_color=self._get_team_color(i),
                hovertemplate=(
                    f'<b>{team}</b><br>'
                    'Месяц: %{x}<br>'
                    'Очки: %{y}<br>'
                    '<extra></extra>'
                )
            ))
        
        fig.update_xaxes(title_text='Месяц')
        fig.update_yaxes(title_text='Очки за месяц')
        
        season_text = f" ({season_filter})" if season_filter else ""
        title = f"📅 Очки по месяцам{season_text}<br><sub>Агрегация результатов по календарным месяцам</sub>"
        fig = self._apply_layout_theme(fig, title)
        
        fig.update_layout(
            barmode='group',
            height=600
        )
        
        if save_path is None:
            save_path = self.output_dir / "task3_monthly_points.html"
        
        fig.write_html(str(save_path))
        logger.info(f"✅ График сохранён: {save_path}")
        
        if show:
            fig.show()
        
        return fig
    
    def create_comprehensive_dashboard(
        self,
        df: pd.DataFrame,
        team_names: Optional[List[str]] = None,
        season_filter: Optional[str] = None,
        save_path: Optional[str] = None,
        show: bool = True
    ) -> go.Figure:
        """
        Создаёт комплексный dashboard с несколькими графиками
        
        Args:
            df: DataFrame с данными динамики
            team_names: Список команд
            season_filter: Фильтр по сезону
            save_path: Путь для сохранения
            show: Показать график
        
        Returns:
            go.Figure
        """
        logger.info("Создание комплексного dashboard...")
        
        plot_df = df.copy()
        
        if season_filter:
            plot_df = plot_df[plot_df['season_code'] == season_filter]
        
        if team_names is None:
            max_points = plot_df.groupby('team_name')['cumulative_points'].max()
            team_names = max_points.nlargest(6).index.tolist()
        
        plot_df = plot_df[plot_df['team_name'].isin(team_names)]
        
        # Создаём subplot 2x2
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                '📈 Накопленные очки',
                '⚽ Накопленная разница мячей',
                '🎯 Забитые голы (накопительно)',
                '🛡️ Пропущенные голы (накопительно)'
            ),
            vertical_spacing=0.12,
            horizontal_spacing=0.08
        )
        
        for i, team in enumerate(team_names):
            team_data = plot_df[plot_df['team_name'] == team].sort_values('match_number')
            color = self._get_team_color(i)
            
            # График 1: Накопленные очки
            fig.add_trace(
                go.Scatter(
                    x=team_data['match_number'],
                    y=team_data['cumulative_points'],
                    mode='lines',
                    name=team,
                    line=dict(color=color, width=2),
                    legendgroup=team,
                    showlegend=True
                ),
                row=1, col=1
            )
            
            # График 2: Разница мячей
            fig.add_trace(
                go.Scatter(
                    x=team_data['match_number'],
                    y=team_data['cumulative_goal_diff'],
                    mode='lines',
                    name=team,
                    line=dict(color=color, width=2),
                    legendgroup=team,
                    showlegend=False
                ),
                row=1, col=2
            )
            
            # График 3: Забитые голы
            fig.add_trace(
                go.Scatter(
                    x=team_data['match_number'],
                    y=team_data['cumulative_goals_for'],
                    mode='lines',
                    name=team,
                    line=dict(color=color, width=2),
                    legendgroup=team,
                    showlegend=False
                ),
                row=2, col=1
            )
            
            # График 4: Пропущенные голы
            fig.add_trace(
                go.Scatter(
                    x=team_data['match_number'],
                    y=team_data['cumulative_goals_against'],
                    mode='lines',
                    name=team,
                    line=dict(color=color, width=2),
                    legendgroup=team,
                    showlegend=False
                ),
                row=2, col=2
            )
        
        # Добавляем нулевую линию на график разницы мячей
        fig.add_hline(y=0, line_dash="dash", line_color="#6c757d", row=1, col=2)
        
        # Обновляем оси
        fig.update_xaxes(title_text='Матч №', row=2, col=1)
        fig.update_xaxes(title_text='Матч №', row=2, col=2)
        fig.update_yaxes(title_text='Очки', row=1, col=1)
        fig.update_yaxes(title_text='GD', row=1, col=2)
        fig.update_yaxes(title_text='Голы', row=2, col=1)
        fig.update_yaxes(title_text='Голы', row=2, col=2)
        
        # Применяем тему
        season_text = f" — {season_filter}" if season_filter else ""
        title = f"🏆 Комплексный анализ динамики команд{season_text}<br>"
        fig = self._apply_layout_theme(fig, title)
        
        fig.update_layout(
            height=900,
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='center',
                x=0.5
            )
        )
        
        # Обновляем цвет фона для subplot titles
        for annotation in fig['layout']['annotations']:
            annotation['font'] = dict(size=14, color=self.LAYOUT_THEME['font_color'])
        
        if save_path is None:
            save_path = self.output_dir / "task3_comprehensive_dashboard.html"
        
        fig.write_html(str(save_path))
        logger.info(f"✅ Dashboard сохранён: {save_path}")
        
        if show:
            fig.show()
        
        return fig
    
    def create_season_comparison(
        self,
        df: pd.DataFrame,
        team_name: str,
        seasons: Optional[List[str]] = None,
        save_path: Optional[str] = None,
        show: bool = True
    ) -> go.Figure:
        """
        Сравнение динамики одной команды в разных сезонах
        
        Args:
            df: DataFrame с данными динамики
            team_name: Название команды
            seasons: Список сезонов для сравнения (если None — все)
            save_path: Путь для сохранения
            show: Показать график
        
        Returns:
            go.Figure
        """
        logger.info(f"Создание сравнения сезонов для команды: {team_name}")
        
        plot_df = df[df['team_name'] == team_name].copy()
        
        if seasons:
            plot_df = plot_df[plot_df['season_code'].isin(seasons)]
        
        unique_seasons = plot_df['season_code'].unique()
        
        fig = go.Figure()
        
        for i, season in enumerate(sorted(unique_seasons)):
            season_data = plot_df[plot_df['season_code'] == season].sort_values('match_number')
            
            fig.add_trace(go.Scatter(
                x=season_data['match_number'],
                y=season_data['cumulative_points'],
                mode='lines+markers',
                name=season,
                line=dict(color=self._get_team_color(i), width=3),
                marker=dict(size=5),
                hovertemplate=(
                    f'<b>{season}</b><br>'
                    'Матч: %{x}<br>'
                    'Очки: %{y}<br>'
                    '<extra></extra>'
                )
            ))
        
        fig.update_xaxes(title_text='Номер матча в сезоне')
        fig.update_yaxes(title_text='Накопленные очки')
        
        title = f"📊 {team_name}: Сравнение сезонов<br><sub>Динамика набора очков в разные годы</sub>"
        fig = self._apply_layout_theme(fig, title)
        
        fig.update_layout(height=600)
        
        if save_path is None:
            safe_name = team_name.replace(' ', '_').lower()
            save_path = self.output_dir / f"task3_season_comparison_{safe_name}.html"
        
        fig.write_html(str(save_path))
        logger.info(f"✅ График сохранён: {save_path}")
        
        if show:
            fig.show()
        
        return fig
    
    def generate_summary_stats(
        self,
        df: pd.DataFrame,
        season_filter: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Генерирует сводную статистику по динамике команд
        
        Args:
            df: DataFrame с данными динамики
            season_filter: Фильтр по сезону
        
        Returns:
            pd.DataFrame со сводной статистикой
        """
        logger.info("Генерация сводной статистики...")
        
        plot_df = df.copy()
        
        if season_filter:
            plot_df = plot_df[plot_df['season_code'] == season_filter]
        
        # Группируем по команде и сезону, берём последние значения
        summary = plot_df.groupby(['team_name', 'season_code', 'league_name']).agg({
            'match_number': 'max',
            'cumulative_points': 'max',
            'cumulative_goal_diff': 'max',
            'cumulative_goals_for': 'max',
            'cumulative_goals_against': 'max'
        }).reset_index()
        
        summary.columns = [
            'Команда', 'Сезон', 'Лига',
            'Матчей', 'Очки', 'Разница мячей',
            'Забито', 'Пропущено'
        ]
        
        summary = summary.sort_values(['Сезон', 'Очки'], ascending=[True, False])
        
        logger.info(f"✅ Сводная статистика: {len(summary)} записей")
        
        return summary

