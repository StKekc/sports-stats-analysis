"""
Модуль визуализации для прогнозирования исходов матчей (Задача 5)

Создает визуализации для анализа результатов ML модели:
- Confusion matrix
- Важность признаков
- Распределение вероятностей
- Анализ ошибок
- Интерактивные dashboard'ы
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, classification_report

logger = logging.getLogger(__name__)


class MatchPredictionVisualizer:
    """
    Класс для визуализации результатов прогнозирования матчей

    Создает интерактивные графики для анализа ML модели:
    - Confusion matrix heatmap
    - Feature importance bars
    - Probability distributions
    - Error analysis plots
    - Interactive prediction explorer
    """

    # Цветовая палитра
    COLOR_PALETTE = {
        'home_win': '#E74C3C',  # Красный
        'draw': '#F39C12',  # Оранжевый
        'away_win': '#3498DB',  # Синий
        'correct': '#2ECC71',  # Зеленый
        'incorrect': '#E74C3C',  # Красный
        'feature': '#9B59B6',  # Фиолетовый
        'background': '#1a1a2e',
        'grid': '#2d3a4f',
        'text': '#eaeaea'
    }

    # Настройки темы
    LAYOUT_THEME = {
        'paper_bgcolor': '#1a1a2e',
        'plot_bgcolor': '#16213e',
        'font_color': '#eaeaea',
        'gridcolor': '#2d3a4f',
        'title_font_size': 22,
        'axis_title_font_size': 16,
        'legend_font_size': 14
    }

    # Названия классов
    CLASS_NAMES = {
        0: 'Home Win',
        1: 'Draw',
        2: 'Away Win'
    }

    CLASS_NAMES_RU = {
        0: 'Победа дома',
        1: 'Ничья',
        2: 'Победа в гостях'
    }

    def __init__(self, output_dir: str = "outputs/task5"):
        """
        Инициализация визуализатора

        Args:
            output_dir: Директория для сохранения графиков
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"MatchPredictionVisualizer инициализирован. Output: {self.output_dir}")

    def _apply_theme(self, fig: go.Figure, title: str) -> go.Figure:
        """
        Применяет единую тему оформления

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
                'font': {
                    'size': self.LAYOUT_THEME['title_font_size'],
                    'color': self.LAYOUT_THEME['font_color'],
                    'family': 'JetBrains Mono, Consolas, monospace'
                }
            },
            paper_bgcolor=self.LAYOUT_THEME['paper_bgcolor'],
            plot_bgcolor=self.LAYOUT_THEME['plot_bgcolor'],
            font={
                'color': self.LAYOUT_THEME['font_color'],
                'family': 'JetBrains Mono, Consolas, monospace',
                'size': 12
            },
            hoverlabel=dict(
                bgcolor='#1a1a2e',
                font_size=12,
                font_family='JetBrains Mono, Consolas, monospace'
            ),
            margin=dict(l=50, r=50, t=80, b=50)
        )

        fig.update_xaxes(
            gridcolor=self.LAYOUT_THEME['gridcolor'],
            linecolor=self.LAYOUT_THEME['gridcolor'],
            title_font={'size': self.LAYOUT_THEME['axis_title_font_size']}
        )

        fig.update_yaxes(
            gridcolor=self.LAYOUT_THEME['gridcolor'],
            linecolor=self.LAYOUT_THEME['gridcolor'],
            title_font={'size': self.LAYOUT_THEME['axis_title_font_size']}
        )

        return fig

    def plot_confusion_matrix(
            self,
            predictions_df: pd.DataFrame,
            normalize: bool = True,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создает heatmap confusion matrix

        Args:
            predictions_df: DataFrame с прогнозами
            normalize: Нормализовать ли значения
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание confusion matrix...")

        # Извлекаем фактические и предсказанные значения
        y_true = predictions_df['result_numeric'].values
        y_pred = predictions_df['prediction'].values

        # Создаем confusion matrix
        cm = confusion_matrix(y_true, y_pred, labels=[0, 1, 2])

        if normalize:
            cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
            cm_text = np.round(cm, 2)
            title_suffix = " (Нормализованная)"
            color_scale = 'Blues'
        else:
            cm_text = cm.astype(int)
            title_suffix = ""
            color_scale = 'Reds'

        # Создаем heatmap с правильной настройкой colorbar
        heatmap_trace = go.Heatmap(
            z=cm,
            x=['Home Win', 'Draw', 'Away Win'],
            y=['Home Win', 'Draw', 'Away Win'],
            text=cm_text,
            texttemplate='%{text}',
            textfont={'size': 14},
            colorscale=color_scale,
            hovertemplate=(
                'Факт: %{y}<br>'
                'Прогноз: %{x}<br>'
                'Значение: %{z:.3f}<br>'
                '<extra></extra>'
            ),
            colorbar=dict(
                title='Доля' if normalize else 'Количество'
            )
        )
        
        fig = go.Figure(data=heatmap_trace)

        # Добавляем заголовок и подписи
        total = len(predictions_df)
        correct = (y_true == y_pred).sum()
        accuracy = correct / total

        fig.update_layout(
            title=f'Confusion Matrix{title_suffix}<br>'
                  f'<sub>Accuracy: {accuracy:.1%} ({correct}/{total})</sub>',
            xaxis_title='Predicted',
            yaxis_title='Actual',
            width=700,
            height=600
        )

        fig = self._apply_theme(fig, "")

        # Сохранение
        if save_path is None:
            save_name = "confusion_matrix_normalized" if normalize else "confusion_matrix_raw"
            save_path = self.output_dir / f"{save_name}.html"

        fig.write_html(str(save_path))
        logger.info(f" Confusion matrix сохранена: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_feature_importance(
            self,
            feature_importance: List[Tuple[str, float]],
            top_n: int = 15,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создает bar chart важности признаков

        Args:
            feature_importance: Список кортежей (признак, важность)
            top_n: Количество топ признаков для отображения
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info(f"Создание графика важности признаков (топ-{top_n})...")

        # Сортируем и берем топ-N
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        top_features = feature_importance[:top_n]

        # Разделяем на признаки и значения
        features = [f[0] for f in top_features]
        importance = [f[1] for f in top_features]

        # Создаем bar chart
        fig = go.Figure(data=go.Bar(
            x=importance,
            y=features,
            orientation='h',
            marker=dict(
                color=self.COLOR_PALETTE['feature'],
                line=dict(color='#34495e', width=1)
            ),
            hovertemplate=(
                'Признак: %{y}<br>'
                'Важность: %{x:.4f}<br>'
                '<extra></extra>'
            )
        ))

        fig.update_layout(
            title=f'Важность признаков (топ-{top_n})',
            xaxis_title='Важность признака',
            yaxis_title='Признак',
            yaxis={'categoryorder': 'total ascending'},
            height=max(400, len(features) * 25),
            width=800
        )

        fig = self._apply_theme(fig, "")

        # Сохранение
        if save_path is None:
            save_path = self.output_dir / "feature_importance.html"

        fig.write_html(str(save_path))
        logger.info(f" График важности признаков сохранен: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_probability_distribution(
            self,
            predictions_df: pd.DataFrame,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создает распределение вероятностей по классам

        Args:
            predictions_df: DataFrame с прогнозами
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание распределения вероятностей...")

        # Извлекаем вероятности из строк
        prob_home = []
        prob_draw = []
        prob_away = []

        for prob_str in predictions_df['probabilities']:
            try:
                # Парсим строку формата "H:0.75, D:0.15, A:0.10"
                parts = prob_str.split(',')
                h = float(parts[0].split(':')[1])
                d = float(parts[1].split(':')[1])
                a = float(parts[2].split(':')[1])
                prob_home.append(h)
                prob_draw.append(d)
                prob_away.append(a)
            except:
                prob_home.append(0.33)
                prob_draw.append(0.33)
                prob_away.append(0.33)

        # Создаем subplot для каждого класса
        fig = make_subplots(
            rows=1, cols=3,
            subplot_titles=['Вероятность домашней победы',
                            'Вероятность ничьей',
                            'Вероятность выездной победы'],
            shared_yaxes=True
        )

        # Гистограмма для домашних побед
        fig.add_trace(
            go.Histogram(
                x=prob_home,
                name='Home Win',
                marker_color=self.COLOR_PALETTE['home_win'],
                nbinsx=20,
                opacity=0.7
            ),
            row=1, col=1
        )

        # Гистограмма для ничьих
        fig.add_trace(
            go.Histogram(
                x=prob_draw,
                name='Draw',
                marker_color=self.COLOR_PALETTE['draw'],
                nbinsx=20,
                opacity=0.7
            ),
            row=1, col=2
        )

        # Гистограмма для выездных побед
        fig.add_trace(
            go.Histogram(
                x=prob_away,
                name='Away Win',
                marker_color=self.COLOR_PALETTE['away_win'],
                nbinsx=20,
                opacity=0.7
            ),
            row=1, col=3
        )

        # Обновляем оси
        fig.update_xaxes(title_text='Вероятность', row=1, col=1, range=[0, 1])
        fig.update_xaxes(title_text='Вероятность', row=1, col=2, range=[0, 1])
        fig.update_xaxes(title_text='Вероятность', row=1, col=3, range=[0, 1])
        fig.update_yaxes(title_text='Количество матчей', row=1, col=1)

        fig.update_layout(
            title='Распределение вероятностей по классам',
            height=500,
            width=1000,
            showlegend=False
        )

        fig = self._apply_theme(fig, "")

        # Сохранение
        if save_path is None:
            save_path = self.output_dir / "probability_distribution.html"

        fig.write_html(str(save_path))
        logger.info(f" Распределение вероятностей сохранено: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_error_analysis(
            self,
            predictions_df: pd.DataFrame,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Анализ ошибок модели

        Args:
            predictions_df: DataFrame с прогнозами
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание графика анализа ошибок...")

        # Определяем правильные и неправильные прогнозы
        predictions_df['is_correct'] = (
                predictions_df['result_numeric'] == predictions_df['prediction']
        )

        # Анализ по типам исходов
        error_analysis = []

        for result_type in ['H', 'D', 'A']:
            mask = predictions_df['result'] == result_type
            if mask.any():
                subset = predictions_df[mask]
                correct = subset['is_correct'].sum()
                total = len(subset)
                error_rate = 1 - (correct / total)

                error_analysis.append({
                    'result_type': result_type,
                    'total': total,
                    'correct': correct,
                    'error_rate': error_rate,
                    'accuracy': correct / total
                })

        # Создаем DataFrame для анализа
        error_df = pd.DataFrame(error_analysis)

        # Создаем grouped bar chart
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=error_df['result_type'],
            y=error_df['total'],
            name='Всего матчей',
            marker_color='#34495e',
            text=error_df['total'],
            textposition='auto'
        ))

        fig.add_trace(go.Bar(
            x=error_df['result_type'],
            y=error_df['correct'],
            name='Правильные прогнозы',
            marker_color=self.COLOR_PALETTE['correct'],
            text=error_df['correct'],
            textposition='auto'
        ))

        # Добавляем accuracy как line chart (вторичная ось)
        fig.add_trace(go.Scatter(
            x=error_df['result_type'],
            y=error_df['accuracy'],
            name='Точность',
            yaxis='y2',
            mode='lines+markers',
            line=dict(color='#F39C12', width=3),
            marker=dict(size=10)
        ))

        fig.update_layout(
            title='Анализ ошибок по типам исходов',
            xaxis_title='Тип исхода',
            yaxis_title='Количество матчей',
            yaxis2=dict(
                title='Точность',
                overlaying='y',
                side='right',
                range=[0, 1],
                tickformat='.0%'
            ),
            barmode='group',
            height=600,
            width=800
        )

        fig = self._apply_theme(fig, "")

        # Сохранение
        if save_path is None:
            save_path = self.output_dir / "error_analysis.html"

        fig.write_html(str(save_path))
        logger.info(f" Анализ ошибок сохранен: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_prediction_explorer(
            self,
            predictions_df: pd.DataFrame,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Интерактивный explorer прогнозов

        Args:
            predictions_df: DataFrame с прогнозами
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание интерактивного explorer прогнозов...")

        # Подготавливаем данные
        df = predictions_df.copy()

        # Парсим вероятности
        prob_data = []
        for idx, row in df.iterrows():
            try:
                prob_str = row['probabilities']
                parts = prob_str.split(',')
                prob_h = float(parts[0].split(':')[1])
                prob_d = float(parts[1].split(':')[1])
                prob_a = float(parts[2].split(':')[1])

                # Преобразуем prediction в int, обрабатывая возможные NaN/None
                pred_val = row['prediction']
                if pd.isna(pred_val) or pred_val is None:
                    pred_class = 'H'  # Значение по умолчанию
                else:
                    pred_idx = int(float(pred_val))  # Сначала float, потом int для безопасности
                    pred_class = ['H', 'D', 'A'][pred_idx]
                
                prob_data.append({
                    'home_prob': prob_h,
                    'draw_prob': prob_d,
                    'away_prob': prob_a,
                    'max_prob': max(prob_h, prob_d, prob_a),
                    'predicted_class': pred_class
                })
            except:
                prob_data.append({
                    'home_prob': 0.33,
                    'draw_prob': 0.33,
                    'away_prob': 0.33,
                    'max_prob': 0.33,
                    'predicted_class': 'U'
                })

        prob_df = pd.DataFrame(prob_data)
        df = pd.concat([df, prob_df], axis=1)

        # Определяем правильность прогноза
        df['is_correct'] = df['result_numeric'] == df['prediction']
        df['result_type'] = df['result'].map({'H': 'Home Win', 'D': 'Draw', 'A': 'Away Win'})
        df['predicted_type'] = df['predicted_class'].map(
            {'H': 'Home Win', 'D': 'Draw', 'A': 'Away Win', 'U': 'Unknown'}
        )

        # Создаем scatter plot
        fig = px.scatter(
            df,
            x='home_prob',
            y='away_prob',
            color='is_correct',
            size='max_prob',
            hover_name='home_team_name',
            hover_data={
                'away_team_name': True,
                'result_type': True,
                'predicted_type': True,
                'home_prob': ':.2f',
                'draw_prob': ':.2f',
                'away_prob': ':.2f',
                'is_correct': False
            },
            color_discrete_map={
                True: self.COLOR_PALETTE['correct'],
                False: self.COLOR_PALETTE['incorrect']
            },
            title='Интерактивный explorer прогнозов',
            labels={
                'home_prob': 'Вероятность домашней победы',
                'away_prob': 'Вероятность выездной победы',
                'is_correct': 'Правильный прогноз'
            }
        )

        # Добавляем аннотации для углов
        fig.add_annotation(
            x=0.9, y=0.1,
            text="Высокая уверенность<br>в домашней победе",
            showarrow=False,
            font=dict(size=10, color='white'),
            bgcolor='rgba(0,0,0,0.5)',
            bordercolor='white',
            borderwidth=1
        )

        fig.add_annotation(
            x=0.1, y=0.9,
            text="Высокая уверенность<br>в выездной победе",
            showarrow=False,
            font=dict(size=10, color='white'),
            bgcolor='rgba(0,0,0,0.5)',
            bordercolor='white',
            borderwidth=1
        )

        fig.add_annotation(
            x=0.3, y=0.3,
            text="Неопределенность<br>(вероятность ничьей)",
            showarrow=False,
            font=dict(size=10, color='white'),
            bgcolor='rgba(0,0,0,0.5)',
            bordercolor='white',
            borderwidth=1
        )

        fig.update_layout(
            height=700,
            width=900,
            legend=dict(
                title='',
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1
            )
        )

        fig = self._apply_theme(fig, "")

        # Сохранение
        if save_path is None:
            save_path = self.output_dir / "prediction_explorer.html"

        fig.write_html(str(save_path))
        logger.info(f" Prediction explorer сохранен: {save_path}")

        if show:
            fig.show()

        return fig

    def create_comprehensive_dashboard(
            self,
            predictions_df: pd.DataFrame,
            feature_importance: List[Tuple[str, float]],
            metrics: Dict[str, float],
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Создает комплексный dashboard со всеми визуализациями

        Args:
            predictions_df: DataFrame с прогнозами
            feature_importance: Важность признаков
            metrics: Метрики модели
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание комплексного dashboard...")

        # Создаем subplot 2x2
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Confusion Matrix',
                'Важность признаков (топ-10)',
                'Распределение вероятностей',
                'Анализ ошибок'
            ),
            specs=[
                [{'type': 'heatmap'}, {'type': 'bar'}],
                [{'type': 'histogram', 'colspan': 2}, None]
            ],
            vertical_spacing=0.12,
            horizontal_spacing=0.1
        )

        # 1. Confusion Matrix
        y_true = predictions_df['result_numeric'].values
        y_pred = predictions_df['prediction'].values
        cm = confusion_matrix(y_true, y_pred, labels=[0, 1, 2])
        cm_normalized = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]

        fig.add_trace(
            go.Heatmap(
                z=cm_normalized,
                x=['Home', 'Draw', 'Away'],
                y=['Home', 'Draw', 'Away'],
                colorscale='Blues',
                showscale=True,
                colorbar=dict(
                    title='Доля',
                    x=0.45, 
                    y=0.8, 
                    len=0.35
                )
            ),
            row=1, col=1
        )

        # 2. Feature Importance (top 10)
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        top_features = feature_importance[:10]

        fig.add_trace(
            go.Bar(
                x=[f[1] for f in top_features],
                y=[f[0] for f in top_features],
                orientation='h',
                marker_color=self.COLOR_PALETTE['feature']
            ),
            row=1, col=2
        )

        # 3. Probability Distribution
        # Парсим вероятности
        prob_home = []
        for prob_str in predictions_df['probabilities']:
            try:
                parts = prob_str.split(',')
                prob_home.append(float(parts[0].split(':')[1]))
            except:
                prob_home.append(0.33)

        fig.add_trace(
            go.Histogram(
                x=prob_home,
                nbinsx=20,
                marker_color=self.COLOR_PALETTE['home_win'],
                name='Home Win Probability'
            ),
            row=2, col=1
        )

        # 4. Метрики модели в виде аннотаций
        accuracy = metrics.get('accuracy', 0)
        f1 = metrics.get('f1_score', 0)

        fig.add_annotation(
            x=0.5, y=-0.15,
            xref='paper', yref='paper',
            text=f"Accuracy: {accuracy:.3f} | F1-Score: {f1:.3f}",
            showarrow=False,
            font=dict(size=14, color='white'),
            bgcolor='rgba(0,0,0,0.5)',
            bordercolor='white',
            borderwidth=1
        )

        # Обновляем layout
        fig.update_layout(
            title='ML Model Dashboard: Прогноз исходов матчей',
            height=900,
            width=1200,
            showlegend=False
        )

        # Обновляем оси
        fig.update_xaxes(title_text='Predicted', row=1, col=1)
        fig.update_yaxes(title_text='Actual', row=1, col=1)
        fig.update_xaxes(title_text='Importance', row=1, col=2)
        fig.update_xaxes(title_text='Probability', row=2, col=1)
        fig.update_yaxes(title_text='Count', row=2, col=1)

        fig = self._apply_theme(fig, "")

        # Обновляем подзаголовки
        for annotation in fig['layout']['annotations']:
            if annotation['text'] in ['Confusion Matrix', 'Важность признаков (топ-10)',
                                      'Распределение вероятностей', 'Анализ ошибок']:
                annotation['font'] = dict(size=16, color='white')

        # Сохранение
        if save_path is None:
            save_path = self.output_dir / "comprehensive_dashboard.html"

        fig.write_html(str(save_path))
        logger.info(f" Comprehensive dashboard сохранен: {save_path}")

        if show:
            fig.show()

        return fig

    def plot_calibration_curve(
            self,
            predictions_df: pd.DataFrame,
            save_path: Optional[str] = None,
            show: bool = True
    ) -> go.Figure:
        """
        Кривая калибровки модели

        Args:
            predictions_df: DataFrame с прогнозами
            save_path: Путь для сохранения
            show: Показать график

        Returns:
            go.Figure
        """
        logger.info("Создание кривой калибровки...")

        # Извлекаем вероятности и фактические результаты
        prob_home = []
        actual_home = []

        for idx, row in predictions_df.iterrows():
            try:
                prob_str = row['probabilities']
                parts = prob_str.split(',')
                prob = float(parts[0].split(':')[1])
                actual = 1 if row['result'] == 'H' else 0

                prob_home.append(prob)
                actual_home.append(actual)
            except:
                continue

        if not prob_home:
            logger.warning("Не удалось извлечь вероятности для калибровки")
            return None

        # Бинниннг вероятностей
        bins = np.linspace(0, 1, 11)
        bin_centers = (bins[:-1] + bins[1:]) / 2

        mean_predicted = []
        mean_actual = []

        for i in range(len(bins) - 1):
            mask = (np.array(prob_home) >= bins[i]) & (np.array(prob_home) < bins[i + 1])
            if mask.any():
                mean_predicted.append(np.mean(np.array(prob_home)[mask]))
                mean_actual.append(np.mean(np.array(actual_home)[mask]))

        # Создаем график
        fig = go.Figure()

        # Идеальная калибровка (диагональ)
        fig.add_trace(go.Scatter(
            x=[0, 1],
            y=[0, 1],
            mode='lines',
            name='Идеальная калибровка',
            line=dict(color='white', dash='dash', width=2)
        ))

        # Кривая калибровки модели
        fig.add_trace(go.Scatter(
            x=mean_predicted,
            y=mean_actual,
            mode='markers+lines',
            name='Модель',
            line=dict(color=self.COLOR_PALETTE['home_win'], width=3),
            marker=dict(size=10, symbol='circle')
        ))

        fig.update_layout(
            title='Кривая калибровки модели (домашние победы)',
            xaxis_title='Предсказанная вероятность',
            yaxis_title='Факческая доля',
            xaxis=dict(range=[0, 1]),
            yaxis=dict(range=[0, 1]),
            width=700,
            height=600
        )

        fig = self._apply_theme(fig, "")

        # Добавляем метрику Brier Score
        from sklearn.metrics import brier_score_loss
        brier = brier_score_loss(actual_home, prob_home)

        fig.add_annotation(
            x=0.05, y=0.95,
            xref='paper', yref='paper',
            text=f"Brier Score: {brier:.3f}",
            showarrow=False,
            font=dict(size=12, color='white'),
            bgcolor='rgba(0,0,0,0.5)',
            bordercolor='white',
            borderwidth=1
        )

        # Сохранение
        if save_path is None:
            save_path = self.output_dir / "calibration_curve.html"

        fig.write_html(str(save_path))
        logger.info(f" Кривая калибровки сохранена: {save_path}")

        if show:
            fig.show()

        return fig

    def generate_model_report(
            self,
            predictions_df: pd.DataFrame,
            metrics: Dict[str, float],
            feature_importance: List[Tuple[str, float]],
            output_path: Optional[str] = None
    ) -> str:
        """
        Генерирует текстовый отчет о модели

        Args:
            predictions_df: DataFrame с прогнозами
            metrics: Метрики модели
            feature_importance: Важность признаков
            output_path: Путь для сохранения отчета

        Returns:
            str: Текст отчета
        """
        logger.info("Генерация отчета о модели...")

        from datetime import datetime
        import textwrap

        # Основные метрики
        accuracy = metrics.get('accuracy', 0)
        f1 = metrics.get('f1_score', 0)
        precision = metrics.get('precision', 0)
        recall = metrics.get('recall', 0)

        # Статистика по прогнозам
        total_matches = len(predictions_df)
        correct_predictions = (predictions_df['result_numeric'] == predictions_df['prediction']).sum()
        error_rate = 1 - (correct_predictions / total_matches)

        # Анализ по классам
        class_stats = []
        for result_type in ['H', 'D', 'A']:
            mask = predictions_df['result'] == result_type
            if mask.any():
                subset = predictions_df[mask]
                class_correct = (subset['result_numeric'] == subset['prediction']).sum()
                class_accuracy = class_correct / len(subset)

                class_stats.append({
                    'type': result_type,
                    'count': len(subset),
                    'correct': class_correct,
                    'accuracy': class_accuracy
                })

        # Важные признаки
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        top_features = feature_importance[:10]

        # Генерация отчета
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("ОТЧЕТ О МОДЕЛИ ПРОГНОЗИРОВАНИЯ ИСХОДОВ МАТЧЕЙ")
        report_lines.append("=" * 80)
        report_lines.append(f"Дата генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Всего матчей в тестовой выборке: {total_matches}")
        report_lines.append("")

        report_lines.append("МЕТРИКИ МОДЕЛИ:")
        report_lines.append("-" * 40)
        report_lines.append(f"Accuracy:         {accuracy:.3f}")
        report_lines.append(f"F1-Score:         {f1:.3f}")
        report_lines.append(f"Precision:        {precision:.3f}")
        report_lines.append(f"Recall:           {recall:.3f}")
        report_lines.append(f"Правильных:       {correct_predictions} ({correct_predictions / total_matches:.1%})")
        report_lines.append(f"Ошибок:           {total_matches - correct_predictions} ({error_rate:.1%})")
        report_lines.append("")

        report_lines.append("АНАЛИЗ ПО КЛАССАМ:")
        report_lines.append("-" * 40)
        for stat in class_stats:
            result_name = {
                'H': 'Домашние победы',
                'D': 'Ничьи',
                'A': 'Выездные победы'
            }[stat['type']]

            report_lines.append(f"{result_name:20} {stat['accuracy']:.1%} "
                                f"({stat['correct']}/{stat['count']})")
        report_lines.append("")

        report_lines.append("ВАЖНОСТЬ ПРИЗНАКОВ (ТОП-10):")
        report_lines.append("-" * 40)
        for i, (feature, importance) in enumerate(top_features, 1):
            report_lines.append(f"{i:2d}. {feature:30} {importance:.4f}")
        report_lines.append("")

        report_lines.append("РЕКОМЕНДАЦИИ ПО УЛУЧШЕНИЮ:")
        report_lines.append("-" * 40)
        recommendations = [
            "1. Добавить больше исторических данных (10+ сезонов)",
            "2. Включить данные о составах и травмах игроков",
            "3. Добавить метеоданные (температура, осадки)",
            "4. Учитывать мотивацию команд (турнирное положение)",
            "5. Попробовать ансамбль моделей (XGBoost + Neural Networks)",
            "6. Добавить временные признаки (день недели, время матча)",
            "7. Использовать трансферные стоимости игроков",
            "8. Учесть расстояние между городами команд"
        ]

        for rec in recommendations:
            report_lines.append(textwrap.fill(rec, width=79))

        report_lines.append("")
        report_lines.append("=" * 80)

        report_text = "\n".join(report_lines)

        # Сохранение отчета
        if output_path is None:
            output_path = self.output_dir / "model_report.txt"

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report_text)

        logger.info(f" Отчет о модели сохранен: {output_path}")

        return report_text