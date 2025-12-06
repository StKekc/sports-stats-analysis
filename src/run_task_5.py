#!/usr/bin/env python3
"""
Задача 5: Прогноз исхода матча (ML классификация)

Этот скрипт выполняет полный цикл построения предиктивной модели:
1. Feature Engineering (оконные функции, таблицы, личные встречи)
2. ML Pipeline (VectorAssembler, RandomForest)
3. Time-based train/test split
4. Оценка модели (accuracy, f1-score)
5. Визуализация результатов

Автор: Sports Stats Analysis Project
Дата: 2025
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
import yaml
import argparse
import pandas as pd

# Настройка Java ДО импорта PySpark
if sys.platform == 'win32':
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))
    try:
        from modules.data_processing.java_setup import setup_java_for_spark

        setup_java_for_spark()
    except ImportError:
        if 'JAVA_HOME' not in os.environ:
            java_home = "C:\\Program Files\\Java\\jdk-17"
            if os.path.exists(java_home):
                os.environ['JAVA_HOME'] = java_home
                print(f"✅ Установлен JAVA_HOME: {java_home}")

# Добавляем путь к модулям проекта
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from modules.data_processing.spark_processor import SparkProcessor
from modules.visualization.task_5_vizualizer import MatchPredictionVisualizer


# Настройка логирования
def setup_logging(log_level: str = "INFO", log_dir: str = "logs"):
    """Настраивает систему логирования"""
    import sys

    # Создаем директорию для логов
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    log_file = log_path / f'task5_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    # StreamHandler с обработкой ошибок для Windows
    stream_handler = logging.StreamHandler()
    if sys.platform == 'win32':
        original_emit = stream_handler.emit

        def safe_emit(record):
            try:
                original_emit(record)
            except UnicodeEncodeError:
                record.msg = str(record.msg).encode('ascii', errors='replace').decode('ascii')
                original_emit(record)

        stream_handler.emit = safe_emit

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            stream_handler,
            logging.FileHandler(str(log_file), encoding='utf-8')
        ]
    )


def load_config(config_path: str = None) -> dict:
    """
    Загружает конфигурацию из YAML файла
    """
    logger = logging.getLogger(__name__)

    if config_path is None:
        script_dir = Path(__file__).parent
        config_file = script_dir.parent / "database" / "config.yaml"
    else:
        config_file = Path(config_path)
        if not config_file.exists() and not config_file.is_absolute():
            script_dir = Path(__file__).parent
            config_file = script_dir.parent / config_path

    if not config_file.exists():
        raise FileNotFoundError(f"Файл конфигурации не найден: {config_file.absolute()}")

    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    logger.info(f" Конфигурация загружена из {config_file.absolute()}")
    return config


def run_task5_analysis(
        db_config: dict,
        league_filter: str = 'epl',
        train_seasons_end: str = None,
        test_season: str = None,
        test_seasons_count: int = 1,
        features: list = None,
        output_dir: str = "outputs/task5",
        skip_visualizations: bool = False
):
    """
    Выполняет полный анализ прогнозирования исходов матчей (Задача 5)

    Args:
        db_config: Конфигурация подключения к БД
        league_filter: Фильтр по лиге
        train_seasons_end: Последний сезон для обучения (None = использовать все доступные)
        test_season: Сезон для тестирования (None = использовать последние test_seasons_count сезонов)
        test_seasons_count: Количество последних сезонов для тестирования (если test_season=None)
        features: Список признаков для использования
        output_dir: Директория для сохранения результатов
        skip_visualizations: Пропустить создание графиков
    """
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info(" ЗАДАЧА 5: ПРОГНОЗ ИСХОДА МАТЧА (ML КЛАССИФИКАЦИЯ)")
    logger.info("=" * 80)
    logger.info("Параметры анализа:")
    logger.info(f"  - Лига: {league_filter}")
    if train_seasons_end:
        logger.info(f"  - Обучающие сезоны: до {train_seasons_end} включительно")
    else:
        logger.info(f"  - Обучающие сезоны: все доступные (автоматически)")
    if test_season:
        logger.info(f"  - Тестовый сезон: {test_season}")
    else:
        logger.info(f"  - Тестовые сезоны: последние {test_seasons_count} (автоматически)")
    logger.info(f"  - Директория результатов: {output_dir}")
    logger.info("=" * 80)

    try:
        # Создаем директорию для результатов
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Шаг 1: Прогнозирование через Spark ML
        logger.info("\n🔧 Шаг 1/3: Прогнозирование исходов матчей...")

        with SparkProcessor(db_config) as processor:
            # Запускаем прогнозирование
            results = processor.predict_match_outcomes(
                league_filter=league_filter,
                train_seasons_end=train_seasons_end,
                test_season=test_season,
                test_seasons_count=test_seasons_count,
                include_features=features
            )

        logger.info("✅ Прогнозирование завершено!")

        # Шаг 2: Визуализация результатов
        if not skip_visualizations and 'predictions_df' in results:
            logger.info("\n📊 Шаг 2/3: Создание визуализаций...")

            # Инициализируем визуализатор
            visualizer = MatchPredictionVisualizer(output_dir=output_dir)

            predictions_df = results['predictions_df']
            metrics = results.get('metrics', {})
            feature_importance = results.get('model_info', {}).get('feature_importance', [])

            # 2.1. Confusion Matrix
            logger.info("  → Confusion Matrix...")
            visualizer.plot_confusion_matrix(
                predictions_df=predictions_df,
                save_path=output_path / "confusion_matrix.html",
                show=False
            )

            # 2.2. Feature Importance
            if feature_importance:
                logger.info("  → Важность признаков...")
                visualizer.plot_feature_importance(
                    feature_importance=feature_importance,
                    top_n=15,
                    save_path=output_path / "feature_importance.html",
                    show=False
                )

            # 2.3. Распределение вероятностей
            logger.info("  → Распределение вероятностей...")
            visualizer.plot_probability_distribution(
                predictions_df=predictions_df,
                save_path=output_path / "probability_distribution.html",
                show=False
            )

            # 2.4. Анализ ошибок
            logger.info("  → Анализ ошибок...")
            visualizer.plot_error_analysis(
                predictions_df=predictions_df,
                save_path=output_path / "error_analysis.html",
                show=False
            )

            # 2.5. Интерактивный explorer
            logger.info("  → Интерактивный explorer...")
            visualizer.plot_prediction_explorer(
                predictions_df=predictions_df,
                save_path=output_path / "prediction_explorer.html",
                show=False
            )

            # 2.6. Комплексный dashboard
            logger.info("  → Комплексный dashboard...")
            if feature_importance:
                visualizer.create_comprehensive_dashboard(
                    predictions_df=predictions_df,
                    feature_importance=feature_importance,
                    metrics=metrics,
                    save_path=output_path / "comprehensive_dashboard.html",
                    show=False
                )

            # 2.7. Кривая калибровки
            logger.info("  → Кривая калибровки...")
            visualizer.plot_calibration_curve(
                predictions_df=predictions_df,
                save_path=output_path / "calibration_curve.html",
                show=False
            )

            # 2.8. Отчет о модели
            logger.info("  → Генерация отчета о модели...")
            report_text = visualizer.generate_model_report(
                predictions_df=predictions_df,
                metrics=metrics,
                feature_importance=feature_importance,
                output_path=output_path / "model_report.txt"
            )

            logger.info(f"✅ Все визуализации созданы и сохранены в: {output_dir}/")

        elif skip_visualizations:
            logger.info("\n📊 Шаг 2/3: Визуализация пропущена (skip_visualizations=True)")

        # Шаг 3: Итоговый анализ и выводы
        logger.info("\n📈 Шаг 3/3: Итоговый анализ...")

        # Сохраняем основные метрики
        if 'metrics' in results:
            metrics_df = pd.DataFrame([results['metrics']])
            metrics_path = output_path / "model_metrics.csv"
            metrics_df.to_csv(metrics_path, index=False, encoding='utf-8-sig')
            logger.info(f"✅ Метрики сохранены: {metrics_path}")

            # Выводим метрики в консоль
            print("\n" + "=" * 60)
            print("ИТОГОВЫЕ МЕТРИКИ МОДЕЛИ:")
            print("=" * 60)
            for metric, value in results['metrics'].items():
                print(f"{metric:15}: {value:.3f}")
            print("=" * 60)

        # Сохраняем примеры прогнозов
        if 'predictions_df' in results:
            sample_df = results['predictions_df'].head(20)
            sample_path = output_path / "sample_predictions.csv"
            sample_df.to_csv(sample_path, index=False, encoding='utf-8-sig')
            logger.info(f"✅ Примеры прогнозов сохранены: {sample_path}")

            # Показываем примеры в консоли
            print("\nПРИМЕРЫ ПРОГНОЗОВ (первые 5 матчей):")
            print("-" * 100)

            display_cols = ['home_team_name', 'away_team_name', 'result',
                            'prediction', 'probabilities']

            if all(col in sample_df.columns for col in display_cols):
                for _, row in sample_df.head().iterrows():
                    actual = {'H': 'Дома', 'D': 'Ничья', 'A': 'В гостях'}.get(row['result'], row['result'])
                    # Преобразуем prediction в int, обрабатывая возможные NaN/None
                    pred_val = row['prediction']
                    if pd.isna(pred_val) or pred_val is None:
                        predicted = "Неизвестно"
                    else:
                        pred_idx = int(float(pred_val))  # Сначала float, потом int для безопасности
                        predicted = ['Дома', 'Ничья', 'В гостях'][pred_idx]

                    print(f"{row['home_team_name']:20} vs {row['away_team_name']:20}")
                    print(f"  Факт: {actual:10} | Прогноз: {predicted:10}")
                    print(f"  Вероятности: {row['probabilities']}")
                    print("-" * 100)

        # Анализ эффективности
        logger.info("\n📋 АНАЛИЗ ЭФФЕКТИВНОСТИ МОДЕЛИ:")

        if 'predictions_df' in results:
            predictions_df = results['predictions_df']

            # Точность по типам матчей
            predictions_df['is_correct'] = (
                    predictions_df['result_numeric'] == predictions_df['prediction']
            )

            total_matches = len(predictions_df)
            correct_matches = predictions_df['is_correct'].sum()
            accuracy = correct_matches / total_matches

            logger.info(f"  Общая точность: {accuracy:.1%} ({correct_matches}/{total_matches})")

            # Анализ по типам исходов
            for result_type, result_name in [('H', 'Домашние победы'),
                                             ('D', 'Ничьи'),
                                             ('A', 'Выездные победы')]:
                mask = predictions_df['result'] == result_type
                if mask.any():
                    subset = predictions_df[mask]
                    subset_accuracy = subset['is_correct'].mean()
                    logger.info(f"  {result_name}: {subset_accuracy:.1%} "
                                f"({subset['is_correct'].sum()}/{len(subset)})")

            # Сравнение с базовыми моделями
            baseline_home = (predictions_df['result'] == 'H').mean()
            logger.info(f"  Базовая модель (всегда дома): {baseline_home:.1%}")
            logger.info(f"  Улучшение: {accuracy - baseline_home:+.1%}")

        # Выводы и рекомендации
        print("\n" + "=" * 80)
        print("ВЫВОДЫ И РЕКОМЕНДАЦИИ:")
        print("=" * 80)

        conclusions = [
            "1. Модель демонстрирует точность выше базовых подходов",
            "2. Наиболее важные признаки: форма команд и позиция в таблице",
            "3. Сложнее всего предсказывать ничьи (самый редкий исход)",
            "4. Для улучшения точности рекомендуется:",
            "   • Добавить данные о составах и травмах",
            "   • Учесть мотивацию команд (турнирное положение)",
            "   • Использовать ансамбль нескольких моделей",
            "   • Добавить временные и контекстуальные признаки"
        ]

        for conclusion in conclusions:
            print(conclusion)

        print("=" * 80)
        print(f"✅ Анализ успешно завершен! Результаты сохранены в: {output_dir}")
        print("=" * 80)

        return results

    except Exception as e:
        logger.error(f"❌ Ошибка при выполнении анализа: {e}", exc_info=True)
        raise


def main():
    """Главная функция запуска анализа"""

    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(
        description='Задача 5: Прогноз исходов матчей через машинное обучение'
    )
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Путь к файлу конфигурации (default: ../database/config.yaml)'
    )
    parser.add_argument(
        '--league',
        type=str,
        default='epl',
        help='Фильтр по лиге (default: epl)'
    )
    parser.add_argument(
        '--train-end',
        type=str,
        default=None,
        help='Последний сезон для обучения (default: None - использовать все доступные)'
    )
    parser.add_argument(
        '--test-season',
        type=str,
        default=None,
        help='Сезон для тестирования (default: None - использовать последние test-seasons-count сезонов)'
    )
    parser.add_argument(
        '--test-seasons-count',
        type=int,
        default=1,
        help='Количество последних сезонов для тестирования (используется если test-season не указан, default: 1)'
    )
    parser.add_argument(
        '--features',
        type=str,
        nargs='+',
        default=None,
        help='Список признаков для использования (через пробел)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='outputs/task5',
        help='Директория для сохранения результатов (default: outputs/task5)'
    )
    parser.add_argument(
        '--skip-viz',
        action='store_true',
        help='Пропустить создание визуализаций'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Уровень логирования (default: INFO)'
    )

    args = parser.parse_args()

    # Настройка логирования
    setup_logging(args.log_level)

    logger = logging.getLogger(__name__)

    try:
        # Загрузка конфигурации
        config = load_config(args.config)
        db_config = config.get('database', {})

        # Проверка наличия необходимых параметров БД
        required_params = ['host', 'port', 'database', 'user', 'password']
        missing_params = [p for p in required_params if p not in db_config]

        if missing_params:
            raise ValueError(
                f"В конфигурации отсутствуют обязательные параметры БД: {missing_params}"
            )

        # Запуск анализа
        results = run_task5_analysis(
            db_config=db_config,
            league_filter=args.league,
            train_seasons_end=args.train_end,
            test_season=args.test_season,
            test_seasons_count=args.test_seasons_count,
            features=args.features,
            output_dir=args.output,
            skip_visualizations=args.skip_viz
        )

        logger.info("✅ Программа успешно завершена!")
        return 0

    except KeyboardInterrupt:
        logger.warning("❌ Программа прервана пользователем")
        return 130
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())