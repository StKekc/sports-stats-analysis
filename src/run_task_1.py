#!/usr/bin/env python3
"""
Задача 1: Анализ игровых стилей команд

Этот скрипт:
1. Читает данные из PostgreSQL через Apache Spark
2. Создает метрики игровых стилей команд
3. Выполняет кластеризацию K-means для определения стилей
4. Сохраняет результаты анализа
5. Создает интерактивные визуализации стилей

Технологии:
- Apache Spark SQL для подготовки данных
- Scikit-learn для кластеризации K-means
- Plotly для интерактивных визуализаций
- Pandas для анализа результатов

Автор: Sports Stats Analysis Project
Дата: 2025
"""

import sys
import os
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Optional
import yaml
import pandas as pd

# Настройка Java ДО импорта PySpark
if sys.platform == 'win32':
    # Добавляем путь к модулям проекта для импорта java_setup
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))
    try:
        from modules.data_processing.java_setup import setup_java_for_spark
        setup_java_for_spark()
    except ImportError:
        # Если модуль не найден, используем простую настройку
        if 'JAVA_HOME' not in os.environ:
            java_home = "C:\\Program Files\\Java\\jdk-17"
            if os.path.exists(java_home):
                os.environ['JAVA_HOME'] = java_home
                print(f"✅ Установлен JAVA_HOME: {java_home}")

# Добавляем путь к модулям проекта
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from modules.data_processing.spark_processor import SparkProcessor
from modules.visualization.team_styles_visualizer import TeamStylesVisualizer


# ============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================================

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Настраивает систему логирования

    Args:
        log_level: Уровень логирования

    Returns:
        Logger instance
    """
    import sys
    
    # Создаём директорию для логов
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)

    # Формат логов
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Создаем StreamHandler с обработкой ошибок кодировки для Windows
    stream_handler = logging.StreamHandler()
    # На Windows устанавливаем обработку ошибок кодировки
    if sys.platform == 'win32':
        # Переопределяем метод emit для обработки Unicode ошибок
        original_emit = stream_handler.emit
        def safe_emit(record):
            try:
                original_emit(record)
            except UnicodeEncodeError:
                # Если не удается закодировать, заменяем проблемные символы
                record.msg = str(record.msg).encode('ascii', errors='replace').decode('ascii')
                original_emit(record)
        stream_handler.emit = safe_emit

    # Настройка
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            stream_handler,
            logging.FileHandler(
                logs_dir / f'task1_styles_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
                encoding='utf-8'
            )
        ]
    )

    return logging.getLogger(__name__)


# ============================================================================
# ЗАГРУЗКА КОНФИГУРАЦИИ
# ============================================================================

def load_config(config_path: str = None) -> dict:
    """
    Загружает конфигурацию из YAML файла

    Args:
        config_path: Путь к файлу конфигурации (если None, используется путь относительно скрипта)

    Returns:
        dict: Конфигурация
    """
    # Если путь не указан, используем путь относительно расположения скрипта
    if config_path is None:
        script_dir = Path(__file__).parent
        config_file = script_dir.parent / "database" / "config.yaml"
    else:
        config_file = Path(config_path)
        # Если путь относительный и файл не найден, пробуем относительно скрипта
        if not config_file.exists() and not config_file.is_absolute():
            script_dir = Path(__file__).parent
            config_file = script_dir.parent / config_path

    if not config_file.exists():
        raise FileNotFoundError(f"Файл конфигурации не найден: {config_file.absolute()}")

    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


# ============================================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================================

def run_task1_analysis(
        db_config: dict,
        league_filter: Optional[str] = None,
        season_filter: Optional[str] = None,
        min_matches: int = 10,
        n_clusters: Optional[int] = None,
        team_name: Optional[str] = None,
        output_dir: str = "outputs/team_styles",
        skip_analysis: bool = False,
        skip_visualizations: bool = False,
        analysis_data_path: Optional[str] = None
) -> dict:
    """
    Выполняет полный анализ игровых стилей команд (Задача 1)

    Pipeline:
    1. Подготовка данных через Spark SQL
    2. Кластеризация команд по стилям игры
    3. Анализ и сохранение результатов
    4. Визуализация результатов

    Args:
        db_config: Конфигурация подключения к БД
        league_filter: Фильтр по лиге (например, 'epl')
        season_filter: Фильтр по сезону (например, '2023-2024')
        min_matches: Минимальное количество матчей для включения команды
        n_clusters: Количество кластеров (если None, определяется автоматически)
        team_name: Название команды для детального анализа
        output_dir: Директория для визуализаций
        skip_analysis: Пропустить анализ (использовать существующие данные)
        skip_visualizations: Пропустить создание визуализаций
        analysis_data_path: Путь к существующим данным анализа

    Returns:
        dict: Результаты выполнения
    """
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("🎯 ЗАДАЧА 1: АНАЛИЗ ИГРОВЫХ СТИЛЕЙ КОМАНД")
    logger.info("=" * 80)
    logger.info("📋 Параметры анализа:")
    logger.info(f"   • Лига: {league_filter or 'все'}")
    logger.info(f"   • Сезон: {season_filter or 'все'}")
    logger.info(f"   • Минимальное количество матчей: {min_matches}")
    logger.info(f"   • Количество кластеров: {n_clusters or 'автоопределение'}")
    logger.info(f"   • Директория визуализаций: {output_dir}")
    logger.info("=" * 80)

    results = {
        'status': 'success',
        'analysis_results': None,
        'visualizations': [],
        'analysis_data_path': None
    }

    # Путь для сохранения данных анализа
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not analysis_data_path:
        analysis_data_path = f"data/team_styles_analysis_{timestamp}"

    analysis_path = Path(analysis_data_path)

    try:
        # ====================================================================
        # ШАГ 1: АНАЛИЗ ИГРОВЫХ СТИЛЕЙ
        # ====================================================================

        if not skip_analysis:
            logger.info("\n🔍 Шаг 1/2: Анализ игровых стилей (K-means кластеризация)")
            logger.info("-" * 60)

            try:
                with SparkProcessor(db_config) as processor:
                    # Выполняем анализ стилей
                    analysis_results = processor.analyze_team_playing_styles(
                        league_filter=league_filter,
                        season_filter=season_filter,
                        min_matches=min_matches,
                        n_clusters=n_clusters
                    )

                    # Сохраняем результаты в атрибуте для последующего использования
                    processor.last_style_analysis = analysis_results

                results['analysis_results'] = analysis_results
                results['analysis_data_path'] = str(analysis_path)

                logger.info(f"[OK] Анализ завершён. Определено {analysis_results['n_clusters']} стилей игры")
                logger.info(f"   Проанализировано команд: {len(analysis_results['teams_with_styles'])}")
                logger.info(f"   Качество кластеризации (силуэт): {analysis_results['silhouette_score']:.3f}")

                # Выводим информацию о найденных стилях
                logger.info("\n🎭 Обнаруженные стили игры:")
                for _, row in analysis_results['cluster_analysis'].iterrows():
                    logger.info(f"   • {row['style_name']}: {row['team_count']} команд ({row['percentage']:.1f}%)")
            except Exception as e:
                error_msg = str(e)
                if "JAVA_GATEWAY_EXITED" in error_msg or "Java gateway" in error_msg:
                    logger.error(f"\n❌ Ошибка Java gateway при создании Spark сессии: {error_msg}")
                    logger.error("\n💡 Система уже пыталась использовать минимальную конфигурацию.")
                    logger.error("\n💡 Дополнительные решения:")
                    logger.error("   1. Убедитесь, что Java JDK 8, 11, 17 или 21 установлена")
                    logger.error("   2. Проверьте, что JAVA_HOME установлена правильно:")
                    if 'JAVA_HOME' in os.environ:
                        logger.error(f"      Текущая JAVA_HOME: {os.environ['JAVA_HOME']}")
                    else:
                        logger.error("      JAVA_HOME не установлена!")
                    logger.error("   3. Перезапустите терминал/IDE после установки Java")
                    logger.error("   4. Закройте другие приложения, использующие Java")
                    logger.error("   5. Проверьте, что порты не заняты другими процессами")
                    logger.error("   6. Попробуйте перезагрузить компьютер")
                else:
                    # Для других ошибок просто логируем
                    logger.error(f"\n❌ Ошибка при выполнении анализа: {error_msg}")
                raise

        else:
            logger.info("\n⏭️  Шаг 1/2: Анализ пропущен (skip_analysis=True)")

            if not analysis_path.exists():
                raise FileNotFoundError(
                    f"Данные анализа не найдены: {analysis_path}\n"
                    "Запустите без --skip-analysis для выполнения анализа."
                )

            # Загружаем существующие результаты анализа
            logger.info(f"   Загрузка данных анализа из: {analysis_path}")

            # Загружаем CSV файлы
            teams_path = analysis_path / "teams_with_playing_styles.csv"
            clusters_path = analysis_path / "cluster_analysis.csv"
            leagues_path = analysis_path / "league_distribution.csv"

            if not teams_path.exists():
                raise FileNotFoundError(f"Файл не найден: {teams_path}")

            teams_df = pd.read_csv(teams_path, encoding='utf-8-sig')
            cluster_analysis = pd.read_csv(clusters_path,
                                           encoding='utf-8-sig') if clusters_path.exists() else pd.DataFrame()
            league_distribution = pd.read_csv(leagues_path, encoding='utf-8-sig',
                                              index_col=0) if leagues_path.exists() else pd.DataFrame()

            # Загружаем изменения стилей если есть
            changes_path = analysis_path / "style_changes.csv"
            style_changes = pd.read_csv(changes_path, encoding='utf-8-sig') if changes_path.exists() else pd.DataFrame()

            # Определяем силуэтный коэффициент из отчета если есть
            report_path = analysis_path / "analysis_report.txt"
            silhouette_score = 0.0
            if report_path.exists():
                with open(report_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Парсим силуэтный коэффициент
                    import re
                    match = re.search(r'силуэт[:\s]+([\d.]+)', content, re.IGNORECASE)
                    if match:
                        silhouette_score = float(match.group(1))

            analysis_results = {
                'teams_with_styles': teams_df,
                'cluster_analysis': cluster_analysis,
                'league_distribution': league_distribution,
                'style_changes': style_changes,
                'n_clusters': len(cluster_analysis) if not cluster_analysis.empty else 0,
                'silhouette_score': silhouette_score
            }

            results['analysis_results'] = analysis_results
            results['analysis_data_path'] = str(analysis_path)

            logger.info(f"[OK] Данные загружены. Определено {analysis_results['n_clusters']} стилей игры")
            logger.info(f"   Команд: {len(teams_df)}")

        # ====================================================================
        # ШАГ 2: ВИЗУАЛИЗАЦИЯ РЕЗУЛЬТАТОВ
        # ====================================================================

        if not skip_visualizations:
            logger.info("\n📊 Шаг 2/2: Создание визуализаций (Plotly)")
            logger.info("-" * 60)

            # Создаём визуализатор
            visualizer = TeamStylesVisualizer(output_dir=output_dir)

            # Генерируем все визуализации
            viz_results = visualizer.generate_all_visualizations(
                analysis_results
            )

            results['visualizations'] = viz_results['visualizations']

            logger.info(f"[OK] Визуализации созданы: {len(results['visualizations'])} файлов")
            logger.info(f"   Директория: {viz_results['output_dir']}")

            # Детальный анализ конкретной команды если указана
            if team_name:
                logger.info(f"\n🔍 Детальный анализ команды: {team_name}")
                teams_df = analysis_results['teams_with_styles']

                if team_name in teams_df['team_name'].values:
                    team_row = teams_df[teams_df['team_name'] == team_name].iloc[0]
                    cluster_analysis = analysis_results['cluster_analysis']

                    visualizer.plot_team_style_analysis(
                        team_row,
                        cluster_analysis,
                        show=False
                    )

                    # Получаем рекомендации
                    with SparkProcessor(db_config) as processor:
                        if hasattr(processor, 'last_style_analysis'):
                            processor.last_style_analysis = analysis_results

                        recommendations = processor.get_team_style_recommendations(
                            team_name,
                            season_filter
                        )

                        if recommendations:
                            logger.info(f"   📝 Рекомендации для {team_name}:")
                            for i, rec in enumerate(recommendations.get('recommendations', []), 1):
                                logger.info(f"     {i}. {rec}")
                else:
                    logger.warning(f"   Команда '{team_name}' не найдена в результатах анализа")

        else:
            logger.info("\n⏭️  Шаг 2/2: Визуализация пропущена (skip_visualizations=True)")

        # ====================================================================
        # ШАГ 3: ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ
        # ====================================================================

        logger.info("\n📈 Дополнительный анализ результатов:")

        teams_df = analysis_results['teams_with_styles']
        cluster_analysis = analysis_results['cluster_analysis']

        if not teams_df.empty:
            # 1. Самые атакующие команды
            top_attacking = teams_df.nlargest(5, 'attacking_power')
            logger.info("\n⚡ Самые атакующие команды:")
            for _, row in top_attacking.iterrows():
                logger.info(f"   • {row['team_name']}: {row['attacking_power']:.2f} гол/90 мин "
                            f"({row.get('playing_style', 'Не определен')})")

            # 2. Самые контролирующие команды
            top_possession = teams_df.nlargest(5, 'possession_control')
            logger.info("\n🎯 Самые контролирующие команды:")
            for _, row in top_possession.iterrows():
                logger.info(f"   • {row['team_name']}: {row['possession_control']:.1f}% владения "
                            f"({row.get('playing_style', 'Не определен')})")

            # 3. Самые эффективные команды
            top_efficient = teams_df.nlargest(5, 'attack_efficiency')
            logger.info("\n🎯 Самые эффективные команды:")
            for _, row in top_efficient.iterrows():
                logger.info(f"   • {row['team_name']}: {row['attack_efficiency']:.2f} эффективность "
                            f"({row.get('playing_style', 'Не определен')})")

            # 4. Распределение по лигам
            if 'league_name' in teams_df.columns:
                league_counts = teams_df['league_name'].value_counts()
                logger.info("\n🌍 Распределение по лигам:")
                for league, count in league_counts.head(5).items():
                    logger.info(f"   • {league}: {count} команд")

        # ====================================================================
        # ИТОГИ
        # ====================================================================

        logger.info("\n" + "=" * 80)
        logger.info("[OK] АНАЛИЗ УСПЕШНО ЗАВЕРШЁН!")
        logger.info("=" * 80)

        logger.info("\n📁 Созданные файлы:")
        logger.info(f"   Данные анализа: {results['analysis_data_path']}")

        if results['visualizations']:
            logger.info(f"   📊 Визуализации ({output_dir}/):")
            for viz in results['visualizations']:
                logger.info(f"      • {viz}")

        # Основные выводы
        if analysis_results['cluster_analysis'] is not None and not analysis_results['cluster_analysis'].empty:
            logger.info("\n🎭 ОСНОВНЫЕ ВЫВОДЫ:")

            # Самый частый стиль
            common_style = analysis_results['cluster_analysis'].loc[
                analysis_results['cluster_analysis']['team_count'].idxmax()
            ]
            logger.info(f"   📊 Самый частый стиль: {common_style['style_name']} "
                        f"({common_style['team_count']} команд, {common_style['percentage']:.1f}%)")

            # Самый редкий стиль
            rare_style = analysis_results['cluster_analysis'].loc[
                analysis_results['cluster_analysis']['team_count'].idxmin()
            ]
            logger.info(f"   🎯 Самый редкий стиль: {rare_style['style_name']} "
                        f"({rare_style['team_count']} команд, {rare_style['percentage']:.1f}%)")

            # Лучшие атакующие команды по стилям
            logger.info(f"\n🏆 Лучшие команды по каждому стилю:")
            for _, row in analysis_results['cluster_analysis'].iterrows():
                style_teams = teams_df[teams_df['playing_style'] == row['style_name']]
                if not style_teams.empty:
                    best_team = style_teams.nlargest(1, 'attacking_power').iloc[0]
                    logger.info(f"   • {row['style_name']}: {best_team['team_name']} "
                                f"({best_team['attacking_power']:.2f} гол/90 мин)")

        # Качество кластеризации
        logger.info(f"\n📊 Качество кластеризации:")
        logger.info(f"   • Силуэтный коэффициент: {analysis_results['silhouette_score']:.3f}")
        if analysis_results['silhouette_score'] > 0.5:
            logger.info("     ✓ Отличное разделение кластеров")
        elif analysis_results['silhouette_score'] > 0.25:
            logger.info("     ✓ Умеренное разделение кластеров")
        else:
            logger.info("     ⚠️  Слабое разделение кластеров")

        logger.info("\n" + "=" * 80)

        return results

    except Exception as e:
        logger.error(f"[ERROR] Ошибка при выполнении анализа: {e}", exc_info=True)
        results['status'] = 'error'
        results['error'] = str(e)
        raise


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Главная функция запуска"""

    parser = argparse.ArgumentParser(
        description='Задача 1: Анализ игровых стилей команд (K-means кластеризация)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Анализ всех команд
  python run_task1_team_styles.py

  # Анализ EPL за сезон 2023-2024
  python run_task1_team_styles.py --league epl --season 2023-2024

  # Анализ с конкретным количеством кластеров
  python run_task1_team_styles.py --league epl --clusters 5

  # Анализ конкретной команды
  python run_task1_team_styles.py --team "Manchester City"

  # Только визуализация (без анализа)
  python run_task1_team_styles.py --skip-analysis --analysis-data "data/team_styles_analysis_20250101_120000"

  # Только анализ (без визуализации)
  python run_task1_team_styles.py --skip-viz
        """
    )

    # Аргументы фильтрации
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Путь к файлу конфигурации (default: ../database/config.yaml относительно скрипта)'
    )
    parser.add_argument(
        '--league',
        type=str,
        default=None,
        help='Фильтр по лиге (например: epl, laliga, bundesliga)'
    )
    parser.add_argument(
        '--season',
        type=str,
        default=None,
        help='Фильтр по сезону (например: 2023-2024)'
    )
    parser.add_argument(
        '--min-matches',
        type=int,
        default=10,
        help='Минимальное количество матчей (default: 10)'
    )
    parser.add_argument(
        '--clusters',
        type=int,
        default=None,
        help='Количество кластеров (если не указано, определяется автоматически)'
    )
    parser.add_argument(
        '--team',
        type=str,
        default=None,
        help='Название команды для детального анализа'
    )

    # Аргументы путей
    parser.add_argument(
        '--output',
        type=str,
        default='outputs/team_styles',
        help='Директория для визуализаций (default: outputs/team_styles)'
    )
    parser.add_argument(
        '--analysis-data',
        type=str,
        default=None,
        help='Путь к существующим данным анализа (для --skip-analysis)'
    )

    # Флаги пропуска этапов
    parser.add_argument(
        '--skip-analysis',
        action='store_true',
        help='Пропустить анализ, использовать существующие данные'
    )
    parser.add_argument(
        '--skip-viz',
        action='store_true',
        help='Пропустить создание визуализаций'
    )

    # Логирование
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Уровень логирования (default: INFO)'
    )

    args = parser.parse_args()

    # Настройка логирования
    logger = setup_logging(args.log_level)

    try:
        # Загрузка конфигурации
        logger.info(f"Загрузка конфигурации: {args.config}")
        config = load_config(args.config)
        db_config = config.get('database', {})

        # Проверка параметров БД
        required_params = ['host', 'port', 'database', 'user', 'password']
        missing = [p for p in required_params if p not in db_config]

        if missing:
            raise ValueError(f"Отсутствуют параметры БД: {missing}")

        # Запуск анализа
        results = run_task1_analysis(
            db_config=db_config,
            league_filter=args.league,
            season_filter=args.season,
            min_matches=args.min_matches,
            n_clusters=args.clusters,
            team_name=args.team,
            output_dir=args.output,
            skip_analysis=args.skip_analysis,
            skip_visualizations=args.skip_viz,
            analysis_data_path=args.analysis_data
        )

        logger.info("🎉 Программа успешно завершена!")
        return 0

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Программа прервана пользователем")
        return 130
    except Exception as e:
        logger.error(f"[ERROR] Критическая ошибка: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())