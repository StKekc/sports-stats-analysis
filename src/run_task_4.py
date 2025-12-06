#!/usr/bin/env python3
"""
Задача 4: Определение «самого неудобного соперника»

Этот скрипт:
1. Читает данные матчей из PostgreSQL через Apache Spark
2. Использует низкоуровневый Spark RDD API для обработки данных:
   - flatMap: преобразование матчей в пары команд-результатов
   - reduceByKey: агрегация статистики по парам команд
   - groupByKey: группировка по командам
   - map: поиск самого неудобного соперника
3. Создает визуализации результатов
4. Сохраняет результаты в CSV и HTML

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

# Установка переменных окружения для Windows ДО импорта PySpark
# Добавляем путь к модулям проекта для импорта java_setup
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Настройка Java ДО импорта PySpark
if sys.platform == 'win32':
    try:
        from modules.data_processing.java_setup import setup_java_for_spark
        setup_java_for_spark()
    except ImportError:
        # Если модуль не найден, используем простую настройку
        print("⚠️  Модуль java_setup не найден, используем базовую настройку")
        if 'JAVA_HOME' not in os.environ:
            java_home = "C:\\Program Files\\Java\\jdk-17"
            if os.path.exists(java_home):
                os.environ['JAVA_HOME'] = java_home
                print(f"✅ Установлен JAVA_HOME: {java_home}")
            else:
                print("⚠️  Java не найдена. Установите Java JDK и установите переменную окружения JAVA_HOME")

# =====================================================================

# Теперь импортируем остальные модули

from modules.data_processing.spark_processor import SparkProcessor
from modules.visualization.task_4_visualizer import ToughestOpponentVisualizer


# Настройка логирования
def setup_logging(log_level: str = "INFO", log_dir: str = "logs"):
    """Настраивает систему логирования"""
    import sys

    # Создаем директорию для логов, если её нет
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    log_file = log_path / f'task4_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    # Создаем StreamHandler с обработкой ошибок кодировки для Windows
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


def run_task4_analysis(
        db_config: dict,
        league_filter: str = None,
        season_filter: str = None,
        min_matches: int = 5,
        top_teams: int = 100,
        output_dir: str = "outputs/task4",
        team_analysis: str = None,
        skip_visualizations: bool = False,
        check_data: bool = True
):
    """
    Выполняет полный анализ самых неудобных соперников (Задача 4)

    Args:
        db_config: Конфигурация подключения к БД
        league_filter: Фильтр по лиге
        season_filter: Фильтр по сезону
        min_matches: Минимальное количество матчей между командами
        top_teams: Количество топ команд для анализа
        output_dir: Директория для сохранения результатов
        team_analysis: Конкретная команда для детального анализа
        skip_visualizations: Пропустить создание графиков
        check_data: Проверить данные перед анализом
    """
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info(" ЗАДАЧА 4: ОПРЕДЕЛЕНИЕ «САМОГО НЕУДОБНОГО СОПЕРНИКА»")
    logger.info("=" * 80)
    logger.info("📊 Параметры анализа:")
    logger.info(f"  - Лига: {league_filter or 'все'}")
    logger.info(f"  - Сезон: {season_filter or 'все'}")
    logger.info(f"  - Минимальное матчей между командами: {min_matches}")
    logger.info(f"  - Топ команд для анализа: {top_teams}")
    logger.info(f"  - Детальный анализ команды: {team_analysis or 'нет'}")
    logger.info(f"  - Директория результатов: {output_dir}")
    logger.info("=" * 80)

    try:
        # Шаг 1: Проверка данных
        if check_data:
            logger.info("\n Шаг 1/4: Проверка доступности данных...")
            try:
                with SparkProcessor(db_config) as processor:
                    data_stats = processor.check_data_availability()

                    if data_stats.get('matches', {}).get('count', 0) == 0:
                        logger.error("❌ В таблице 'matches' нет данных!")
                        return None

                    logger.info("✅ Данные доступны и готовы к анализу")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось проверить данные: {e}")
                logger.warning("Продолжаем анализ без проверки...")

        # Шаг 2: Инициализация Spark и анализ данных через RDD API
        logger.info("\n Шаг 2/4: Обработка данных через Spark RDD API...")
        logger.info(" Используемые RDD операции:")
        logger.info("  • flatMap(): преобразование матчей в пары команд-результатов")
        logger.info("  • reduceByKey(): агрегация статистики по парам команд")
        logger.info("  • filter(): отбор пар с достаточным количеством матчей")
        logger.info("  • map(): расчет процента побед")
        logger.info("  • groupByKey(): группировка соперников по командам")
        logger.info("  • map(): поиск соперника с минимальным процентом побед")

        try:
            with SparkProcessor(db_config) as processor:
                # Выполнение RDD анализа
                results = processor.find_toughest_opponents(
                    league_filter=league_filter,
                    season_filter=season_filter,
                    min_matches=min_matches,
                    top_teams=top_teams
                )

            if results is None:
                logger.error("❌ Анализ не выполнен")
                return None

            results_df = results.get('toughest_opponents', pd.DataFrame())
            detailed_stats = results.get('detailed_pair_stats', pd.DataFrame())

            logger.info(f"✅ Обработка завершена. Проанализировано команд: {len(results_df)}")

            # Вывод результатов в консоль
            if len(results_df) > 0:
                logger.info("\n Топ-10 самых неудобных соперников:")
                print("\n" + "=" * 100)
                display_df = results_df.head(10)[['team_name', 'toughest_opponent_name',
                                                  'win_percentage', 'total_matches']].copy()
                display_df.columns = ['Команда', 'Самый неудобный соперник', '% побед', 'Матчей']
                print(display_df.to_string(index=False, formatters={'% побед': '{:.1f}%'.format}))
                print("=" * 100 + "\n")
            else:
                logger.warning("⚠️ Не найдено данных для отображения")

        except Exception as e:
            logger.error(f"❌ Ошибка при выполнении анализа RDD: {e}", exc_info=True)
            return None

        # Шаг 3: Сохранение результатов в CSV
        logger.info("\n Шаг 3/4: Сохранение результатов...")

        try:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            # Сохраняем основные результаты
            main_csv = output_path / f"task4_toughest_opponents.csv"
            if len(results_df) > 0:
                results_df.to_csv(main_csv, index=False, encoding='utf-8')
                logger.info(f"✅ Основные результаты: {main_csv}")
            else:
                logger.warning("⚠️ Нет данных для сохранения в CSV")

            # Сохраняем подробную статистику
            if len(detailed_stats) > 0:
                detail_csv = output_path / f"task4_detailed_pair_stats.csv"
                detailed_stats.to_csv(detail_csv, index=False, encoding='utf-8')
                logger.info(f"✅ Подробная статистика: {detail_csv}")

        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении результатов: {e}")

        # Шаг 4: Визуализация
        if not skip_visualizations and len(results_df) > 0:
            logger.info("\n Шаг 4/4: Создание визуализаций...")

            try:
                visualizer = ToughestOpponentVisualizer(output_dir=output_dir)

                # График 1: Столбчатая диаграмма
                logger.info("  -> Столбчатая диаграмма самых неудобных соперников...")
                visualizer.plot_toughest_opponents_bar(results_df, top_n=15, show=False)

                # График 2: Распределение процентов побед
                logger.info("  -> Гистограмма распределения процентов побед...")
                visualizer.plot_win_percentage_distribution(results_df, show=False)

                # График 3: Network граф
                logger.info("  -> Network граф взаимоотношений команд...")
                visualizer.create_team_network_graph(results_df, top_n=20, show=False)

                # График 4: Комплексный dashboard
                logger.info("  -> Комплексный dashboard...")
                visualizer.create_comprehensive_dashboard(results_df, detailed_stats, show=False)

                # График 5: Сводный отчет
                logger.info("  -> Генерация сводного отчета...")
                visualizer.generate_summary_report(results_df)

                logger.info(f"✅ Все визуализации созданы и сохранены в: {output_dir}/")

            except Exception as e:
                logger.error(f"❌ Ошибка при создании визуализаций: {e}")
        elif skip_visualizations:
            logger.info("\n Шаг 4/4: Визуализация пропущена (skip_visualizations=True)")
        else:
            logger.info("\n Шаг 4/4: Пропуск визуализации (нет данных)")

        # Шаг 5: Детальный анализ конкретной команды (если указана)
        if team_analysis and not skip_visualizations and len(results_df) > 0:
            logger.info(f"\n Дополнительный шаг: Детальный анализ команды '{team_analysis}'...")

            try:
                with SparkProcessor(db_config) as processor:
                    team_stats = processor.analyze_all_opponents_for_team(
                        team_name=team_analysis,
                        league_filter=league_filter,
                        season_filter=season_filter
                    )

                if len(team_stats) > 0:
                    # Сохраняем CSV с детальной статистикой команды
                    team_csv = output_path / f"team_analysis_{team_analysis.replace(' ', '_').lower()}.csv"
                    team_stats.to_csv(team_csv, index=False, encoding='utf-8')
                    logger.info(f"✅ Детальная статистика команды: {team_csv}")

                    # Создаем график
                    visualizer.plot_team_detailed_analysis(team_stats, team_analysis, show=False)

                    logger.info(f"✅ Детальный анализ команды '{team_analysis}' завершен")
                else:
                    logger.warning(f"⚠️ Не удалось найти данные для команды '{team_analysis}'")

            except Exception as e:
                logger.error(f"❌ Ошибка при детальном анализе команды: {e}")

        # Итоговая статистика
        logger.info("\n" + "=" * 80)
        logger.info(" АНАЛИЗ ЗАВЕРШЕН!")
        logger.info("=" * 80)

        if len(results_df) > 0:
            logger.info(" Созданные файлы:")
            if Path(output_dir, "task4_toughest_opponents.csv").exists():
                logger.info(f"   • Основные результаты: {output_dir}/task4_toughest_opponents.csv")
            if Path(output_dir, "task4_detailed_pair_stats.csv").exists():
                logger.info(f"   • Подробная статистика: {output_dir}/task4_detailed_pair_stats.csv")
            if not skip_visualizations:
                html_files = list(Path(output_dir).glob("*.html"))
                if html_files:
                    logger.info(f"   • Графики и отчеты: {len(html_files)} HTML файлов")
                if Path(output_dir, "analysis_report.txt").exists():
                    logger.info(f"   • Текстовый отчет: {output_dir}/analysis_report.txt")
            if team_analysis and Path(output_dir,
                                      f"team_analysis_{team_analysis.replace(' ', '_').lower()}.csv").exists():
                logger.info(f"   • Анализ команды {team_analysis}: соответствующий CSV файл")

        # Основные выводы
        if len(results_df) > 0:
            logger.info("\n ОСНОВНЫЕ ВЫВОДЫ:")
            # Самый сложный случай
            toughest = results_df.iloc[0]
            logger.info(f"    Самый неудобный соперник:")
            logger.info(f"      {toughest['team_name']} vs {toughest['toughest_opponent_name']}")
            logger.info(
                f"      → Всего {toughest['total_matches']} матчей, только {toughest['win_percentage']:.1f}% побед")

            # Статистика распределения
            low_percentage = len(results_df[results_df['win_percentage'] < 25])
            medium_percentage = len(results_df[(results_df['win_percentage'] >= 25) &
                                               (results_df['win_percentage'] < 50)])
            high_percentage = len(results_df[results_df['win_percentage'] >= 50])

            logger.info(f"    Распределение команд по проценту побед:")
            logger.info(f"      • < 25% побед: {low_percentage} команд")
            logger.info(f"      • 25-50% побед: {medium_percentage} команд")
            logger.info(f"      • > 50% побед: {high_percentage} команд")
        else:
            logger.info("\n ⚠️  Нет данных для выводов")

        logger.info("=" * 80 + "\n")

        return results

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при выполнении анализа: {e}", exc_info=True)
        return None


def main():
    """Главная функция запуска анализа"""

    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(
        description='Задача 4: Определение самых неудобных соперников через Spark RDD API'
    )
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
        default=5,
        help='Минимальное количество матчей между командами (default: 5)'
    )
    parser.add_argument(
        '--top-teams',
        type=int,
        default=0,
        help='Количество топ команд для анализа (0 = все команды, default: 0)'
    )
    parser.add_argument(
        '--team',
        type=str,
        default=None,
        help='Конкретная команда для детального анализа'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='outputs/task4',
        help='Директория для сохранения результатов (default: outputs/task4)'
    )
    parser.add_argument(
        '--skip-viz',
        action='store_true',
        help='Пропустить создание визуализаций'
    )
    parser.add_argument(
        '--skip-check',
        action='store_true',
        help='Пропустить предварительную проверку данных'
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
        results = run_task4_analysis(
            db_config=db_config,
            league_filter=args.league,
            season_filter=args.season,
            min_matches=args.min_matches,
            top_teams=args.top_teams,
            output_dir=args.output,
            team_analysis=args.team,
            skip_visualizations=args.skip_viz,
            check_data=not args.skip_check
        )

        logger.info("✅ Программа успешно завершена!")
        return 0

    except KeyboardInterrupt:
        logger.warning("\n ⚠️  Программа прервана пользователем")
        return 130
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())