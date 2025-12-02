#!/usr/bin/env python3
"""
Задача 2: Процент побед (дома/в гостях)

Этот скрипт:
1. Читает данные из PostgreSQL через Apache Spark
2. Выполняет Spark SQL запрос для расчета процента побед команд дома и в гостях
3. Создает визуализации (графики и дашборды)
4. Сохраняет результаты в CSV и изображения

Автор: Sports Stats Analysis Project
Дата: 2025
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
import yaml
import argparse

# Добавляем путь к модулям проекта
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from modules.data_processing.spark_processor import SparkProcessor
from modules.visualization.win_rate_visualizer import WinRateVisualizer


# Настройка логирования
def setup_logging(log_level: str = "INFO"):
    """Настраивает систему логирования"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                f'logs/task2_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
            )
        ]
    )


def load_config(config_path: str = "database/config.yaml") -> dict:
    """
    Загружает конфигурацию из YAML файла
    
    Args:
        config_path: Путь к файлу конфигурации
    
    Returns:
        dict: Конфигурация
    """
    logger = logging.getLogger(__name__)
    
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Файл конфигурации не найден: {config_path}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"✅ Конфигурация загружена из {config_path}")
    return config


def run_task2_analysis(
    db_config: dict,
    league_filter: str = None,
    season_filter: str = None,
    top_n: int = 10,
    output_dir: str = "outputs",
    skip_visualizations: bool = False
):
    """
    Выполняет полный анализ процента побед дома/в гостях (Задача 2)
    
    Args:
        db_config: Конфигурация подключения к БД
        league_filter: Фильтр по лиге (например, 'epl')
        season_filter: Фильтр по сезону (например, '2024-2025')
        top_n: Количество топ команд
        output_dir: Директория для сохранения результатов
        skip_visualizations: Пропустить создание графиков
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("🏆 ЗАДАЧА 2: ПРОЦЕНТ ПОБЕД (ДОМА/В ГОСТЯХ)")
    logger.info("=" * 80)
    logger.info(f"Параметры анализа:")
    logger.info(f"  - Лига: {league_filter or 'все'}")
    logger.info(f"  - Сезон: {season_filter or 'все'}")
    logger.info(f"  - Топ команд: {top_n}")
    logger.info(f"  - Директория результатов: {output_dir}")
    logger.info("=" * 80)
    
    try:
        # Шаг 1: Инициализация Spark и обработка данных
        logger.info("\n📊 Шаг 1/3: Обработка данных через Apache Spark SQL...")
        
        with SparkProcessor(db_config) as processor:
            # Выполнение Spark SQL запроса
            results_df = processor.calculate_home_away_win_rate(
                league_filter=league_filter,
                season_filter=season_filter,
                top_n=top_n
            )
        
        logger.info(f"✅ Обработка завершена. Получено записей: {len(results_df)}")
        
        # Шаг 2: Сохранение результатов в CSV
        logger.info("\n💾 Шаг 2/3: Сохранение результатов...")
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        csv_file = output_path / f"task2_home_away_win_rate_top{top_n}.csv"
        results_df.to_csv(csv_file, index=False, encoding='utf-8')
        logger.info(f"✅ Результаты сохранены в CSV: {csv_file}")
        
        # Вывод результатов в консоль
        logger.info("\n📋 Топ команды по проценту побед:")
        print("\n" + "=" * 100)
        print(results_df.to_string(index=False))
        print("=" * 100 + "\n")
        
        # Шаг 3: Визуализация
        if not skip_visualizations:
            logger.info("\n📈 Шаг 3/3: Создание визуализаций...")
            
            visualizer = WinRateVisualizer(output_dir=output_dir)
            
            # График 1: Столбчатая диаграмма
            logger.info("  → Создание столбчатой диаграммы...")
            visualizer.plot_home_away_comparison_bar(results_df)
            
            # График 2: Разница между дома и в гостях
            logger.info("  → Создание графика разницы...")
            visualizer.plot_home_away_difference(results_df)
            
            # График 3: Интерактивный Plotly график
            logger.info("  → Создание интерактивного графика...")
            visualizer.plot_interactive_plotly(results_df)
            
            # График 4: Комплексный dashboard
            logger.info("  → Создание комплексного dashboard...")
            visualizer.plot_comprehensive_dashboard(results_df)
            
            # График 5: Таблица результатов
            logger.info("  → Создание таблицы результатов...")
            visualizer.generate_summary_table(results_df)
            
            logger.info(f"✅ Все визуализации созданы и сохранены в: {output_dir}/")
        else:
            logger.info("\n⏭️  Шаг 3/3: Визуализация пропущена (skip_visualizations=True)")
        
        # Итоговая статистика
        logger.info("\n" + "=" * 80)
        logger.info("✅ АНАЛИЗ УСПЕШНО ЗАВЕРШЕН!")
        logger.info("=" * 80)
        logger.info("📁 Созданные файлы:")
        logger.info(f"   • CSV: {csv_file}")
        if not skip_visualizations:
            logger.info(f"   • Графики: {output_path}/*.png")
            logger.info(f"   • Интерактивные: {output_path}/*.html")
        logger.info("=" * 80)
        
        # Основные выводы
        logger.info("\n📊 ОСНОВНЫЕ ВЫВОДЫ:")
        if len(results_df) > 0:
            best_home = results_df.iloc[0]
            logger.info(f"   🏠 Лучший результат дома: {best_home['team_name']} ({best_home['home_win_pct']:.1f}%)")
            
            best_away_idx = results_df['away_win_pct'].idxmax()
            best_away = results_df.loc[best_away_idx]
            logger.info(f"   ✈️  Лучший результат в гостях: {best_away['team_name']} ({best_away['away_win_pct']:.1f}%)")
            
            results_df['diff'] = results_df['home_win_pct'] - results_df['away_win_pct']
            biggest_diff_idx = results_df['diff'].abs().idxmax()
            biggest_diff = results_df.loc[biggest_diff_idx]
            logger.info(f"   📈 Наибольшая разница дома/гости: {biggest_diff['team_name']} ({biggest_diff['diff']:.1f}%)")
        
        logger.info("=" * 80 + "\n")
        
        return results_df
        
    except Exception as e:
        logger.error(f"❌ Ошибка при выполнении анализа: {e}", exc_info=True)
        raise


def main():
    """Главная функция запуска анализа"""
    
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(
        description='Задача 2: Анализ процента побед команд дома и в гостях через Spark SQL'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='database/config.yaml',
        help='Путь к файлу конфигурации (default: database/config.yaml)'
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
        help='Фильтр по сезону (например: 2024-2025)'
    )
    parser.add_argument(
        '--top',
        type=int,
        default=10,
        help='Количество топ команд для анализа (default: 10)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='outputs',
        help='Директория для сохранения результатов (default: outputs)'
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
    Path('logs').mkdir(exist_ok=True)
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
        results = run_task2_analysis(
            db_config=db_config,
            league_filter=args.league,
            season_filter=args.season,
            top_n=args.top,
            output_dir=args.output,
            skip_visualizations=args.skip_viz
        )
        
        logger.info("🎉 Программа успешно завершена!")
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Программа прервана пользователем")
        return 130
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())

