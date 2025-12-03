#!/usr/bin/env python3
"""
Задача 3: Динамика результатов по сезонам/месяцам

Этот скрипт:
1. Читает данные из PostgreSQL через Apache Spark
2. Рассчитывает кумулятивные метрики с помощью оконных функций (Window Functions)
3. Сохраняет результаты в Parquet (ETL)
4. Создает интерактивные визуализации (Plotly)

Технологии:
- Apache Spark SQL + Window Functions
- Parquet для хранения агрегированных данных
- Plotly для интерактивных линейных графиков

Автор: Sports Stats Analysis Project
Дата: 2025
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Optional
import yaml

# Добавляем путь к модулям проекта
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from modules.data_processing.spark_processor import SparkProcessor
from modules.visualization.team_dynamics_visualizer import TeamDynamicsVisualizer


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
    # Создаём директорию для логов
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    # Формат логов
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Настройка
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                logs_dir / f'task3_dynamics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
            )
        ]
    )
    
    return logging.getLogger(__name__)


# ============================================================================
# ЗАГРУЗКА КОНФИГУРАЦИИ
# ============================================================================

def load_config(config_path: str = "database/config.yaml") -> dict:
    """
    Загружает конфигурацию из YAML файла
    
    Args:
        config_path: Путь к файлу конфигурации
    
    Returns:
        dict: Конфигурация
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Файл конфигурации не найден: {config_path}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config


# ============================================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================================

def run_task3_analysis(
    db_config: dict,
    league_filter: Optional[str] = None,
    season_filter: Optional[str] = None,
    team_names: Optional[List[str]] = None,
    output_dir: str = "outputs/task3",
    data_dir: str = "data/processed/task3",
    skip_etl: bool = False,
    skip_visualizations: bool = False
) -> dict:
    """
    Выполняет полный анализ динамики результатов команд (Задача 3)
    
    Pipeline:
    1. ETL: Spark SQL + Window Functions → Parquet
    2. Визуализация: Parquet → Plotly HTML
    
    Args:
        db_config: Конфигурация подключения к БД
        league_filter: Фильтр по лиге (например, 'epl')
        season_filter: Фильтр по сезону (например, '2023-2024')
        team_names: Список команд для анализа
        output_dir: Директория для визуализаций
        data_dir: Директория для Parquet данных
        skip_etl: Пропустить ETL (использовать существующий Parquet)
        skip_visualizations: Пропустить создание визуализаций
    
    Returns:
        dict: Результаты выполнения
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("🏆 ЗАДАЧА 3: ДИНАМИКА РЕЗУЛЬТАТОВ ПО СЕЗОНАМ")
    logger.info("=" * 80)
    logger.info("📋 Параметры анализа:")
    logger.info(f"   • Лига: {league_filter or 'все'}")
    logger.info(f"   • Сезон: {season_filter or 'все'}")
    logger.info(f"   • Команды: {', '.join(team_names) if team_names else 'топ по очкам'}")
    logger.info(f"   • Директория данных: {data_dir}")
    logger.info(f"   • Директория визуализаций: {output_dir}")
    logger.info("=" * 80)
    
    results = {
        'status': 'success',
        'etl_records': 0,
        'visualizations': [],
        'parquet_path': None
    }
    
    # Пути
    parquet_path = Path(data_dir) / "team_dynamics.parquet"
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    try:
        # ====================================================================
        # ШАГ 1: ETL (Extract, Transform, Load)
        # ====================================================================
        
        if not skip_etl:
            logger.info("\n📊 Шаг 1/2: ETL - Spark SQL + Window Functions → Parquet")
            logger.info("-" * 60)
            
            with SparkProcessor(db_config) as processor:
                # Расчёт динамики с оконными функциями
                dynamics_df = processor.calculate_team_dynamics(
                    league_filter=league_filter,
                    season_filter=season_filter,
                    team_names=team_names,
                    output_parquet_path=str(parquet_path)
                )
            
            results['etl_records'] = len(dynamics_df)
            results['parquet_path'] = str(parquet_path)
            
            logger.info(f"✅ ETL завершён. Записей: {len(dynamics_df)}")
            logger.info(f"   Parquet: {parquet_path}")
            
        else:
            logger.info("\n⏭️  Шаг 1/2: ETL пропущен (skip_etl=True)")
            
            # Загружаем существующие данные
            if not parquet_path.exists():
                raise FileNotFoundError(
                    f"Parquet файл не найден: {parquet_path}\n"
                    "Запустите без --skip-etl для создания данных."
                )
            
            logger.info(f"   Загрузка из: {parquet_path}")
            
            with SparkProcessor(db_config) as processor:
                dynamics_df = processor.load_dynamics_from_parquet(str(parquet_path))
            
            # Применяем фильтры если указаны
            if season_filter:
                dynamics_df = dynamics_df[dynamics_df['season_code'] == season_filter]
            if team_names:
                dynamics_df = dynamics_df[dynamics_df['team_name'].isin(team_names)]
            
            results['etl_records'] = len(dynamics_df)
            results['parquet_path'] = str(parquet_path)
        
        # ====================================================================
        # ШАГ 2: ВИЗУАЛИЗАЦИЯ
        # ====================================================================
        
        if not skip_visualizations:
            logger.info("\n📈 Шаг 2/2: Создание визуализаций (Plotly)")
            logger.info("-" * 60)
            
            visualizer = TeamDynamicsVisualizer(output_dir=output_dir)
            
            # Определяем команды для визуализации
            viz_teams = team_names
            
            # 1. График накопленных очков
            logger.info("   → Создание графика накопленных очков...")
            fig1 = visualizer.plot_cumulative_points(
                dynamics_df,
                team_names=viz_teams,
                season_filter=season_filter,
                show=False
            )
            results['visualizations'].append('task3_cumulative_points.html')
            
            # 2. График разницы мячей
            logger.info("   → Создание графика разницы мячей...")
            fig2 = visualizer.plot_cumulative_goal_diff(
                dynamics_df,
                team_names=viz_teams,
                season_filter=season_filter,
                show=False
            )
            results['visualizations'].append('task3_goal_diff_dynamics.html')
            
            # 3. График очков по месяцам
            logger.info("   → Создание графика очков по месяцам...")
            fig3 = visualizer.plot_monthly_aggregation(
                dynamics_df,
                team_names=viz_teams,
                season_filter=season_filter,
                show=False
            )
            results['visualizations'].append('task3_monthly_points.html')
            
            # 4. Комплексный dashboard
            logger.info("   → Создание комплексного dashboard...")
            fig4 = visualizer.create_comprehensive_dashboard(
                dynamics_df,
                team_names=viz_teams,
                season_filter=season_filter,
                show=False
            )
            results['visualizations'].append('task3_comprehensive_dashboard.html')
            
            # 5. Сводная статистика в CSV
            logger.info("   → Генерация сводной статистики...")
            summary_df = visualizer.generate_summary_stats(
                dynamics_df,
                season_filter=season_filter
            )
            summary_path = output_path / "task3_summary_stats.csv"
            summary_df.to_csv(summary_path, index=False, encoding='utf-8')
            results['visualizations'].append('task3_summary_stats.csv')
            
            logger.info(f"✅ Визуализации созданы: {len(results['visualizations'])} файлов")
            
        else:
            logger.info("\n⏭️  Шаг 2/2: Визуализация пропущена (skip_visualizations=True)")
        
        # ====================================================================
        # ИТОГИ
        # ====================================================================
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ АНАЛИЗ УСПЕШНО ЗАВЕРШЁН!")
        logger.info("=" * 80)
        
        logger.info("\n📁 Созданные файлы:")
        logger.info(f"   📦 Parquet: {results['parquet_path']}")
        
        if results['visualizations']:
            logger.info(f"   📊 Визуализации ({output_dir}/):")
            for viz in results['visualizations']:
                logger.info(f"      • {viz}")
        
        # Основные выводы
        if len(dynamics_df) > 0:
            logger.info("\n📊 ОСНОВНЫЕ ВЫВОДЫ:")
            
            # Топ команда по очкам
            final_points = dynamics_df.groupby(['team_name', 'season_code'])['cumulative_points'].max()
            if len(final_points) > 0:
                top_team = final_points.idxmax()
                top_points = final_points.max()
                logger.info(f"   🥇 Лидер по очкам: {top_team[0]} ({top_team[1]}) — {top_points} очков")
            
            # Лучшая разница мячей
            final_gd = dynamics_df.groupby(['team_name', 'season_code'])['cumulative_goal_diff'].max()
            if len(final_gd) > 0:
                best_gd_team = final_gd.idxmax()
                best_gd = final_gd.max()
                logger.info(f"   ⚽ Лучшая разница мячей: {best_gd_team[0]} ({best_gd_team[1]}) — {best_gd:+d}")
        
        logger.info("\n" + "=" * 80)
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Ошибка при выполнении анализа: {e}", exc_info=True)
        results['status'] = 'error'
        results['error'] = str(e)
        raise


# ============================================================================
# CLI INTERFACE
# ============================================================================

def parse_team_names(teams_str: str) -> List[str]:
    """Парсит строку с названиями команд"""
    if not teams_str:
        return None
    return [t.strip() for t in teams_str.split(',')]


def main():
    """Главная функция запуска"""
    
    parser = argparse.ArgumentParser(
        description='Задача 3: Динамика результатов по сезонам (Spark Window Functions)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Анализ EPL за сезон 2023-2024
  python run_task3_team_dynamics.py --league epl --season 2023-2024

  # Конкретные команды
  python run_task3_team_dynamics.py --league epl --teams "Liverpool,Arsenal,Manchester City"

  # Только визуализация (без ETL)
  python run_task3_team_dynamics.py --skip-etl --season 2023-2024

  # Только ETL (без визуализации)
  python run_task3_team_dynamics.py --league epl --skip-viz
        """
    )
    
    # Аргументы фильтрации
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
        help='Фильтр по сезону (например: 2023-2024)'
    )
    parser.add_argument(
        '--teams',
        type=str,
        default=None,
        help='Список команд через запятую (например: "Liverpool,Arsenal,Chelsea")'
    )
    
    # Аргументы путей
    parser.add_argument(
        '--output',
        type=str,
        default='outputs/task3',
        help='Директория для визуализаций (default: outputs/task3)'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default='data/processed/task3',
        help='Директория для Parquet данных (default: data/processed/task3)'
    )
    
    # Флаги пропуска этапов
    parser.add_argument(
        '--skip-etl',
        action='store_true',
        help='Пропустить ETL, использовать существующий Parquet'
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
        logger.info(f"📂 Загрузка конфигурации: {args.config}")
        config = load_config(args.config)
        db_config = config.get('database', {})
        
        # Проверка параметров БД
        required_params = ['host', 'port', 'database', 'user', 'password']
        missing = [p for p in required_params if p not in db_config]
        
        if missing:
            raise ValueError(f"Отсутствуют параметры БД: {missing}")
        
        # Парсинг команд
        team_names = parse_team_names(args.teams)
        
        # Запуск анализа
        results = run_task3_analysis(
            db_config=db_config,
            league_filter=args.league,
            season_filter=args.season,
            team_names=team_names,
            output_dir=args.output,
            data_dir=args.data_dir,
            skip_etl=args.skip_etl,
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

