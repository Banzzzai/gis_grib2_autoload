#!/usr/bin/env python3
"""
Главный модуль для запуска сервиса мониторинга ECMWF
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.downloader import ECMWFDownloader
from src.utils import setup_logging, validate_date_format

def main():
    parser = argparse.ArgumentParser(
        description='Сервис мониторинга и загрузки данных ECMWF',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Режимы работы:
  python main.py                    # Запуск постоянного сервиса
  python main.py --once             # Разовая загрузка (сегодня)
  python main.py --date 20260730    # Разовая загрузка за дату
  python main.py --debug            # Сервис с подробным логом

Сервис работает постоянно и автоматически:
  • Ожидает появления данных 00z (6:00-10:00 утра)
  • Проверяет каждые 2 минуты в утреннее окно
  • Автоматически загружает данные при появлении
  • Каждый час проверяет, не пропущены ли данные
        """
    )
    
    parser.add_argument('--once', action='store_true',
                        help='Разовая загрузка (не запускать сервис)')
    parser.add_argument('--date', type=str,
                        help='Дата для разовой загрузки (YYYYMMDD)')
    parser.add_argument('--debug', action='store_true',
                        help='Подробный лог')
    
    args = parser.parse_args()
    
    if args.debug:
        import os
        os.environ['LOG_LEVEL'] = 'DEBUG'
        print("🐛 Отладочный режим")
    
    if args.date and not validate_date_format(args.date):
        print(f"❌ Неверный формат даты '{args.date}'. Используйте YYYYMMDD")
        sys.exit(1)
    
    logger = setup_logging("main")
    
    try:
        downloader = ECMWFDownloader()
        
        if args.once or args.date:
            # Разовая загрузка
            from datetime import datetime
            date_str = args.date if args.date else datetime.now().strftime("%Y%m%d")
            logger.info(f"📥 Разовая загрузка за {date_str}")
            downloader.process_date(date_str, wait_for_data=False)
        else:
            # Запуск постоянного сервиса
            downloader.start_service()
        
    except KeyboardInterrupt:
        logger.info("\n👋 Работа завершена")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()