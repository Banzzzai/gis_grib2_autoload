"""
Вспомогательные утилиты и логирование
"""
import logging
import logging.handlers
import sys
from typing import Optional
from datetime import datetime
import colorlog
from .config import LOG_CONFIG

class Logger:
    """
    Фабрика логгеров. При инициализации возвращает настроенный экземпляр logging.Logger.
    Формат идентичен common/logger.py для совместимости, но без зависимости от common.
    """
    def __new__(
        cls,
        name: str,
        level: int = logging.INFO,
        log_file: Optional[str] = None
    ) -> logging.Logger:
        logger = logging.getLogger(name)
        
        # Защита от дублирования хендлеров
        if logger.handlers:
            return logger
            
        logger.setLevel(level)
        logger.propagate = False

        log_format = "%(log_color)s[%(levelname)-8s]%(reset)s %(asctime)s - %(name)s - %(message)s (%(filename)s:%(lineno)d)"
        log_file_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
        date_format = "%Y-%m-%d %H:%M:%S"

        # 1. Консольный хендлер с цветом
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        formatter = colorlog.ColoredFormatter(
            fmt=log_format,
            datefmt=date_format,
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 2. Файловый хендлер с ротацией (без цвета)
        if log_file:
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=LOG_CONFIG.get("max_bytes", 10 * 1024 * 1024),
                backupCount=LOG_CONFIG.get("backup_count", 5)
            )
            file_handler.setLevel(level)
            file_formatter = logging.Formatter(log_file_format, datefmt=date_format)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        return logger

def setup_logging(name: str = None) -> logging.Logger:
    """Настройка логирования с использованием нашей фабрики Logger."""
    level_str = LOG_CONFIG.get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    
    return Logger(
        name=name or "root",
        level=level,
        log_file=LOG_CONFIG.get("file")
    )

def validate_date_format(date_str: str) -> bool:
    """Проверка формата даты YYYYMMDD"""
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        return False