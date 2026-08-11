"""
Вспомогательные утилиты
"""
import logging
import logging.handlers
import sys
from pathlib import Path
from .config import LOG_CONFIG

def setup_logging(name: str = None) -> logging.Logger:
    """
    Настройка логирования
    """
    logger = logging.getLogger(name or __name__)
    
    # Проверяем, не настроен ли уже логгер
    if logger.handlers:
        return logger
    
    logger.setLevel(LOG_CONFIG["level"])
    
    # Форматтер
    formatter = logging.Formatter(LOG_CONFIG["format"])
    
    # Файловый handler с ротацией
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_CONFIG["file"],
        maxBytes=LOG_CONFIG["max_bytes"],
        backupCount=LOG_CONFIG["backup_count"]
    )
    file_handler.setFormatter(formatter)
    
    # Консольный handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def validate_date_format(date_str: str) -> bool:
    """
    Проверка формата даты YYYYMMDD
    """
    from datetime import datetime
    
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        return False