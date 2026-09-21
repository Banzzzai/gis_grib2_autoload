"""
Конфигурация приложения
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Базовые пути
BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / "logs"
TEMP_DIR = BASE_DIR / "temp"

# Создаем директории если их нет
LOGS_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)

# Настройки ECMWF
ECMWF_BASE_URL = os.getenv("ECMWF_BASE_URL", "https://data.ecmwf.int/forecasts")

# Настройки Minio
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "minio:10000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "adminminio"),
    "secure": os.getenv("MINIO_SECURE", "False").lower() == "true",
    "bucket": os.getenv("MINIO_BUCKET", "ecmwf-data"),
    "region": os.getenv("MINIO_REGION", "us-east-1")
}

# Параметры загрузки
DOWNLOAD_CONFIG = {
    "time_slots": ["00z", "06z", "12z", "18z"],
    "model": "ifs",
    "resolution": "0p25",
    "target_folders": ["oper"], 
    "delay_between_downloads": 0,
    "retry_count": 3,
    "max_parallel_downloads": 20,
}

# Логирование
LOG_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "file": str(LOGS_DIR / "ecmwf_download.log"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "max_bytes": 10 * 1024 * 1024,
    "backup_count": 5
}