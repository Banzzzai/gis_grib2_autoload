"""
Основной модуль для скачивания и загрузки данных ECMWF
"""
import os
import time
import requests
import re
import schedule
import signal
import sys
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from minio import Minio
from minio.error import S3Error
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import *
from .utils import setup_logging

class ECMWFDownloader:
    """
    Класс для работы с данными ECMWF
    """
    
    def __init__(self):
        """
        Инициализация загрузчика
        """
        self.logger = setup_logging(self.__class__.__name__)
        self.session = self._create_session()
        self.minio_client = self._init_minio()
        self.stats = {
            "downloaded": 0,
            "uploaded": 0,
            "skipped": 0,
            "errors": 0
        }
        self.running = True
        self.current_task_date = None
        
        # Настройка обработчика сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов завершения"""
        self.logger.info(f"\n🛑 Получен сигнал завершения. Завершаю работу...")
        self.running = False
    
    def _create_session(self) -> requests.Session:
        """
        Создание HTTP сессии с настройками
        """
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=DOWNLOAD_CONFIG["retry_count"],
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _init_minio(self) -> Minio:
        """
        Инициализация клиента Minio
        """
        try:
            client = Minio(
                MINIO_CONFIG["endpoint"],
                access_key=MINIO_CONFIG["access_key"],
                secret_key=MINIO_CONFIG["secret_key"],
                secure=MINIO_CONFIG["secure"],
                region=MINIO_CONFIG["region"]
            )
            
            bucket = MINIO_CONFIG["bucket"]
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                self.logger.info(f"Создан бакет: {bucket}")
            else:
                self.logger.info(f"Используем существующий бакет: {bucket}")
            
            return client
            
        except Exception as e:
            self.logger.error(f"Ошибка подключения к Minio: {e}")
            raise
    
    def start_service(self):
        """
        Запуск постоянного сервиса с планировщиком
        """
        self.logger.info("="*60)
        self.logger.info("🚀 ЗАПУСК СЕРВИСА МОНИТОРИНГА ECMWF")
        self.logger.info("="*60)
        self.logger.info(f"Целевой слот: 00z (появление ~6:00-10:00)")
        self.logger.info(f"Модель: {DOWNLOAD_CONFIG['model']}")
        self.logger.info(f"Разрешение: {DOWNLOAD_CONFIG['resolution']}")
        self.logger.info(f"Папки: {', '.join(DOWNLOAD_CONFIG['target_folders'])}")
        self.logger.info(f"Бакет Minio: {MINIO_CONFIG['bucket']}")
        self.logger.info("="*60)
        
        # Планируем задачи
        self._schedule_jobs()
        
        # Основной цикл
        self.logger.info("📅 Сервис запущен. Ожидание по расписанию...")
        self.logger.info("   Нажмите Ctrl+C для остановки\n")
        
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(30)  # Проверяем планировщик каждые 30 секунд
            except KeyboardInterrupt:
                self.logger.info("\n⚠ Прерывание работы...")
                break
            except Exception as e:
                self.logger.error(f"Ошибка в главном цикле: {e}")
                time.sleep(60)
        
        self.logger.info("👋 Сервис остановлен")
    
    def _schedule_jobs(self):
        """
        Настройка заданий планировщика
        """
        # Утренняя проверка: каждые 2 минуты с 6:00 до 12:00
        schedule.every().day.at("06:00").do(self._start_morning_check)
        schedule.every().day.at("12:00").do(self._stop_morning_check)
        
        # Дневная проверка (на случай задержки): каждый час с 12:00 до 18:00
        schedule.every().hour.at(":00").do(self._check_if_missed).tag("hourly-check")
        
        # Ежедневный сброс статистики в полночь
        schedule.every().day.at("00:00").do(self._reset_daily_stats)
        
        # Статусный отчет каждый час
        schedule.every().hour.at(":30").do(self._status_report)
        
        self.logger.info("✅ Расписание настроено:")
        self.logger.info("   • 06:00-12:00 — проверка каждые 10 минуты")
        self.logger.info("   • 12:00-18:00 — проверка каждый час (если пропущено)")
        self.logger.info("   • 00:00 — сброс статистики")
        self.logger.info("   • Каждые 30 мин — отчет о состоянии")
    
    def _start_morning_check(self):
        """Запуск утреннего мониторинга"""
        self.logger.info("")
        self.logger.info("🌅 Начало утреннего мониторинга (06:00)")
        self.logger.info("="*50)
        
        # Определяем дату для проверки
        now = datetime.now()
        if now.hour < 10:  # Если сейчас раньше 10 утра, проверяем вчерашний 00z
            self.current_task_date = (now - timedelta(days=1)).strftime("%Y%m%d")
        else:
            self.current_task_date = now.strftime("%Y%m%d")
        
        self.logger.info(f"📅 Целевая дата: {self.current_task_date}")
        
        # Запускаем частые проверки
        self._intensive_check()
    
    def _intensive_check(self):
        """Интенсивная проверка каждые 2 минуты"""
        if not self.current_task_date:
            return
        
        slot_url = f"{ECMWF_BASE_URL}/{self.current_task_date}/00z/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}"
        
        check_count = 0
        max_checks = 36  # Максимум 36 проверок (6 часов)
        
        while self.running and check_count < max_checks:
            check_count += 1
            current_time = datetime.now()
            
            # Если уже позже 12:00, прекращаем интенсивную проверку
            if current_time.hour >= 12:
                self.logger.info("🕛 Полдень. Прекращение интенсивной проверки.")
                break
            
            # Проверяем наличие данных
            if self._check_url_exists(slot_url):
                self.logger.info("")
                self.logger.info(f"🎉 Данные появились! Время: {current_time.strftime('%H:%M:%S')}")
                self.logger.info(f"   Выполнено проверок: {check_count}")
                
                # Загружаем данные
                self._process_time_slot(self.current_task_date, "00z")
                
                # Сбрасываем задачу
                self.current_task_date = None
                self._reset_stats()
                break
            
            # Логируем каждую проверку (каждые 10 минут)
            elapsed = check_count * 10
            self.logger.info(f"⏳ Проверка {check_count}: данных ещё нет (прошло {elapsed} мин)")
            
            time.sleep(600)  # 10 минут в сек
        
        if check_count >= max_checks:
            self.logger.warning("⚠ Превышено максимальное количество проверок")
    
    def _stop_morning_check(self):
        """Остановка утреннего мониторинга"""
        self.logger.info("🕛 Завершение утреннего мониторинга (12:00)")
    
    def _check_if_missed(self):
        """Проверка, не пропустили ли данные (выполняется каждый час)"""
        now = datetime.now()
        
        # Проверяем только с 12:00 до 18:00
        if now.hour < 12 or now.hour > 18:
            return
        
        date_str = now.strftime("%Y%m%d")
        slot_url = f"{ECMWF_BASE_URL}/{date_str}/00z/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}"
        
        if self._check_url_exists(slot_url):
            self.logger.info(f"🔍 Обнаружены пропущенные данные за {date_str}")
            
            # Проверяем, не загружены ли уже эти данные
            minio_prefix = f"{date_str}/00z/"
            try:
                objects = list(self.minio_client.list_objects(
                    MINIO_CONFIG['bucket'], 
                    prefix=minio_prefix, 
                    recursive=True
                ))
                if len(objects) == 0:
                    self.logger.info("📥 Данные ещё не загружены. Выполняю загрузку...")
                    self._process_time_slot(date_str, "00z")
                else:
                    self.logger.info("✅ Данные уже загружены ранее")
            except S3Error:
                self.logger.info("📥 Выполняю загрузку...")
                self._process_time_slot(date_str, "00z")
    
    def _status_report(self):
        """Вывод статуса сервиса"""
        now = datetime.now()
        status = "Активен" if self.running else "Останавливается"
        
        self.logger.info(f"📊 Статус: {status} | Время: {now.strftime('%H:%M')} | "
                        f"Скачано: {self.stats['downloaded']} | "
                        f"Загружено: {self.stats['uploaded']} | "
                        f"Ошибок: {self.stats['errors']}")
    
    def _reset_daily_stats(self):
        """Ежедневный сброс статистики"""
        self.logger.info("📅 Полночь. Сброс дневной статистики.")
        self._reset_stats()
    
    def _reset_stats(self):
        """Сброс статистики"""
        self.stats = {
            "downloaded": 0,
            "uploaded": 0,
            "skipped": 0,
            "errors": 0
        }

    def process_date(self, date_str: str, wait_for_data: bool = False):
        """
        Обработка данных за конкретную дату - для разовых запусков
        
        Args:
            date_str: Дата в формате YYYYMMDD
            wait_for_data: Если True - ждать появления данных
        """
        date_url = f"{ECMWF_BASE_URL}/{date_str}"
        
        if not self._check_url_exists(date_url):
            self.logger.warning(f"Нет данных за дату: {date_str}")
            return False
        
        time_slot = "00z"
        slot_url = f"{ECMWF_BASE_URL}/{date_str}/{time_slot}/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}"
        
        # Если данные уже есть - сразу загружаем
        if self._check_url_exists(slot_url):
            self.logger.info(f"✅ Слот {time_slot} доступен. Загрузка...")
            self._process_time_slot(date_str, time_slot)
            self._print_stats()
            return True
        
        # Если не ждем - выходим
        if not wait_for_data:
            self.logger.warning(f"Слот {time_slot} не доступен для {date_str}")
            return False
        
        # Режим ожидания
        self.logger.info(f"⏳ Ожидание появления {time_slot} для {date_str}...")
        
        while self.running:
            if self._check_url_exists(slot_url):
                self.logger.info(f"🎉 Данные появились! Загрузка...")
                self._process_time_slot(date_str, time_slot)
                self._print_stats()
                return True
            
            self.logger.info(f"⏳ Данных ещё нет. Повторная проверка через 10 минут...")
            time.sleep(600)
        
        return False

    def _print_stats(self):
        """Вывод статистики"""
        self.logger.info("="*50)
        self.logger.info("СТАТИСТИКА:")
        self.logger.info(f"  Скачано: {self.stats['downloaded']}")
        self.logger.info(f"  Загружено в Minio: {self.stats['uploaded']}")
        self.logger.info(f"  Пропущено: {self.stats['skipped']}")
        self.logger.info(f"  Ошибок: {self.stats['errors']}")
        self.logger.info("="*50)
    
    def _check_url_exists(self, url: str) -> bool:
        """
        Проверка существования URL
        """
        try:
            response = self.session.head(url, allow_redirects=True, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def _process_time_slot(self, date_str: str, time_slot: str):
        """
        Обработка временного слота
        """
        base_url = f"{ECMWF_BASE_URL}/{date_str}/{time_slot}/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}"
        
        self.logger.info(f"📂 Обработка: {base_url}")
        
        if not self._check_url_exists(base_url):
            self.logger.warning(f"❌ URL недоступен: {base_url}")
            return
        
        for folder in DOWNLOAD_CONFIG["target_folders"]:
            folder_url = f"{base_url}/{folder}"
            self._process_folder(date_str, time_slot, folder, folder_url)
    
    def _process_folder(self, date_str: str, time_slot: str, folder: str, folder_url: str):
        """
        Обработка конкретной папки с параллельной загрузкой
        """
        self.logger.info(f"📁 Проверка папки: {folder_url}")
        
        if not self._check_url_exists(folder_url):
            self.logger.debug(f"Папка не найдена: {folder_url}")
            return
        
        items = self._list_directory(folder_url)
        grib_files = [item for item in items if item['name'].endswith('.grib2')]
        
        if not grib_files:
            self.logger.warning(f"⚠ Нет .grib2 файлов в {folder}")
            return
        
        max_workers = DOWNLOAD_CONFIG.get("max_parallel_downloads", 5)
        self.logger.info(f"📄 Найдено {len(grib_files)} файлов. Загрузка в {max_workers} потоков...")
        
        start_time = datetime.now()
        completed = 0
        total = len(grib_files)
        
        with tempfile.TemporaryDirectory(dir=TEMP_DIR) as temp_dir:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                
                for file_info in grib_files:
                    future = executor.submit(
                        self._download_and_upload,
                        file_info['url'],
                        file_info['name'],
                        date_str,
                        time_slot,
                        folder,
                        temp_dir
                    )
                    futures[future] = file_info['name']
                
                self.logger.info(f"⏳ Отправлено {total} задач в {max_workers} потоков. Ожидание...")
                
                # Используем as_completed с таймаутом
                for future in as_completed(futures, timeout=7200):  # 2 часа максимум
                    completed += 1
                    filename = futures[future]
                    try:
                        future.result(timeout=600)  # 10 минут на один файл
                        
                        # Прогресс каждые 5 файлов или последний
                        if completed % 5 == 0 or completed == total:
                            elapsed = (datetime.now() - start_time).seconds
                            speed = completed / (elapsed / 60) if elapsed > 0 else 0
                            self.logger.info(f"  📊 Прогресс: {completed}/{total} ({speed:.1f} файл/мин, прошло {elapsed//60}м {elapsed%60}с)")
                            
                    except Exception as e:
                        self.stats["errors"] += 1
                        self.logger.error(f"  ❌ Ошибка в {filename}: {type(e).__name__}: {str(e)[:100]}")
        
        elapsed = (datetime.now() - start_time).seconds
        self.logger.info(f"✅ Папка {folder}: {completed}/{total} файлов за {elapsed}с ({elapsed//60}м {elapsed%60}с)")
        self.logger.info(f"   Скачано: {self.stats['downloaded']}, Ошибок: {self.stats['errors']}")
    
    def _list_directory(self, url: str) -> list:
        """
        Получение списка содержимого директории
        """
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            items = []
            html = response.text
            
            # Ищем ссылки через regex
            links = re.findall(r'href="([^"]*)"', html)
            
            for href in links:
                if not href or href.startswith('?') or href == '/' or href == '../':
                    continue
                
                clean_href = href.rstrip('/')
                if not clean_href:
                    continue
                
                full_url = urljoin(url, clean_href)
                is_directory = href.endswith('/')
                
                items.append({
                    'name': clean_href,
                    'url': full_url,
                    'is_directory': is_directory
                })
            
            return items
            
        except Exception as e:
            self.logger.error(f"Ошибка при чтении директории {url}: {e}")
            return []
    
    import threading

    def _download_and_upload(self, file_url: str, filename: str, 
                            date_str: str, time_slot: str, folder: str, temp_dir: str):
        """
        Скачивание и загрузка файла в Minio (с динамической строкой прогресса)
        """
        actual_filename = filename.split('/')[-1] if '/' in filename else filename
        minio_path = f"{date_str}/{time_slot}/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}/{folder}/{actual_filename}"
        
        short_name = actual_filename.replace('20260803000000-', '').replace('-oper-fc.grib2', '')
        thread_id = threading.current_thread().name.split('-')[-1][:2]
        
        # Проверяем существование в Minio
        try:
            self.minio_client.stat_object(MINIO_CONFIG["bucket"], minio_path)
            self.logger.info(f"[T{thread_id}] ⏭ {short_name}")
            self.stats["skipped"] += 1
            return
        except S3Error:
            pass
        
        local_path = os.path.join(temp_dir, actual_filename)
        start_time = time.time()
        
        try:
            response = self.session.get(file_url, stream=True, timeout=(30, 600))
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            file_size = 0
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                        file_size += len(chunk)
                        
                        if total_size > 0:
                            pct = file_size / total_size * 100
                            elapsed = time.time() - start_time
                            speed = file_size / elapsed / 1024 if elapsed > 0 else 0
                            bar_len = 20
                            filled = int(bar_len * pct / 100)
                            bar = '█' * filled + '░' * (bar_len - filled)
                            
                            # Динамическая строка в stderr
                            sys.stderr.write(f"\r[T{thread_id}] {short_name}: [{bar}] {pct:5.1f}% {file_size//(1024*1024)}/{total_size//(1024*1024)}MB {speed:5.0f}KB/s")
                            sys.stderr.flush()
            
            elapsed = time.time() - start_time
            sys.stderr.write(f"\r[T{thread_id}] ✅ {short_name}: {file_size//(1024*1024)}MB за {elapsed:.0f}с{' ' * 30}\n")
            sys.stderr.flush()
            
            self.stats["downloaded"] += 1
            
            self.minio_client.fput_object(
                MINIO_CONFIG["bucket"],
                minio_path,
                local_path,
                content_type='application/x-grib2'
            )
            self.stats["uploaded"] += 1
            
        except Exception as e:
            sys.stderr.write(f"\r[T{thread_id}] ❌ {short_name}: {e}{' ' * 30}\n")
            sys.stderr.flush()
            self.stats["errors"] += 1
            raise
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)