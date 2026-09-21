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
from urllib.parse import urljoin
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from minio import Minio
from minio.error import S3Error

from .config import *
from .utils import setup_logging

class ECMWFDownloader:
    def __init__(self):
        self.logger = setup_logging(self.__class__.__name__)
        self.session = self._create_session()
        self.minio_client = self._init_minio()
        self.stats = {"downloaded": 0, "uploaded": 0, "skipped": 0, "errors": 0}
        self.running = True
        self.current_task_date = None

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self.logger.info("\n🛑 Получен сигнал завершения. Завершаю работу...")
        self.running = False

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        retry_strategy = Retry(total=DOWNLOAD_CONFIG["retry_count"], backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _init_minio(self) -> Minio:
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
        self.logger.info("="*60)
        self.logger.info("🚀 ЗАПУСК СЕРВИСА МОНИТОРИНГА ECMWF")
        self.logger.info("="*60)
        self.logger.info(f"Целевой слот: 00z (появление ~6:00-10:00)")
        self.logger.info(f"Модель: {DOWNLOAD_CONFIG['model']}")
        self.logger.info(f"Разрешение: {DOWNLOAD_CONFIG['resolution']}")
        self.logger.info(f"Папки: {', '.join(DOWNLOAD_CONFIG['target_folders'])}")
        self.logger.info(f"Бакет Minio: {MINIO_CONFIG['bucket']}")
        self.logger.info("="*60)

        self._schedule_jobs()
        self.logger.info("📅 Сервис запущен. Ожидание по расписанию...")
        self.logger.info("   Нажмите Ctrl+C для остановки\n")
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(30)
            except KeyboardInterrupt:
                self.logger.info("\n⚠ Прерывание работы...")
                break
            except Exception as e:
                self.logger.error(f"Ошибка в главном цикле: {e}")
                time.sleep(60)
        self.logger.info("👋 Сервис остановлен")

    def _schedule_jobs(self):
        schedule.every().day.at("06:00").do(self._start_morning_check)
        schedule.every().day.at("12:00").do(self._stop_morning_check)
        schedule.every().hour.at(":00").do(self._check_if_missed).tag("hourly-check")
        schedule.every().day.at("00:00").do(self._reset_daily_stats)
        schedule.every().hour.at(":30").do(self._status_report)

    def _start_morning_check(self):
        self.logger.info("🌅 Начало утреннего мониторинга (06:00)")
        now = datetime.now()
        self.current_task_date = (now - timedelta(days=1)).strftime("%Y%m%d") if now.hour < 10 else now.strftime("%Y%m%d")
        self.logger.info(f"📅 Целевая дата: {self.current_task_date}")
        self._intensive_check()

    def _intensive_check(self):
        if not self.current_task_date:
            return
        slot_url = f"{ECMWF_BASE_URL}/{self.current_task_date}/00z/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}"
        check_count = 0
        max_checks = 36
        while self.running and check_count < max_checks:
            check_count += 1
            current_time = datetime.now()
            if current_time.hour >= 12:
                break
            if self._check_url_exists(slot_url):
                self.logger.info(f"🎉 Данные появились! Время: {current_time.strftime('%H:%M:%S')}")
                self._process_time_slot(self.current_task_date, "00z")
                self.current_task_date = None
                self._reset_stats()
                break
            time.sleep(600)

    def _stop_morning_check(self):
        self.logger.info("🕛 Завершение утреннего мониторинга (12:00)")

    def _check_if_missed(self):
        now = datetime.now()
        if now.hour < 12 or now.hour > 18:
            return
        date_str = now.strftime("%Y%m%d")
        slot_url = f"{ECMWF_BASE_URL}/{date_str}/00z/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}"
        if self._check_url_exists(slot_url):
            self.logger.info(f"🔍 Обнаружены пропущенные данные за {date_str}")
            minio_prefix = f"{date_str}/00z/"
            try:
                objects = list(self.minio_client.list_objects(MINIO_CONFIG['bucket'], prefix=minio_prefix, recursive=True))
                if len(list(objects)) == 0:
                    self._process_time_slot(date_str, "00z")
            except S3Error:
                self._process_time_slot(date_str, "00z")

    def _status_report(self):
        now = datetime.now()
        status = "Активен" if self.running else "Останавливается"
        self.logger.info(f"📊 Статус: {status} | Время: {now.strftime('%H:%M')} | Скачано: {self.stats['downloaded']} | Загружено: {self.stats['uploaded']} | Ошибок: {self.stats['errors']}")

    def _reset_daily_stats(self):
        self.logger.info("📅 Полночь. Сброс дневной статистики.")
        self._reset_stats()

    def _reset_stats(self):
        self.stats = {"downloaded": 0, "uploaded": 0, "skipped": 0, "errors": 0}

    def process_date(self, date_str: str, wait_for_data: bool = False):
        date_url = f"{ECMWF_BASE_URL}/{date_str}"
        if not self._check_url_exists(date_url):
            self.logger.warning(f"Нет данных за дату: {date_str}")
            return False
        time_slot = "00z"
        slot_url = f"{ECMWF_BASE_URL}/{date_str}/{time_slot}/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}"
        if self._check_url_exists(slot_url):
            self._process_time_slot(date_str, time_slot)
            self._print_stats()
            return True
        if not wait_for_data:
            return False
        while self.running:
            if self._check_url_exists(slot_url):
                self._process_time_slot(date_str, time_slot)
                self._print_stats()
                return True
            time.sleep(600)
        return False

    def _print_stats(self):
        self.logger.info("="*50)
        self.logger.info(f"СТАТИСТИКА: Скачано: {self.stats['downloaded']} | Загружено: {self.stats['uploaded']} | Пропущено: {self.stats['skipped']} | Ошибок: {self.stats['errors']}")
        self.logger.info("="*50)

    def _check_url_exists(self, url: str) -> bool:
        try:
            response = self.session.head(url, allow_redirects=True, timeout=10)
            return response.status_code == 200
        except:
            return False

    def _process_time_slot(self, date_str: str, time_slot: str):
        base_url = f"{ECMWF_BASE_URL}/{date_str}/{time_slot}/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}"
        if not self._check_url_exists(base_url):
            return
        for folder in DOWNLOAD_CONFIG["target_folders"]:
            self._process_folder(date_str, time_slot, folder, f"{base_url}/{folder}")

    def _process_folder(self, date_str: str, time_slot: str, folder: str, folder_url: str):
        if not self._check_url_exists(folder_url):
            return
        items = self._list_directory(folder_url)
        grib_files = [item for item in items if item['name'].endswith('.grib2')]
        if not grib_files:
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
                    future = executor.submit(self._download_and_upload, file_info['url'], file_info['name'], date_str, time_slot, folder, temp_dir)
                    futures[future] = file_info['name']

                for future in as_completed(futures, timeout=7200):
                    completed += 1
                    try:
                        future.result(timeout=600)
                    except Exception as e:
                        self.stats["errors"] += 1
                        self.logger.error(f"❌ Ошибка: {type(e).__name__}: {str(e)[:100]}")

    def _list_directory(self, url: str) -> list:
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            items = []
            for href in re.findall(r'href="([^"]*)"', response.text):
                if not href or href.startswith('?') or href in ['/', '../']:
                    continue
                clean_href = href.rstrip('/')
                if not clean_href:
                    continue
                items.append({'name': clean_href, 'url': urljoin(url, clean_href), 'is_directory': href.endswith('/')})
            return items
        except Exception as e:
            self.logger.error(f"Ошибка при чтении директории {url}: {e}")
            return []

    def _download_and_upload(self, file_url: str, filename: str, date_str: str, time_slot: str, folder: str, temp_dir: str):
        actual_filename = filename.split('/')[-1] if '/' in filename else filename
        minio_path = f"{date_str}/{time_slot}/{DOWNLOAD_CONFIG['model']}/{DOWNLOAD_CONFIG['resolution']}/{folder}/{actual_filename}"
        short_name = actual_filename.replace('20260803000000-', '').replace('-oper-fc.grib2', '')
        thread_id = threading.current_thread().name.split('-')[-1][:2]

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
                            sys.stderr.write(f"\r[T{thread_id}] {short_name}: [{bar}] {pct:5.1f}% {file_size//(1024*1024)}/{total_size//(1024*1024)}MB {speed:5.0f}KB/s")
                            sys.stderr.flush()

            elapsed = time.time() - start_time
            sys.stderr.write(f"\r[T{thread_id}] ✅ {short_name}: {file_size//(1024*1024)}MB за {elapsed:.0f}с{' ' * 30}\n")
            sys.stderr.flush()

            self.stats["downloaded"] += 1
            self.minio_client.fput_object(MINIO_CONFIG["bucket"], minio_path, local_path, content_type='application/x-grib2')
            self.stats["uploaded"] += 1
        except Exception as e:
            sys.stderr.write(f"\r[T{thread_id}] ❌ {short_name}: {e}{' ' * 30}\n")
            sys.stderr.flush()
            self.stats["errors"] += 1
            raise
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)