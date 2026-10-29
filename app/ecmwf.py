import asyncio
import logging
import random
import re
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
import httpx
from common_stribog3.minio_client.async_minio_client import AsyncMinioClient

from app.config import Settings

logger = logging.getLogger(__name__)

GRIB_SUFFIXES = (".grib2", ".grb2")


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag, attrs) -> None:
        if tag.lower() != "a":
            return

        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


class ECMWFDownloader:
    def __init__(
        self,
        settings: Settings,
        store: AsyncMinioClient,
        http: httpx.AsyncClient,
    ) -> None:
        self.settings = settings
        self.store = store
        self.http = http
        self.semaphore = asyncio.Semaphore(
            settings.max_parallel_downloads
        )
        self.settings.temp_dir.mkdir(parents=True, exist_ok=True)

    async def _get_with_retry(self, url: str) -> httpx.Response:
        last_error = None

        for attempt in range(self.settings.retry_count + 1):
            try:
                response = await self.http.get(url)

                if response.status_code in {429, 500, 502, 503, 504}:
                    response.raise_for_status()

                return response

            except (
                httpx.TimeoutException,
                httpx.NetworkError,
                httpx.HTTPStatusError,
            ) as exc:
                last_error = exc

                if attempt >= self.settings.retry_count:
                    break

                delay = (
                    self.settings.retry_backoff_seconds * (2 ** attempt)
                    + random.uniform(0, 0.5)
                )

                logger.warning(
                    "HTTP request failed; retry %s/%s in %.1fs: %s",
                    attempt + 1,
                    self.settings.retry_count,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

        raise last_error

    async def list_grib_files(self, directory_url: str) -> list[str]:
        response = await self._get_with_retry(directory_url)

        if response.status_code == 404:
            return []

        response.raise_for_status()

        parser = LinkParser()
        parser.feed(response.text)

        result = []
        expected_host = urlparse(directory_url).netloc

        for href in parser.links:
            absolute = urljoin(directory_url, href)
            parsed = urlparse(absolute)

            # Не следуем по ссылкам на посторонние домены.
            if parsed.netloc != expected_host:
                continue

            name = parsed.path.rstrip("/").rsplit("/", 1)[-1]

            if name.lower().endswith(GRIB_SUFFIXES):
                result.append(absolute)

        return sorted(set(result))

    def _object_key(self, source_url: str) -> str:
        parsed = urlparse(source_url)
        base_path = urlparse(
            self.settings.normalized_base_url
        ).path.rstrip("/") + "/"

        path = parsed.path

        if base_path and path.startswith(base_path):
            path = path[len(base_path):]

        key = path.lstrip("/")

        if not key or ".." in key.split("/"):
            raise ValueError(f"Unexpected ECMWF object path: {source_url}")

        return key

    def _bucket_for_url(self, source_url: str) -> str:
        parts = urlparse(source_url).path.strip("/").split("/")

        if len(parts) < 2:
            raise ValueError(f"Unexpected ECMWF URL: {source_url}")

        stream = parts[-2]

        bucket_by_stream = {
            "oper": self.settings.minio_bucket,
            "wave": self.settings.minio_wave_bucket,
        }

        try:
            return bucket_by_stream[stream]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported ECMWF stream: {stream}"
            ) from exc

    async def _download_one(self, source_url: str) -> bool:
        async with self.semaphore:
            key = self._object_key(source_url)
            bucket = self._bucket_for_url(source_url)

            try:
                if await self.store.exists_object(bucket, key):
                    logger.debug("Already in MinIO: %s/%s", bucket, key)
                    return False

                filename = re.sub(
                    r"[^A-Za-z0-9_.-]",
                    "_",
                    key.replace("/", "__"),
                )

                temp_path = (
                        self.settings.temp_dir
                        / f"{filename}.{datetime.now(UTC).timestamp():.6f}.part"
                )

                try:
                    for attempt in range(self.settings.retry_count + 1):
                        temp_path.unlink(missing_ok=True)

                        try:
                            async with self.http.stream(
                                    "GET",
                                    source_url,
                            ) as response:
                                if response.status_code == 404:
                                    logger.info(
                                        "Not published yet: %s",
                                        source_url,
                                    )
                                    return False

                                if response.status_code in {
                                    429, 500, 502, 503, 504
                                }:
                                    response.raise_for_status()

                                response.raise_for_status()

                                with temp_path.open("wb") as output:
                                    async for chunk in response.aiter_bytes(
                                            1024 * 1024
                                    ):
                                        output.write(chunk)

                            break

                        except (
                                httpx.TimeoutException,
                                httpx.NetworkError,
                                httpx.HTTPStatusError,
                        ):
                            if attempt >= self.settings.retry_count:
                                raise

                            delay = (
                                    self.settings.retry_backoff_seconds
                                    * (2 ** attempt)
                                    + random.uniform(0, 0.5)
                            )

                            logger.warning(
                                "Download failed; retry %s/%s in %.1fs: %s",
                                attempt + 1,
                                self.settings.retry_count,
                                delay,
                                source_url,
                            )
                            await asyncio.sleep(delay)

                    if not temp_path.exists() or temp_path.stat().st_size == 0:
                        raise RuntimeError(
                            f"Downloaded file is empty: {source_url}"
                        )

                    size = temp_path.stat().st_size

                    await self.store.add_object(
                        bucket,
                        key,
                        temp_path,
                    )

                    logger.info(
                        "Uploaded %s/%s (%d bytes)",
                        bucket,
                        key,
                        size,
                    )
                    return True

                finally:
                    temp_path.unlink(missing_ok=True)

            except Exception:
                logger.exception(
                    "Failed processing ECMWF file: %s",
                    source_url,
                )
                return False

    @staticmethod
    def _forecast_valid_time(source_url: str) -> datetime | None:
        parsed = urlparse(source_url)
        filename = parsed.path.rsplit("/", 1)[-1]

        # Шаг прогноза: например, 0h, 3h, 12h или 2d.
        match = re.search(
            r"-(\d+)([hd]?)-(oper|wave)-fc(?:\.|$)",
            filename,
            re.IGNORECASE,
        )

        if not match:
            logger.debug("Cannot parse forecast step: %s", filename)
            return None

        step = int(match.group(1))
        unit = match.group(2).lower()

        if unit == "d":
            step *= 24

        parts = parsed.path.strip("/").split("/")

        try:
            filename_index = len(parts) - 1
            stream_index = filename_index - 1

            stream = parts[stream_index]
            cycle_text = parts[stream_index - 3]
            date_text = parts[stream_index - 4]

            if stream not in {"oper", "wave"}:
                return None

            if not re.fullmatch(r"\d{8}", date_text):
                return None

            if not re.fullmatch(r"\d{2}z", cycle_text, re.IGNORECASE):
                return None

            initialization = datetime.strptime(
                f"{date_text}{cycle_text[:2]}",
                "%Y%m%d%H",
            ).replace(tzinfo=UTC)

            return initialization + timedelta(hours=step)

        except (ValueError, IndexError):
            logger.debug("Cannot determine forecast time: %s", source_url)
            return None

    async def run_once(self) -> dict[str, int]:
        now = datetime.now(UTC)
        cutoff = now - timedelta(
            hours=self.settings.ecmwf_cycle_delay_hours
        )

        # Ищем последний цикл, который уже достиг порога готовности.
        available_cycles: list[datetime] = []

        for offset in range(self.settings.ecmwf_lookback_days):
            cycle_date = now.date() - timedelta(days=offset)

            for slot in self.settings.time_slots:
                cycle_hour = int(slot[:2])

                cycle_time = datetime(
                    cycle_date.year,
                    cycle_date.month,
                    cycle_date.day,
                    cycle_hour,
                    tzinfo=UTC,
                )

                if cycle_time <= cutoff:
                    available_cycles.append(cycle_time)

        if not available_cycles:
            logger.warning(
                "No eligible ECMWF cycle found before %s",
                cutoff.isoformat(),
            )
            return {
                "discovered": 0,
                "selected": 0,
                "uploaded": 0,
                "failed": 0,
                "finished_at_epoch": int(datetime.now(UTC).timestamp()),
            }

        selected_cycle = max(available_cycles)
        date_text = selected_cycle.strftime("%Y%m%d")
        cycle_text = selected_cycle.strftime("%H") + "z"

        logger.info(
            "Selected ECMWF cycle: date=%s cycle=%s "
            "(cycle time: %s, readiness cutoff: %s)",
            date_text,
            cycle_text,
            selected_cycle.isoformat(),
            cutoff.isoformat(),
        )

        candidates: list[str] = []

        # Скачиваем все GRIB-файлы только из выбранного цикла.
        for stream in self.settings.target_folders:
            directory = (
                f"{self.settings.normalized_base_url}/"
                f"{date_text}/{cycle_text}/{self.settings.model}/"
                f"{self.settings.resolution}/{stream}/"
            )

            try:
                urls = await self.list_grib_files(directory)
                candidates.extend(urls)

                logger.info(
                    "Discovered %d GRIB files for stream=%s, cycle=%s",
                    len(urls),
                    stream,
                    cycle_text,
                )

            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    logger.warning(
                        "ECMWF directory not published: %s",
                        directory,
                    )
                else:
                    logger.exception(
                        "Could not list ECMWF directory: %s",
                        directory,
                    )

            except Exception:
                logger.exception(
                    "Could not list ECMWF directory: %s",
                    directory,
                )

        candidates = sorted(set(candidates))

        logger.info(
            "Discovery finished: %d candidate files for cycle %s/%s",
            len(candidates),
            date_text,
            cycle_text,
        )

        results = await asyncio.gather(
            *(self._download_one(url) for url in candidates),
            return_exceptions=True,
        )

        uploaded = sum(result is True for result in results)
        failed = sum(
            isinstance(result, BaseException)
            for result in results
        )

        return {
            "discovered": len(candidates),
            "selected": len(candidates),
            "uploaded": uploaded,
            "failed": failed,
            "finished_at_epoch": int(datetime.now(UTC).timestamp()),
        }
