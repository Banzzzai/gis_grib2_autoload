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
from app.minio_store import MinioStore

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

    async def _download_one(self, source_url: str) -> bool:
        async with self.semaphore:
            key = self._object_key(source_url)

            try:
                if await self.store.exists_object(self.settings.minio_bucket, key):
                    logger.debug("Already in MinIO: %s", key)
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
                    # Повторяем именно скачивание, а не загружаем
                    # повторно весь объект в оперативную память.
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

                    await self.store.add_object(self.settings.minio_bucket, key, temp_path)

                    logger.info(
                        "Uploaded %s (%d bytes)",
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

    async def run_once(self) -> dict[str, int]:
        now = datetime.now(UTC)

        dates = [
            (now.date() - timedelta(days=offset)).strftime("%Y%m%d")
            for offset in range(self.settings.ecmwf_lookback_days)
        ]

        candidates: list[str] = []

        for date_text in dates:
            for cycle in self.settings.time_slots:
                for stream in self.settings.target_folders:
                    directory = (
                        f"{self.settings.normalized_base_url}/"
                        f"{date_text}/{cycle}/{self.settings.model}/"
                        f"{self.settings.resolution}/{stream}/"
                    )

                    try:
                        urls = await self.list_grib_files(directory)
                        candidates.extend(urls)

                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 404:
                            logger.debug(
                                "Directory not published yet: %s",
                                directory,
                            )
                        else:
                            logger.warning(
                                "Could not list %s: %s",
                                directory,
                                exc,
                            )

                    except Exception:
                        logger.exception(
                            "Could not list ECMWF directory: %s",
                            directory,
                        )

        candidates = sorted(set(candidates))

        logger.info(
            "Discovery finished: %d candidate files",
            len(candidates),
        )

        # Semaphore ограничивает число активных скачиваний.
        # Исключения отдельных файлов не отменяют обработку остальных.
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
            "uploaded": uploaded,
            "failed": failed,
            "finished_at_epoch": int(datetime.now(UTC).timestamp()),
        }
