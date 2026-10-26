import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
import httpx
from fastapi import FastAPI, HTTPException
from common_stribog3.minio_client.async_minio_client import MinioData, AsyncMinioClient

from app.config import get_settings
from app.ecmwf import ECMWFDownloader
from app.logging_config import configure_logging

settings = get_settings()
configure_logging(settings.log_level)

logger = logging.getLogger(__name__)

state: dict[str, Any] = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "last_result": None,
    "last_error": None,
}

run_lock = asyncio.Lock()


async def execute_run(
    downloader: ECMWFDownloader,
) -> dict[str, int]:
    if run_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="A download run is already in progress",
        )

    async with run_lock:
        state["running"] = True
        state["last_started_at"] = datetime.now(UTC).isoformat()
        state["last_error"] = None

        try:
            result = await downloader.run_once()
            state["last_result"] = result
            return result

        except Exception as exc:
            state["last_error"] = str(exc)
            logger.exception("Download run failed")
            raise

        finally:
            state["running"] = False
            state["last_finished_at"] = datetime.now(UTC).isoformat()


@asynccontextmanager
async def lifespan(app: FastAPI):
    store_data = MinioData(
        host=settings.minio_host,
        port=settings.minio_port,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure
    )
    minio_client = AsyncMinioClient(store_data)
    # Проверяем доступность MinIO при запуске.
    if not await minio_client.exists_bucket(settings.minio_bucket):
        await minio_client.create_bucket(settings.minio_bucket)

    timeout = httpx.Timeout(settings.http_timeout_seconds)

    limits = httpx.Limits(
        max_connections=settings.max_parallel_downloads + 5,
        max_keepalive_connections=settings.max_parallel_downloads,
    )

    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
        follow_redirects=True,
        headers={
            "User-Agent": "ecmwf-minio-downloader/0.1",
        },
    ) as http:
        downloader = ECMWFDownloader(settings, minio_client, http)
        app.state.downloader = downloader

        async def scheduler() -> None:
            while True:
                try:
                    await execute_run(downloader)

                except asyncio.CancelledError:
                    raise

                except HTTPException as exc:
                    if exc.status_code != 409:
                        logger.exception("Scheduled run failed")

                except Exception:
                    logger.exception("Scheduled run failed")

                await asyncio.sleep(settings.poll_interval_seconds)

        task = asyncio.create_task(
            scheduler(),
            name="ecmwf-scheduler",
        )

        try:
            yield

        finally:
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass


app = FastAPI(
    title="ECMWF GRIB2 to MinIO",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/status")
async def status() -> dict[str, Any]:
    return state.copy()


@app.post("/run")
async def run_now() -> dict[str, int]:
    downloader = getattr(app.state, "downloader", None)

    if downloader is None:
        raise HTTPException(
            status_code=503,
            detail="Downloader is not initialized",
        )

    return await execute_run(downloader)
