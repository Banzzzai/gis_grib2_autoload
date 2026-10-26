import asyncio
from minio import Minio
from minio.error import S3Error

from app.config import Settings


class MinioStore:
    def __init__(self, settings: Settings) -> None:
        self.bucket = settings.minio_bucket

        self.client = Minio(
            settings.minio_host,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
            region=settings.minio_region,
        )

    async def ensure_bucket(self) -> None:
        exists = await asyncio.to_thread(
            self.client.bucket_exists,
            self.bucket,
        )

        if not exists:
            await asyncio.to_thread(
                self.client.make_bucket,
                self.bucket,
            )

    async def object_exists(self, key: str) -> bool:
        try:
            await asyncio.to_thread(
                self.client.stat_object,
                self.bucket,
                key,
            )
            return True

        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NotFound"}:
                return False

            # Ошибки авторизации, подключения и конфигурации
            # не должны маскироваться под отсутствие объекта.
            raise

    async def upload_file(self, key: str, path) -> None:
        await asyncio.to_thread(
            self.client.fput_object,
            self.bucket,
            key,
            str(path),
            content_type="application/octet-stream",
        )