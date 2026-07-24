import os
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union
import boto3
from botocore.exceptions import ClientError

from src.config import settings

logger = logging.getLogger(__name__)

class BaseStorage(ABC):
    """Абстрактный интерфейс хранилища."""
    
    @abstractmethod
    def save(self, data: bytes, key: str) -> str:
        """Сохраняет байты по ключу (пути). Возвращает путь/ключ."""
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Проверяет, существует ли объект по ключу."""
        pass


class LocalStorage(BaseStorage):
    """Реализация для локальной файловой системы (Data Lake)."""
    
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def save(self, data: bytes, key: str) -> str:
        file_path = self.root_dir / key
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Идемпотентность: не перезаписываем, если уже есть
        if file_path.exists():
            logger.info(f"⚠️  Файл уже существует, пропускаем: {file_path}")
            return str(file_path)
            
        with open(file_path, "wb") as f:
            f.write(data)
        return str(file_path)

    def exists(self, key: str) -> bool:
        return (self.root_dir / key).exists()


class S3Storage(BaseStorage):
    """Реализация для S3-совместимого хранилища (AWS S3, MinIO, Yandex Object Storage)."""
    
    def __init__(self, bucket: str, endpoint_url: str = None):
        self.bucket = bucket
        # Если endpoint_url не передан, используется стандартный AWS S3
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )

    def save(self, data: bytes, key: str) -> str:
        if self.exists(key):
            logger.warning(f"️  Объект уже существует в S3, пропускаем: {key}")
            return f"s3://{self.bucket}/{key}"
            
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data
        )
        return f"s3://{self.bucket}/{key}"

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False


def get_storage() -> BaseStorage:
    """Возвращает экземпляр хранилища в зависимости от настроек."""
    return LocalStorage(root_dir=settings.raw_dir)