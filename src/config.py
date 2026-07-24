from pathlib import Path
from pydantic import Field, ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Конфигурация пайплайна.
    
    Загружается из переменных окружения и .env файла.
    frozen=True гарантирует, что конфигурация неизменяема в рантайме.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        frozen=True
    )
    
    # Telegram credentials
    telegram_api_id: int = Field(..., description="Telegram API ID")
    telegram_api_hash: str = Field(..., description="Telegram API Hash")
    telegram_phone: str = Field(..., description="Phone number for Telegram auth")
    telegram_channel: str = Field(..., description="Channel username without @")
    
    # Storage
    data_dir: Path = Field(default=Path("./data"), description="Root directory for data lake")
    
    # Pipeline behavior
    batch_size: int = Field(default=100, description="Messages per batch")
    max_concurrent_downloads: int = Field(default=5, description="Concurrent download limit")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Создаём директории при инициализации
        self.data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "raw").mkdir(exist_ok=True)
        (self.data_dir / "processed").mkdir(exist_ok=True)
    
    @property
    def raw_dir(self) -> Path:
        """Путь к сырым данным (data lake raw zone)."""
        return self.data_dir / "raw"
    
    @property
    def processed_dir(self) -> Path:
        """Путь к обработанным данным."""
        return self.data_dir / "processed"


def get_settings() -> Settings:
    """Получить экземпляр настроек (кэшируется)"""
    from functools import lru_cache
    
    @lru_cache()
    def _get_settings():
        return Settings()
    
    return _get_settings()

settings = get_settings()