import asyncio
import logging
import random
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List

from prefect import task, get_run_logger
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError

from src.config import settings
from src.extractor import ChapterMetadata
from src.storage import get_storage
from src.database import (
    init_database, 
    is_chapter_downloaded, 
    save_chapter_record, 
    ChapterRecord
)

logger = logging.getLogger(__name__)

CONCURRENCY_LIMIT = 2
MIN_DELAY_BETWEEN_DOWNLOADS = 2
MAX_DELAY_BETWEEN_DOWNLOADS = 5


@task(
    name="download_and_save_batch",
    retries=2,
    retry_delay_seconds=60,
    log_prints=True
)
async def process_chapters_batch_task(chapters: List[ChapterMetadata]) -> int:
    """
    Обрабатывает пакет глав с проверкой идемпотентности через DuckDB.
    """
    log = get_run_logger()
    
    # Инициализируем базу данных
    init_database()
    
    log.info(f"Начало пакетной обработки {len(chapters)} глав...")
    
    session_name = "tg_session"
    client = TelegramClient(
        session_name,
        settings.telegram_api_id,
        settings.telegram_api_hash,
        device_model="Desktop",
        system_version="4.16.30-vxCUSTOM",
    )
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    counter_lock = asyncio.Lock()
    successful_count = 0
    skipped_count = 0

    async def process_single_chapter(meta: ChapterMetadata):
        nonlocal successful_count, skipped_count
        
        # ИДЕМПОТЕНТНОСТЬ: проверяем, не скачали ли уже
        if is_chapter_downloaded(meta.book_name, meta.chapter_number):
            log.info(f"⏭ Пропуск (уже скачано): {meta.book_name}, Глава {meta.chapter_number}")
            async with counter_lock:
                skipped_count += 1
            return
        
        await asyncio.sleep(random.uniform(0.5, 2.0))
        
        async with semaphore:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    log.info(f"Загрузка: {meta.book_name}, Глава {meta.chapter_number}")
                    
                    msg = await client.get_messages(settings.telegram_channel, ids=meta.message_id)
                    if not msg or not msg.media:
                        log.warning(f"⚠️ Медиа не найдено для главы {meta.chapter_number}")
                        return

                    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{meta.file_extension}") as tmp:
                        temp_path = Path(tmp.name)
                    
                    await client.download_media(msg.media, file=str(temp_path))
                    
                    storage = get_storage()
                    key = f"{meta.book_name}/{meta.chapter_number:02d}.{meta.file_extension}"
                    
                    def _save_sync():
                        with open(temp_path, "rb") as f:
                            return storage.save(f.read(), key)
                    
                    saved_path = await asyncio.to_thread(_save_sync)
                    
                    # Сохраняем метаданные в DuckDB
                    file_size = temp_path.stat().st_size
                    chapter_record = ChapterRecord(
                        book_name=meta.book_name,
                        chapter_number=meta.chapter_number,
                        file_path=saved_path,
                        file_size_bytes=file_size,
                        message_id=meta.message_id,
                        downloaded_at=datetime.utcnow(),
                        status='success'
                    )
                    save_chapter_record(chapter_record)
                    
                    log.info(f"Сохранено: {saved_path} ({file_size / 1024 / 1024:.1f} MB)")
                    
                    async with counter_lock:
                        successful_count += 1
                    
                    delay = random.uniform(MIN_DELAY_BETWEEN_DOWNLOADS, MAX_DELAY_BETWEEN_DOWNLOADS)
                    await asyncio.sleep(delay)
                    
                    return
                    
                except FloodWaitError as e:
                    wait_seconds = e.seconds + random.uniform(5, 15)
                    log.warning(f"FloodWait! Ждём {wait_seconds:.1f}с...")
                    await asyncio.sleep(wait_seconds)
                    
                except RPCError as e:
                    log.error(f"PC ошибка (попытка {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(random.uniform(10, 30))
                        
                except Exception as e:
                    log.error(f"Ошибка главы {meta.chapter_number}: {e}")
                    
                    # Сохраняем запись о неудаче
                    try:
                        error_record = ChapterRecord(
                            book_name=meta.book_name,
                            chapter_number=meta.chapter_number,
                            file_path="",
                            file_size_bytes=0,
                            message_id=meta.message_id,
                            downloaded_at=datetime.utcnow(),
                            status='failed'
                        )
                        save_chapter_record(error_record)
                    except Exception as db_err:
                        log.error(f"Не удалось сохранить ошибку в БД: {db_err}")
                    
                    return
                    
                finally:
                    if 'temp_path' in locals() and temp_path.exists():
                        temp_path.unlink(missing_ok=True)

    try:
        await client.start(phone=settings.telegram_phone)
        log.info("Клиент Telegram успешно инициализирован.")
        
        tasks = [process_single_chapter(meta) for meta in chapters]
        await asyncio.gather(*tasks)
        
    finally:
        await client.disconnect()
        log.info(f"Пайплайн завершён! Успешно: {successful_count} | Пропущено: {skipped_count} | Всего: {len(chapters)}")
        
    return successful_count