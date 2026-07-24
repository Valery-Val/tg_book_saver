import logging
import re
from typing import AsyncGenerator

from pydantic import BaseModel, ConfigDict
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, DocumentAttributeAudio, DocumentAttributeFilename

from src.config import settings

logger = logging.getLogger(__name__)

class ChapterMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)
    book_name: str
    chapter_number: int
    file_extension: str
    message_id: int

def sanitize_book_name(name: str) -> str:
    name = name.split('\n')[0].strip()
    name = re.sub(r'[\\/:*?"<>|\n\r\t]', '_', name)
    return name[:50].strip('_')

async def extract_metadata(max_books: int = 0) -> AsyncGenerator[ChapterMetadata, None]:
    """
    Извлекает метаданные. При этом не скачивает файлы.
    """
    client = TelegramClient("tg_session_meta", settings.telegram_api_id, settings.telegram_api_hash)
    await client.start(phone=settings.telegram_phone)
    logger.info("Scanning channel for metadata...")

    current_book = "Неизвестная книга"
    chapter_num = 0
    books_with_chapters = 0

    async for msg in client.iter_messages(settings.telegram_channel, reverse=True):
        is_cover = msg.message and msg.photo
        if is_cover:
            current_book = sanitize_book_name(msg.message)
            chapter_num = 0
            continue

        if msg.media and isinstance(msg.media, MessageMediaDocument):
            is_audio = any(isinstance(attr, DocumentAttributeAudio) for attr in msg.media.document.attributes)
            
            if is_audio:
                ext = '.mp3'
                for attr in msg.media.document.attributes:
                    if isinstance(attr, DocumentAttributeFilename):
                        ext = attr.file_name.split('.')[-1].lower()
                        break
                
                if chapter_num == 0:
                    books_with_chapters += 1
                    if 0 < max_books < books_with_chapters:
                        logger.info(f"⚠️ Maximum books limit is reached ({max_books}).")
                        break
                
                chapter_num += 1
                yield ChapterMetadata(
                    book_name=current_book,
                    chapter_number=chapter_num,
                    file_extension=ext,
                    message_id=msg.id
                )

    await client.disconnect()
    logger.info(f"Number of found books: {books_with_chapters}")