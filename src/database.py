import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
from pydantic import BaseModel, ConfigDict

from src.config import settings

logger = logging.getLogger(__name__)

# Путь к базе данных
DB_PATH = settings.data_dir / "processed" / "metadata.db"


class BookMetadata(BaseModel):
    """Модель для таблицы books"""
    model_config = ConfigDict(frozen=True)
    
    book_name: str
    total_chapters: int
    first_chapter_date: Optional[datetime] = None
    last_chapter_date: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class ChapterRecord(BaseModel):
    """Модель для таблицы chapters"""
    model_config = ConfigDict(frozen=True)
    
    book_name: str
    chapter_number: int
    file_path: str
    file_size_bytes: int
    duration_seconds: Optional[int] = None  # Можно парсить из ID3 тегов позже
    message_id: int
    downloaded_at: datetime
    status: str  # 'success', 'failed', 'skipped'


def init_database():
    """
    Инициализирует базу данных и создаёт таблицы, если их нет.
    Вызывается при старте пайплайна.
    """
    # Создаём директорию, если её нет
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(str(DB_PATH))
    
    try:
        # Таблица книг (агрегированная информация)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS books (
                book_name VARCHAR PRIMARY KEY,
                total_chapters INTEGER DEFAULT 0,
                first_chapter_date TIMESTAMP,
                last_chapter_date TIMESTAMP,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """)
        
        # Таблица глав (детальная информация)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chapters (
                book_name VARCHAR,
                chapter_number INTEGER,
                file_path VARCHAR NOT NULL,
                file_size_bytes INTEGER NOT NULL,
                duration_seconds INTEGER,
                message_id INTEGER NOT NULL,
                downloaded_at TIMESTAMP NOT NULL,
                status VARCHAR NOT NULL,
                PRIMARY KEY (book_name, chapter_number)
            )
        """)
        
        # Индексы для быстрого поиска
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_chapters_status 
            ON chapters(status)
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_chapters_downloaded_at 
            ON chapters(downloaded_at)
        """)
        
        logger.info(f"База данных инициализирована: {DB_PATH}")
        
    finally:
        conn.close()


def is_chapter_downloaded(book_name: str, chapter_number: int) -> bool:
    """
    Проверяет, была ли глава уже успешно скачана.
    Используется для идемпотентности пайплайна.
    """
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    
    try:
        result = conn.execute("""
            SELECT COUNT(*) FROM chapters
            WHERE book_name = ? 
              AND chapter_number = ? 
              AND status = 'success'
        """, [book_name, chapter_number]).fetchone()
        
        return result[0] > 0
        
    finally:
        conn.close()


def save_chapter_record(chapter: ChapterRecord):
    """
    Сохраняет запись о скачанной главе в базу данных.
    Использует UPSERT (INSERT OR REPLACE) для идемпотентности.
    """
    conn = duckdb.connect(str(DB_PATH))
    
    try:
        conn.execute("""
            INSERT INTO chapters (
                book_name, chapter_number, file_path, file_size_bytes,
                duration_seconds, message_id, downloaded_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (book_name, chapter_number) 
            DO UPDATE SET
                file_path = excluded.file_path,
                file_size_bytes = excluded.file_size_bytes,
                downloaded_at = excluded.downloaded_at,
                status = excluded.status
        """, [
            chapter.book_name,
            chapter.chapter_number,
            chapter.file_path,
            chapter.file_size_bytes,
            chapter.duration_seconds,
            chapter.message_id,
            chapter.downloaded_at,
            chapter.status
        ])
        
        # Обновляем агрегированную информацию о книге
        _update_book_metadata(conn, chapter.book_name)
        
        conn.commit()
        logger.debug(f"Сохранена запись о главе: {chapter.book_name}, гл. {chapter.chapter_number}")
        
    finally:
        conn.close()


def _update_book_metadata(conn: duckdb.DuckDBPyConnection, book_name: str):
    """
    Обновляет агрегированную информацию о книге на основе записей о главах.
    """
    now = datetime.utcnow()
    
    # Получаем статистику по книге
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_chapters,
            MIN(downloaded_at) as first_chapter_date,
            MAX(downloaded_at) as last_chapter_date
        FROM chapters
        WHERE book_name = ? AND status = 'success'
    """, [book_name]).fetchone()
    
    total_chapters, first_date, last_date = stats
    
    # UPSERT в таблицу books
    conn.execute("""
        INSERT INTO books (
            book_name, total_chapters, first_chapter_date, 
            last_chapter_date, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (book_name) 
        DO UPDATE SET
            total_chapters = excluded.total_chapters,
            first_chapter_date = excluded.first_chapter_date,
            last_chapter_date = excluded.last_chapter_date,
            updated_at = excluded.updated_at
    """, [book_name, total_chapters, first_date, last_date, now, now])


def get_books_summary() -> list[dict]:
    """
    Возвращает краткую информацию о всех книгах для аналитики.
    """
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    
    try:
        result = conn.execute("""
            SELECT 
                book_name,
                total_chapters,
                first_chapter_date,
                last_chapter_date,
                created_at,
                updated_at
            FROM books
            ORDER BY updated_at DESC
        """).fetchall()
        
        columns = ['book_name', 'total_chapters', 'first_chapter_date', 
                   'last_chapter_date', 'created_at', 'updated_at']
        
        return [dict(zip(columns, row)) for row in result]
        
    finally:
        conn.close()