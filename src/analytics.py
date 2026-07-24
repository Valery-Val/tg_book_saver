import logging
import duckdb
from pathlib import Path
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)

DB_PATH = settings.data_dir / "processed" / "metadata.db"

def show_statistics():
    """
    Показывает статистику по скачанным книгам
    """
    if not DB_PATH.exists():
        logger.error("Database is not created yet. Run the pipeline first.")
        return
    
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    
    try:
        logger.info("\n" + "="*80)
        logger.info("СТАТИСТИКА БИБЛИОТЕКИ")
        logger.info("="*80)
        
        # General stats
        total_stats = conn.execute("""
            SELECT 
                COUNT(DISTINCT book_name) as total_books,
                COUNT(*) as total_chapters,
                SUM(file_size_bytes) as total_size_bytes,
                MIN(downloaded_at) as first_download,
                MAX(downloaded_at) as last_download
            FROM chapters
            WHERE status = 'success'
        """).fetchone()
        
        total_books, total_chapters, total_size, first_dl, last_dl = total_stats
        
        logger.info(f"Всего книг: {total_books}")
        logger.info(f"Всего глав: {total_chapters}")
        logger.info(f"Общий размер: {total_size / 1024 / 1024 / 1024:.2f} GB")
        logger.info(f"Первая загрузка: {first_dl}")
        logger.info(f"Последняя загрузка: {last_dl}")
        
        # Top-5 books by chapters number
        logger.info("\n" + "-"*80)
        logger.info("ТОП-5 КНИГ ПО КОЛИЧЕСТВУ ГЛАВ")
        logger.info("-"*80)
        
        top_books = conn.execute("""
            SELECT 
                book_name,
                total_chapters,
                first_chapter_date,
                last_chapter_date
            FROM books
            ORDER BY total_chapters DESC
            LIMIT 5
        """).fetchall()
        
        for i, (name, chapters, first, last) in enumerate(top_books, 1):
            logger.info(f"{i:2d}. {name[:50]:<50} | {chapters:3d} глав")
        
        # Stats by size
        logger.info("\n" + "-"*80)
        logger.info("РАСПРЕДЕЛЕНИЕ ПО РАЗМЕРАМ")
        logger.info("-"*80)
        
        size_stats = conn.execute("""
            SELECT 
                CASE 
                    WHEN file_size_bytes < 10 * 1024 * 1024 THEN '< 10 MB'
                    WHEN file_size_bytes < 50 * 1024 * 1024 THEN '10-50 MB'
                    WHEN file_size_bytes < 100 * 1024 * 1024 THEN '50-100 MB'
                    ELSE '> 100 MB'
                END as size_range,
                COUNT(*) as count,
                SUM(file_size_bytes) as total_size
            FROM chapters
            WHERE status = 'success'
            GROUP BY size_range
            ORDER BY MIN(file_size_bytes)
        """).fetchall()
        
        for size_range, count, total in size_stats:
            logger.info(f"{size_range:<15} | {count:4d} глав | {total / 1024 / 1024:.1f} MB")
        
        # Errors
        failed_count = conn.execute("""
            SELECT COUNT(*) FROM chapters WHERE status = 'failed'
        """).fetchone()[0]
        
        if failed_count > 0:
            logger.info(f"\n⚠️ Неудачных загрузок: {failed_count}")
        
        logger.info("\n" + "="*80 + "\n")
        
    finally:
        conn.close()


if __name__ == "__main__":
    show_statistics()