import asyncio
import logging
from prefect import flow, get_run_logger

from src.extractor import extract_metadata
from src.processor import process_chapters_batch_task
from src.analytics import show_statistics

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s')


@flow(name="telegram_books_etl", log_prints=True)
async def run_etl(max_books: int = 1):
    logger = get_run_logger()
    limit_str = 'Без лимита' if max_books == 0 else max_books
    logger.info(f"Запуск ETL. Лимит книг: {limit_str}")
    
    # Сбор метаданных
    chapters_metadata = []
    async for meta in extract_metadata(max_books=max_books):
        chapters_metadata.append(meta)
        
    logger.info(f"Найдено глав для обработки: {len(chapters_metadata)}")
    
    if not chapters_metadata:
        logger.warning("⚠️ Нет данных для обработки.")
        return

    # Пакетная параллельная загрузка
    successful = await process_chapters_batch_task(chapters_metadata)
    
    logger.info(f"Пайплайн завершен! Успешно обработано: {successful}")
    
    # # ЭТАП 3: Показываем статистику
    # show_statistics()


if __name__ == "__main__":
    asyncio.run(run_etl(max_books=0))