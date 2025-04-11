from loader import *
from fastapi import HTTPException
from fastapi.responses import FileResponse
import os
import httpx
import logging
from config import (
    SECRET_CODE
)
from database import (
    initialize_database,
    get_setting
)

# Папка для сохранения экспортированных файлов
EXPORT_FOLDER = 'exports'
os.makedirs(EXPORT_FOLDER, exist_ok=True)
current_dir = os.getcwd()
db_path = os.path.join(current_dir, 'bot_database.db')

async def init_db():
    """
    Импорт базы данных:
    - Если файл существует на Google Drive, он скачивается.
    - Если файла нет, создается новая база данных.
    """
    logging.info("Начинаем процесс импорта базы данных.")
    
    try:
        # Формируем URL для скачивания файла
        url = f"https://drive.google.com/uc?id={str(await get_setting('FILE_ID'))}"
        logging.info(f"Готов URL: {url}")

        # Создаём клиент с разрешением редиректов
        async with httpx.AsyncClient(follow_redirects=True) as client:
            # Отправляем запрос для скачивания файла
            download_response = await client.get(url)

        logging.info(f"response.status_code {download_response.status_code}")

        # Проверяем статус ответа
        if download_response.status_code == 200:
            logging.info("Файл найден на Google Drive, начинаем скачивание.")
            with open(db_path, "wb") as f:
                f.write(download_response.content)
            logging.info("Файл успешно скачан.")
        else:
            logging.error(f"Ошибка скачивания. Статус: {download_response.status_code}")
            # Если не удалось скачать файл, создаём базу данных

    except Exception as e:
        logging.error(f"Ошибка при импорте базы данных: {e}")

    initialize_database()

@app.post("/export_db")
async def export_db(request: Request):
    logging.info("Запрос на экспорт базы данных")
    
    try:
        data = await request.json()
        secret_code = data.get("secret_code")

        if secret_code != SECRET_CODE:
            raise HTTPException(status_code=403, detail="Неверный секретный код.")

        logging.info(f"Путь к базе данных: {db_path}")
        if not os.path.exists(db_path):
            raise HTTPException(status_code=404, detail="База данных не найдена.")

        export_path = os.path.join(EXPORT_FOLDER, 'bot_database.db')

        with open(db_path, 'rb') as f_in:
            with open(export_path, 'wb') as f_out:
                f_out.write(f_in.read())

        logging.info(f"Файл готов к отправке: {export_path}")
        return FileResponse(export_path, media_type='application/octet-stream', filename='bot_database.db')

    except Exception as e:
        logging.error(f"Ошибка экспорта: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при экспорте базы данных: {e}")