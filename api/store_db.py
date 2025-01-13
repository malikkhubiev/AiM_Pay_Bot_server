import sqlite3
from loader import *
from fastapi import HTTPException
from fastapi.responses import FileResponse
import os
import httpx
from config import (
    FILE_ID,
)
import logging

# Папка для сохранения экспортированных файлов
EXPORT_FOLDER = 'exports'
os.makedirs(EXPORT_FOLDER, exist_ok=True)
destination = "exports/downloaded_file.sql"

@app.post("/download_file_from_drive")
async def download_file_from_drive(destination: str):
    logging.info(f"Берём db из драйва")
    # Формируем URL для скачивания файла
    url = f"https://drive.google.com/uc?id={FILE_ID}"

    logging.info(f"Готов url {url}")
    # Отправляем асинхронный GET-запрос с использованием httpx
    async with httpx.AsyncClient() as client:
        response = await client.get(url)

    logging.info(response)
    # Проверяем успешность запроса
    if response.status_code == 200:
        with open(destination, "wb") as f:
            f.write(response.content)
        return {"message": "Файл успешно скачан."}
    else:
        raise HTTPException(status_code=response.status_code, detail=f"Ошибка: {response.status_code}")

@app.get("/export_db")
async def export_db():
    logging.info(f"Просят db шку")
    try:
        # Путь к вашей базе данных
        db_path = '../bot_database.db'
        export_path = os.path.join(EXPORT_FOLDER, 'bot_database_dump.sql')
        logging.info(f"Взяли path {export_path}")
        # Открываем соединение с базой данных
        conn = sqlite3.connect(db_path)
        with open(export_path, 'w') as f:
            for line in conn.iterdump():
                f.write(f"{line}\n")
        conn.close()
        logging.info(f"Ждём отправки")
        return FileResponse(export_path, media_type='application/sql', filename='bot_database_dump.sql')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при экспорте базы данных: {e}")