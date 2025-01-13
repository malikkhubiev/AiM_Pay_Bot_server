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
    logging.info("Просят скачать db файл")
    try:
        current_dir = os.getcwd()
        logging.info(f"Текущая рабочая директория: {current_dir}")
        # Путь к вашей базе данных
        db_path = os.path.join(current_dir, 'bot_database.db')
        logging.info(f"Путь к базе данных: {db_path}")
        if not os.path.exists(db_path):
            raise HTTPException(status_code=404, detail="База данных не найдена.")
        
        # Путь к файлу для скачивания
        export_path = os.path.join(EXPORT_FOLDER, 'bot_database.db')
        
        # Копируем файл в папку экспортов
        with open(db_path, 'rb') as f_in:
            with open(export_path, 'wb') as f_out:
                f_out.write(f_in.read())
        
        logging.info(f"Отправка файла: {export_path}")
        # Возвращаем файл как ответ
        return FileResponse(export_path, media_type='application/octet-stream', filename='bot_database.db')
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при экспорте базы данных: {e}")
