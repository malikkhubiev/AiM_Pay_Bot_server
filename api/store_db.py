import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import os
import httpx
from config import (
    FILE_ID,
)

app = FastAPI()

# Папка для сохранения экспортированных файлов
EXPORT_FOLDER = 'exports'
os.makedirs(EXPORT_FOLDER, exist_ok=True)
destination = "exports/downloaded_file.sql"

@app.post("/download_file_from_drive")
async def download_file_from_drive(destination: str):
    # Формируем URL для скачивания файла
    url = f"https://drive.google.com/uc?id={FILE_ID}"

    # Отправляем асинхронный GET-запрос с использованием httpx
    async with httpx.AsyncClient() as client:
        response = await client.get(url)

    # Проверяем успешность запроса
    if response.status_code == 200:
        with open(destination, "wb") as f:
            f.write(response.content)
        return {"message": "Файл успешно скачан."}
    else:
        raise HTTPException(status_code=response.status_code, detail=f"Ошибка: {response.status_code}")

@app.get("/export_db")
async def export_db():
    try:
        # Путь к вашей базе данных
        db_path = '../bot_database.db'
        export_path = os.path.join(EXPORT_FOLDER, 'bot_database_dump.sql')

        # Открываем соединение с базой данных
        conn = sqlite3.connect(db_path)
        with open(export_path, 'w') as f:
            for line in conn.iterdump():
                f.write(f"{line}\n")
        conn.close()

        return FileResponse(export_path, media_type='application/sql', filename='bot_database_dump.sql')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при экспорте базы данных: {e}")