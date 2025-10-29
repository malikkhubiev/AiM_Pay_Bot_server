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
    Import database:
    - If file exists on Google Drive, download it.
    - If file doesn't exist, create a new database.
    """
    logging.info("Starting database import process.")
    
    try:
        # Ensure DB schema exists before any selects
        initialize_database()

        file_id = await get_setting('FILE_ID')
        if not file_id:
            logging.info("No FILE_ID setting found, skipping import.")
            return
            
        # Form URL for file download
        url = f"https://drive.google.com/uc?id={file_id}"
        logging.info(f"URL ready: {url}")

        # Create client with redirect permission
        async with httpx.AsyncClient(follow_redirects=True) as client:
            # Send request to download file
            download_response = await client.get(url)

        logging.info(f"response.status_code {download_response.status_code}")

        # Check response status
        if download_response.status_code == 200:
            logging.info("File found on Google Drive, starting download.")
            with open(db_path, "wb") as f:
                f.write(download_response.content)
            logging.info("File successfully downloaded.")
        else:
            logging.error(f"Download error. Status: {download_response.status_code}")
            # If couldn't download file, create database
            initialize_database()

    except Exception as e:
        logging.error(f"Error during database import: {e}")
        # Initialize database if import fails
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