import os
import json
import sqlite3
from fastapi import FastAPI, HTTPException, Query
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

app = FastAPI()

DB_NAME = "./bot_database.db"
SQL_DUMP_FILE = "bot_database_dump.sql"
LOCAL_DOWNLOAD_FILE = "bot_downloaded_dump.sql"

# Функция для создания временного файла credentials.json из переменных окружения
@app.post("/create_first_crecredentials/")
def create_first_crecredentials():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Это откроет браузер для авторизации
    gauth.SaveCredentialsFile("credentials.json")  # Сохраняем токен для дальнейшего использования

# Функция для создания временного файла credentials.json из переменных окружения
def create_credentials_file():
    credentials = {
        'client_id': os.getenv('CLIENT_ID'),
        'client_secret': os.getenv('CLIENT_SECRET'),
        'access_token': os.getenv('ACCESS_TOKEN'),
        'refresh_token': os.getenv('REFRESH_TOKEN'),
    }
    with open('credentials.json', 'w') as f:
        json.dump(credentials, f)

# Функция для авторизации в Google
def authenticate_drive():
    """
    Настроенная авторизация через PyDrive2 с использованием сохранённых токенов.
    При необходимости обновляется токен доступа.
    """
    gauth = GoogleAuth()

    # Создаём временный файл credentials.json из переменных окружения
    create_credentials_file()

    # Загружаем сохранённые учетные данные
    gauth.LoadCredentialsFile("credentials.json")

    # Удаляем временный файл credentials.json
    os.remove('credentials.json')

    if gauth.access_token_expired:
        # Если токен устарел, обновляем его
        gauth.Refresh()

    return GoogleDrive(gauth)

# Эндпоинт для экспорта базы данных в SQL файл
@app.post("/export-db/")
async def export_db():
    """
    Экспортирует SQLite базу данных в SQL файл.
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        with open(SQL_DUMP_FILE, "w", encoding="utf-8") as f:
            for line in conn.iterdump():
                f.write(f"{line}\n")
        conn.close()
        return {"message": f"База данных сохранена в файл {SQL_DUMP_FILE}."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при экспорте базы данных: {str(e)}")

# Эндпоинт для загрузки файла в Google Drive
@app.post("/upload-to-drive/")
async def upload_to_drive(folder_id: str = Query(None, description="ID папки Google Drive (опционально)")):
    """
    Загружает SQL файл в Google Drive.
    """
    try:
        if not os.path.exists(SQL_DUMP_FILE):
            raise HTTPException(status_code=400, detail="Файл SQL дампа не найден. Сначала выполните экспорт базы данных.")

        drive = authenticate_drive()

        # Создаём файл в Google Drive
        file_metadata = {'parents': [{'id': folder_id}]} if folder_id else {}
        file = drive.CreateFile(file_metadata)
        file.SetContentFile(SQL_DUMP_FILE)  # Указываем путь к SQL дампу
        file.Upload()

        return {
            "message": f"Файл {SQL_DUMP_FILE} загружен в Google Drive.",
            "file_id": file["id"],
            "file_link": f"https://drive.google.com/file/d/{file['id']}/view?usp=sharing",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при загрузке файла в Google Drive: {str(e)}")

# Эндпоинт для скачивания файла с Google Drive и восстановления базы данных
@app.post("/import-from-drive/")
async def import_from_drive(file_id: str):
    """
    Скачивает SQL файл из Google Drive и восстанавливает базу данных.
    """
    try:
        drive = authenticate_drive()

        # Находим файл в Google Drive по ID
        file = drive.CreateFile({'id': file_id})
        file.GetContentFile(LOCAL_DOWNLOAD_FILE)  # Скачиваем файл

        # Восстанавливаем базу данных из SQL дампа
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        with open(LOCAL_DOWNLOAD_FILE, "r", encoding="utf-8") as f:
            sql_script = f.read()
            cursor.executescript(sql_script)
        conn.commit()
        conn.close()

        # Удаляем локальный файл после восстановления базы
        os.remove(LOCAL_DOWNLOAD_FILE)

        return {"message": "База данных успешно восстановлена из SQL дампа."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при импорте базы данных: {str(e)}")
