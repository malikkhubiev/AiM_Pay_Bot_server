from fastapi.responses import JSONResponse 
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # Используем AsyncIOScheduler
from config import PORT
import uvicorn
from database import database, delete_expired_records
from api.base import *
from api.docs import *
from api.kassa import *
from api.store_db import *
from utils import *

# Логируем, чтобы проверить, что переменные установлены
logger.info("Account ID: %s", Configuration.account_id)
logger.info("Secret Key: %s", "SET" if Configuration.secret_key else "NOT SET")

# Инициализация планировщика задач
scheduler = AsyncIOScheduler()

# Запускаем задачу на удаление устаревших записей каждые сутки
scheduler.add_job(delete_expired_records, 'interval', hours=24)

@app.middleware("http")
async def combined_middleware(request: Request, call_next):
    # Подключаем БД перед выполнением запроса
    await database.connect()
    request.state.db = database

    response = await call_next(request)

    # Отключаем БД после выполнения запроса
    await database.disconnect()

    # Проверяем, нужно ли удалить файл после ответа
    if "X-Remove-File" in response.headers:
        file_path = response.headers["X-Remove-File"]
        try:
            os.remove(file_path)
            logging.info(f"Файл {file_path} успешно удалён")
        except Exception:
            logging.error(f"Не удалось удалить файл: {file_path}")

    return response

@app.on_event("startup")
async def initialize_services():
    # Подключение к базе данных
    await init_db()

    # Запуск планировщика
    scheduler.start()
    logger.info("AsyncIOScheduler started.")

@app.on_event("shutdown")
async def shutdown_services():
    # Отключение базы данных
    await database.disconnect()
    
    # Остановка планировщика
    scheduler.shutdown()
    logger.info("AsyncIOScheduler shut down.")

@app.api_route("/", methods=["GET", "HEAD"])
async def super(request: Request):
    return JSONResponse(content={"message": "Супер"}, status_code=200, headers={"Content-Type": "application/json; charset=utf-8"})

# Основной запуск FastAPI
if __name__ == "__main__":
    port = int(PORT)  # Порт будет извлечен из окружения или 8000 по умолчанию
    uvicorn.run(app, host="0.0.0.0", port=port)
