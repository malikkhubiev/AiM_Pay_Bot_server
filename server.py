from fastapi.responses import JSONResponse 
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # Используем AsyncIOScheduler
from config import PORT
import uvicorn
from database import (
    database,
    initialize_settings_once
)
from api.base import *
from api.kassa import *
from api.store_db import *
from utils import *

# Логируем, чтобы проверить, что переменные установлены
logger.info("Account ID: %s", Configuration.account_id)
logger.info("Secret Key: %s", "SET" if Configuration.secret_key else "NOT SET")

# Инициализация планировщика задач
scheduler = AsyncIOScheduler()

@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    # Не открываем соединение с БД на preflight запросах CORS
    if request.method == "OPTIONS":
        return await call_next(request)

    await database.connect()
    request.state.db = database
    try:
        response = await call_next(request)
    finally:
        await database.disconnect()
    return response

@app.on_event("startup")
async def initialize_services():
    # Подключение к базе данных
    await init_db()
    await initialize_settings_once()

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
