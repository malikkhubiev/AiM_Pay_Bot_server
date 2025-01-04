from fastapi.responses import JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler
from config import (
    PORT
)
import uvicorn
from database import (
    get_db,
    delete_expired_records,
)
from api.base import *
from api.docs import *
from api.kassa import *
from utils import *

# Логируем, чтобы проверить, что переменные установлены
logger.info("Account ID: %s", Configuration.account_id)
logger.info("Secret Key: %s", "SET" if Configuration.secret_key else "NOT SET")

# Инициализация планировщика задач
scheduler = BackgroundScheduler()
scheduler.start()

# Запускаем задачу каждую ночь
scheduler.add_job(delete_expired_records, 'interval', hours=24)

# Database session dependency
@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    request.state.db = get_db()
    response = await call_next(request)
    return response

@app.api_route("/", methods=["GET", "HEAD"])
async def super(request: Request):
    return JSONResponse(content={"message": "Супер"}, status_code=200, headers={"Content-Type": "application/json; charset=utf-8"})

async def run_fastapi():
    port = int(PORT)  # Порт будет извлечен из окружения или 8000 по умолчанию
    uvicorn.run(app, host="0.0.0.0", port=port)