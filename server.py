from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from dotenv import load_dotenv
import requests
import datetime
from config import (
    REFERRAL_AMOUNT,
    YOOKASSA_SECRET_KEY,
    MAHIN_URL,
    SERVER_URL,
    YOOKASSA_SHOP_ID,
    PORT,
    BOT_USERNAME,
)
from yookassa import Payment, Configuration
import logging
import uvicorn
from database import User, Referral, Payout, get_db, create_payout, get_user, mark_payout_as_notified, create_referral

load_dotenv()

# Настройка идентификатора магазина и секретного ключа
Configuration.account_id = YOOKASSA_SHOP_ID
Configuration.secret_key = YOOKASSA_SECRET_KEY

# Настроим логирование
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Логируем, чтобы проверить, что переменные установлены
logger.info("Account ID: %s", Configuration.account_id)
logger.info("Secret Key: %s", "SET" if Configuration.secret_key else "NOT SET")

# FastAPI application
app = FastAPI()

class UserRegisterRequest(BaseModel):
    telegram_id: str
    username: str
    referrer_id: str

class PaymentRequest(BaseModel):
    amount: float
    description: str
    telegram_id: str

@app.post("/pay")
async def pay(request: PaymentRequest, db: Session = Depends(get_db)):
    """Инициализация платежа."""
    logger.info("Инициализация платежа для пользователя с Telegram ID: %s", request.telegram_id)

    user = get_user(db, request.telegram_id)
    if not user:
        logger.warning("Пользователь с Telegram ID %s не найден.", request.telegram_id)
        raise HTTPException(status_code=400, detail="User not found")

    # Создание платежа в YooKassa
    try:
        payment_response = await create_payment(request.amount, request.description, request.telegram_id)
        logger.info("Платеж успешно создан для пользователя с Telegram ID %s", request.telegram_id)
        return payment_response
    except HTTPException as e:
        logger.error("Ошибка при создании платежа для пользователя с Telegram ID %s: %s", request.telegram_id, e.detail)
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    
@app.post("/payment_notification")
async def payment_notification(request: Request, db: Session = Depends(get_db)):
    """Обработка уведомления о платеже от YooKassa."""
    try:
        # Логирование заголовков и тела запроса
        headers = request.headers
        body = await request.body()
        logging.info("Request headers: %s", headers)
        logging.info("Raw request body: %s", body.decode("utf-8"))

        # Попытка разобрать JSON
        try:
            data = await request.json()
            logging.info("Parsed JSON: %s", data)
        except Exception as e:
            logging.error("Failed to parse JSON: %s", e)
            raise HTTPException(status_code=400, detail="Invalid JSON format")

        # Проверка типа уведомления и извлечение объекта
        if data.get("type") != "notification" or "object" not in data:
            logging.error("Invalid notification type or missing 'object'")
            raise HTTPException(status_code=400, detail="Invalid notification structure")

        payment_data = data["object"]
        payment_id = payment_data.get("id")
        status = payment_data.get("status")
        metadata = payment_data.get("metadata", {})
        user_telegram_id = metadata.get("telegram_id")

        logging.info("Payment ID: %s, Status: %s, Telegram ID: %s", payment_id, status, user_telegram_id)

        # Логика обработки успешного платежа
        if status == "succeeded" and user_telegram_id:
            user = get_user(db, user_telegram_id)
            if user:
                # Обновление статуса оплаты пользователя
                user.payment_status = "paid"
                db.commit()

                # Выплата рефералу, если он существует
                if user.referrer_id:
                    referrer = get_user(db, user.referrer_id)
                    if referrer:
                        create_payout(db, referrer.id, REFERRAL_AMOUNT)  # Выплата за реферала

                # Создание записи в таблице Referral
                create_referral(db, user.referrer_id, user.id)

                # Обновление статуса выплаты
                mark_payout_as_notified(db, payment_id)

                # Отправка уведомления пользователю через mahin.py
                notify_url = f"{MAHIN_URL}/notify_user"
                notification_data = {
                    "telegram_id": user_telegram_id,
                    "message": "Поздравляем! Ваш платёж прошёл успешно, вы оплатили курс! 🎉"
                }
                try:
                    response = requests.post(notify_url, json=notification_data)
                    response.raise_for_status()
                    logging.info("Пользователь с Telegram ID %s успешно уведомлен через бота.", user_telegram_id)
                    return {"message": "Payment processed and user notified successfully"}
                except requests.RequestException as e:
                    logging.error("Ошибка при отправке уведомления пользователю через бота: %s", e)
                    raise HTTPException(status_code=500, detail="Failed to notify user through bot")

        raise HTTPException(status_code=400, detail="Payment not processed")
    except HTTPException as he:
        # Логирование HTTP-исключений
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        # Логирование неожиданных ошибок
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/greet")
async def greet(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    username = data.get("username")
    referrer_id = data.get("referrer_id")

    user = db.query(User).filter_by(telegram_id=telegram_id).first()

    if user:
        response_message = f"Привет, {username}! Я тебя знаю. Ты участник AiM course!"
    else:
        new_user = User(
            telegram_id=telegram_id,
            username=username,
            referrer_id=referrer_id
        )
        db.add(new_user)
        db.commit()
        response_message = f"Добро пожаловать, {username}! Ты успешно зарегистрирован."
        logging.info(f"Пользователь {username} зарегистрирован {'с реферальной ссылкой' if referrer_id else 'без реферальной ссылки'}.")

    return JSONResponse({"message": response_message})

@app.post("/check_referrals")
async def check_referrals(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    
    # Получаем пользователя по telegram_id
    user = db.query(User).filter_by(telegram_id=telegram_id).first()
    if user:
        # Проверка, есть ли рефералы для данного пользователя
        referral_exists = db.query(Referral).filter_by(referrer_id=user.id).first()
        if referral_exists:
            return {"has_referrals": True}
        else:
            return {"has_referrals": False}
    else:
        return {"has_referrals": False}

@app.post("/check_user")
async def check_user(data: dict, db: Session = Depends(get_db)):
    telegram_id = data.get("telegram_id")
    user = db.query(User).filter_by(telegram_id=telegram_id).first()
    
    if user:
        return {"user_exists": True, "user_id": user.id}
    else:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

@app.post("/create_payment")
async def create_payment(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    amount = data.get("amount")
    
    # Проверка, не существует ли уже незавершенный платеж
    existing_payout = db.query(Payout).filter_by(telegram_id=telegram_id, notified=False).first()
    if existing_payout:
        return {"error": "Пользователь уже имеет незавершенный платеж."}
    
    payment_data = {
        "amount": {
            "value": f"{amount:.2f}",
            "currency": "RUB"
        },
        "confirmation": {
            "type": "redirect",
            "return_url": f"{SERVER_URL}/success"
        },
        "capture": True,
        "description": "Оплата курса",
        "metadata": {
            "telegram_id": telegram_id
        }
    }
    
    try:
        logger.info("Отправка запроса на создание платежа для пользователя с Telegram ID: %s", telegram_id)
        payment = Payment.create(payment_data)  # Создание платежа через yookassa SDK
        confirmation_url = payment.confirmation.confirmation_url
        if confirmation_url:
            logger.info("Платеж успешно создан. Confirmation URL: %s", confirmation_url)
            return JSONResponse({"confirmation": {"confirmation_url": confirmation_url}})
        else:
            logger.error("Ошибка: Confirmation URL не найден в ответе от YooKassa.")
            raise HTTPException(status_code=400, detail="No confirmation URL found")
    except Exception as e:
        logger.error("Ошибка при создании платежа: %s", str(e))
        raise HTTPException(status_code=500, detail=f"Failed to create payment: {str(e)}")

    # Создаем запись в таблице Payout
    # Непонятно куда добавлять при подтверждении или кайдады
    # payout = Payout(user_id=user_id, amount=amount, created_at=datetime.utcnow(), notified=False)
    # db.add(payout)
    # db.commit()

@app.post("/generate_report")
async def generate_report(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    
    # Находим пользователя
    user = db.query(User).filter_by(telegram_id=telegram_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    # Подсчет рефералов для пользователя
    referral_count = db.query(Referral).filter_by(referrer_id=user.id).count()
    
    # Расчет общей суммы потенциальных выплат
    payout_per_referral = REFERRAL_AMOUNT  # значение должно быть задано где-то в server.py или в конфигурации
    total_payout = referral_count * payout_per_referral

    report = {
        "username": user.username,
        "referral_count": referral_count,
        "total_payout": total_payout
    }
    
    return JSONResponse(report)

@app.post("/get_referral_link")
async def get_referral_link(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    
    # Проверяем наличие пользователя
    user = db.query(User).filter_by(telegram_id=telegram_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Генерируем реферальную ссылку
    bot_username = BOT_USERNAME  # Укажите имя бота в настройках или конфигурации
    referral_link = f"https://t.me/{bot_username}?start={telegram_id}"
    
    return {"referral_link": referral_link}

@app.get("/success")
async def success_payment(request: Request):
    return HTMLResponse("<h1 style='text-align: center'>Платёж прошёл успешно. Вы можете возвращаться в бота</h1>")

# Database session dependency
@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    request.state.db = get_db()
    response = await call_next(request)
    return response

@app.api_route("/", methods=["GET", "HEAD"])
async def payment_notification(request: Request):
    return JSONResponse(content={"message": "Супер"}, status_code=200, headers={"Content-Type": "application/json; charset=utf-8"})

async def run_fastapi():
    port = int(PORT)  # Порт будет извлечен из окружения или 8000 по умолчанию
    uvicorn.run(app, host="0.0.0.0", port=port)