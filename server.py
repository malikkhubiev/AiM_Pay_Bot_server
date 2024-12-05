from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session
from dotenv import load_dotenv
import requests
import datetime
from config import (
    REFERRAL_AMOUNT,
    YOOKASSA_SECRET_KEY,
    YOOKASSA_PAYMENTS_URL,
    QIWI_WALLET,
    QIWI_API_TOKEN,
    QIWI_API_URL,
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

# Функция для проверки обязательных параметров
def check_parameters(**kwargs):
    missing_params = [param for param, value in kwargs.items() if value is None]
    if missing_params:
        return {"result": False, "message": f"Не указаны следующие необходимые параметры: {', '.join(missing_params)}"}
    return {"result": True}

def get_user_by_telegram_id(db: Session, telegram_id: str, to_throw: bool = True):
    user = db.query(User).filter_by(telegram_id=telegram_id).first()
    if not user:
        if to_throw:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        else:
            return None
    return user

@app.post("/check_user")
async def check_user(data: dict, db: Session = Depends(get_db)):
    telegram_id = data.get("telegram_id")
    to_throw = data.get("to_throw", True)
    user = get_user_by_telegram_id(db, telegram_id, to_throw)
    return {"user_exists": True, "user": user}

@app.post("/payment_notification")
async def payment_notification(request: Request, db: Session = Depends(get_db)):
    """Обработка уведомления о платеже от Qiwi."""
    try:
        # Попытка распарсить тело запроса
        try:
            data = await request.json()
        except Exception as e:
            logging.error("Failed to parse JSON: %s", e)
            raise HTTPException(status_code=400, detail="Invalid JSON format")

        # Проверка обязательных полей в структуре уведомления
        if not data.get("status") or "data" not in data:
            logging.error("Invalid notification structure or missing 'data'")
            raise HTTPException(status_code=400, detail="Invalid notification structure")

        payment_data = data["data"]
        payment_id = payment_data.get("id")
        status = payment_data.get("status")
        user_telegram_id = payment_data.get("comment")  # Qiwi передает comment как metadata

        logging.info(f"Payment ID: {payment_id}, Status: {status}, Telegram ID: {user_telegram_id}")

        if status == "success" and user_telegram_id:
            user = await check_user(db, user_telegram_id)

            if not user["user_exists"]:
                raise HTTPException(status_code=404, detail="User not found")

            # Обновляем статус пользователя как оплаченный
            user["user"].paid = True
            db.commit()
            logging.info(f"Статус оплаты пользователя обновлен: {user_telegram_id}")

            # Уведомление пользователя
            try:
                notification_data = {
                    "telegram_id": user_telegram_id,
                    "message": "Поздравляем! Ваш платёж прошёл успешно, вы оплатили курс! 🎉"
                }
                response = requests.post(f"{MAHIN_URL}/notify_user", json=notification_data)
                response.raise_for_status()
                logging.info(f"Пользователь с Telegram ID {user_telegram_id} успешно уведомлен.")

                # После успешного уведомления обновляем статус выплаты
                mark_payout_as_notified(db, payment_id)
                return {"message": "Payment processed and user notified successfully"}

            except requests.RequestException as e:
                logging.error("Ошибка при отправке уведомления пользователю через бота: %s", e)

        raise HTTPException(status_code=400, detail="Payment not processed")
    
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/greet")
async def greet(request: Request, db: Session = Depends(get_db)):
    try:
        data = await request.json()
        telegram_id = data.get("telegram_id")
        username = data.get("username")
        referrer_id = data.get("referrer_id")

        check = check_parameters(username=username, telegram_id=telegram_id)
        if not(check["result"]):
            return check["message"]

        user = get_user_by_telegram_id(db, telegram_id, to_throw=False)

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
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/check_referrals")
async def check_referrals(request: Request, db: Session = Depends(get_db)):
    try:
        data = await request.json()
        telegram_id = data.get("telegram_id")

        check = check_parameters(telegram_id=telegram_id)
        if not(check["result"]):
            return check["message"]
        
        user = get_user_by_telegram_id(db, telegram_id)
        
        if user:
            # Проверка, есть ли рефералы для данного пользователя
            referral_exists = db.query(Referral).filter_by(referrer_id=user.id).first()
            if referral_exists:
                return {"has_referrals": True}
            else:
                return {"has_referrals": False}
        else:
            return {"has_referrals": False}
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/create_payment")
async def create_payment(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    amount = data.get("amount")

    check = check_parameters(telegram_id=telegram_id, amount=amount)
    if not(check["result"]):
        return check["message"]

    # Генерация ссылки для оплаты
    payment_link = f"https://qiwi.com/payment/form/99?amountInteger={amount}&extra[%27comment%27]={telegram_id}&extra[%27account%27]={QIWI_WALLET}&currency=643&successUrl={SERVER_URL}/success"

    try:
        logger.info("Создание ссылки на оплату для пользователя с Telegram ID: %s", telegram_id)
        return JSONResponse({"confirmation": {"confirmation_url": payment_link}})
    except Exception as e:
        logger.error("Ошибка при создании ссылки на оплату: %s", str(e))
        raise HTTPException(status_code=500, detail=f"Failed to create payment: {str(e)}")

@app.post("/generate_report")
async def generate_report(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    
    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return check["message"]
    
    # Находим пользователя
    user = get_user_by_telegram_id(db, telegram_id)

    # Подсчет рефералов для пользователя
    referral_count = db.query(Referral).filter_by(referrer_id=user.id).count()

    all_paid_money = db.query(func.sum(Payout.amount)).filter(Payout.telegram_id == telegram_id).scalar()
    
    total_payout = user.balance + all_paid_money
    current_balance = user.balance

    report = {
        "username": user.username,
        "referral_count": referral_count,
        "total_payout": total_payout,
        "current_balance": current_balance,
    }
    
    return JSONResponse(report)

@app.post("/get_referral_link")
async def get_referral_link(request: Request, db: Session = Depends(get_db)):
    try:
        data = await request.json()
        telegram_id = data.get("telegram_id")
        
        check = check_parameters(telegram_id=telegram_id)
        if not(check["result"]):
            return check["message"]
        
        # Находим пользователя
        check_user(db, telegram_id)
        
        # Генерируем реферальную ссылку
        bot_username = BOT_USERNAME  # Укажите имя бота в настройках или конфигурации
        referral_link = f"https://t.me/{bot_username}?start={telegram_id}"
        
        return {"referral_link": referral_link}
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/success")
async def success_payment(request: Request):
    return HTMLResponse("<h1 style='text-align: center'>Платёж прошёл успешно. Вы можете возвращаться в бота</h1>")

# Реферальные выплаты
@app.get("/get_balance/{telegram_id}")
async def get_balance(telegram_id: int, db: Session = Depends(get_db)):
    try:
        """
        Возвращает текущий баланс пользователя по его Telegram ID.
        """
        check = check_parameters(telegram_id=telegram_id)
        if not(check["result"]):
            return check["message"]
        
        # Находим пользователя
        user = get_user_by_telegram_id(db, telegram_id)

        return {"balance": user.balance}
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/add_payout_toDb")
async def add_payout_toDb(request: Request, db: Session = Depends(get_db)):
    try:
        """
        Добавляем в бд Payout pending выплату
        
        """
        data = await request.json()
        telegram_id = data.get("telegram_id")
        amount = data.get("amount")
        card_number = data.get("card_number")

        check = check_parameters(amount=amount, telegram_id=telegram_id, card_number=card_number)
        if not(check["result"]):
            return {"status": "not_ready", "reason": check["message"]}

        # Находим пользователя
        user = get_user_by_telegram_id(db, telegram_id)

        # Создаём запрос на выплату
        payout_request = Payout(
            telegram_id=telegram_id, 
            amount=amount, 
            card_number=card_number, 
            status="pending"
        )
        db.add(payout_request)
        db.commit()
        db.refresh(payout_request)

        return {"status": "ready_to_pay"}

    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/make_payout")
async def make_payout(request: Request, db: Session = Depends(get_db)):
    try:
        data = await request.json()
        telegram_id = data.get("telegram_id")

        # Проверка обязательных параметров
        check = check_parameters(
            telegram_id=telegram_id
        )
        if not check["result"]:
            return check["message"]

        # Находим пользователя
        user = get_user_by_telegram_id(db, telegram_id)

        payout_request = db.query(Payout).filter(telegram_id == telegram_id, status="pending").first()
        if not payout_request:
            raise HTTPException(status_code=404, detail="Запрос на выплату не найден")

        # Проверка на достаточность средств
        if payout_request.amount > user.balance:
            return {"status": "error", "message": "Недостаточно средств для выплаты."}

        # Платёжные данные для Qiwi P2P API
        payout_data = {
            "amount": payout_request.amount,
            "currency": "RUB",
            "card_number": payout_request.card_number,  # Номер карты получателя
            "comment": "Выплата за реферальную программу",
        }

        # Формируем заголовки с авторизацией
        headers = {
            "Authorization": f"Bearer {QIWI_API_TOKEN}",
            "Content-Type": "application/json",
        }

        # Отправляем POST-запрос в Qiwi API
        response = requests.post(QIWI_API_URL, json=payout_data, headers=headers)
        response_data = response.json()

        if response.status_code == 200 and response_data.get("status") == "success":
            # Если запрос успешен, обновляем баланс пользователя
            user.balance -= payout_request.amount

            payout_request.status = "completed"
            db.add(payout_request)
            db.commit()

            logging.info(f"Выплата на сумму {payout_request.amount} выполнена успешно для пользователя {telegram_id}")
            return {"status": "success", "message": f"Выплата на сумму {payout_request.amount:.2f} выполнена успешно"}

        else:
            error_message = response_data.get("error", "Неизвестная ошибка")
            logging.error(f"Ошибка при попытке выплаты через Qiwi: {error_message}")
            raise HTTPException(status_code=500, detail=f"Ошибка при попытке выплаты: {error_message}")

    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")







@app.get("/make_bal/{telegram_id}")
async def make_bal(telegram_id: int, db: Session = Depends(get_db)):

    user = await check_user(telegram_id)
    user.balance += 15000
    db.commit()

    # Обработка других статусов (например, ошибка, отмена)
    return {"message": "Баланс пополнен"}

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