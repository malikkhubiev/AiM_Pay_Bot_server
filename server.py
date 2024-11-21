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
from yookassa import Payment, Configuration, PayoutRequest
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

@app.post("/payout_to_referral")
async def payout_to_referral(request: Request):
    """Выплата рефералу через YooKassa."""
    data = await request.json()
    referrer_account_token = data.get("account_token")  # Токен, который был получен от реферала
    amount = data.get("amount")
    
    if not referrer_account_token or not amount:
        raise HTTPException(status_code=400, detail="Не указаны необходимые параметры")

    payout_data = {
        "amount": {
            "value": f"{amount:.2f}",
            "currency": "RUB"
        },
        "payout_destination_data": {
            "type": "card",  # Или используйте тип для карты
            "account_token": referrer_account_token  # Используем токен, а не номер карты
        },
        "description": "Выплата за реферала"
    }

    try:
        # Отправка запроса на выплату через YooKassa
        payout = Payment.create(payout_data)  # Используем API YooKassa для создания выплаты
        logging.info(f"YooKassa payout created: {payout}")
        return {"message": "Payout initiated successfully", "payout_details": payout}
    except Exception as e:
        logging.error(f"Ошибка при выплате реферралу через YooKassa: {e}")
        raise HTTPException(status_code=500, detail="Failed to process payout")

@app.post("/payment_notification")
async def payment_notification(request: Request, db: Session = Depends(get_db)):
    """Обработка уведомления о платеже от YooKassa."""
    try:
        headers = request.headers
        body = await request.body()
        logging.info("Request headers: %s", headers)
        logging.info("Raw request body: %s", body.decode("utf-8"))

        try:
            data = await request.json()
            logging.info("Parsed JSON: %s", data)
        except Exception as e:
            logging.error("Failed to parse JSON: %s", e)
            raise HTTPException(status_code=400, detail="Invalid JSON format")

        if data.get("type") != "notification" or "object" not in data:
            logging.error("Invalid notification type or missing 'object'")
            raise HTTPException(status_code=400, detail="Invalid notification structure")

        payment_data = data["object"]
        payment_id = payment_data.get("id")
        status = payment_data.get("status")
        metadata = payment_data.get("metadata", {})
        user_telegram_id = metadata.get("telegram_id")

        logging.info("Payment ID: %s, Status: %s, Telegram ID: %s", payment_id, status, user_telegram_id)

        if status == "succeeded" and user_telegram_id:
            user = get_user(db, user_telegram_id)
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # Обновляем статус пользователя как оплаченный
            user.paid = True
            db.commit()
            logging.info("Статус оплаты пользователя обновлен: %s", user_telegram_id)

            # Уведомление пользователя
            notify_url = f"{MAHIN_URL}/notify_user"
            notification_data = {
                "telegram_id": user_telegram_id,
                "message": "Поздравляем! Ваш платёж прошёл успешно, вы оплатили курс! 🎉"
            }
            try:
                response = requests.post(notify_url, json=notification_data)
                response.raise_for_status()
                logging.info("Пользователь с Telegram ID %s успешно уведомлен через бота.", user_telegram_id)

                # После успешного уведомления обновляем статус выплаты
                mark_payout_as_notified(db, payment_id)
                return {"message": "Payment processed and user notified successfully"}
            except requests.RequestException as e:
                logging.error("Ошибка при отправке уведомления пользователю через бота: %s", e)
                raise HTTPException(status_code=500, detail="Failed to notify user through bot")

        raise HTTPException(status_code=400, detail="Payment not processed")
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
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

# Реферальные выплаты
@app.get("/get_balance/{telegram_id}")
async def get_balance(telegram_id: int, db: Session = Depends(get_db)):
    """
    Возвращает текущий баланс пользователя по его Telegram ID.
    """
    user = db.query(User).filter(User.telegram_id == telegram_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    return {"balance": user.balance}

@app.post("/create_payout_request") # Добавляем в Payout запрос на выплату
async def create_payout_request(request: Request, db: Session = Depends(get_db)):
    """
    Делаем запрос на выплату
    
    """
    data = await request.json()
    telegram_id = data.get("telegram_id")
    amount = data.get("amount")

    if not telegram_id or not amount:
        raise HTTPException(status_code=400, detail="Не переданы обязательные параметры")

    # Проверяем пользователя
    user = db.query(User).filter(User.telegram_id == telegram_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    # Создаём запрос на выплату
    payout_request = Payout(telegram_id=telegram_id, amount=amount, status="pending")
    db.add(payout_request)
    db.commit()
    db.refresh(payout_request)

    # Проверяем, есть ли карта
    if user.account_token:
        return {"status": "ready_to_pay"}
    else:
        return {"status": "awaiting_card", "payout_request_id": payout_request.id}

@app.post("/make_payout")
async def make_payout(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    amount = data.get("amount")

    if not telegram_id or not amount:
        raise HTTPException(status_code=400, detail="Не переданы обязательные параметры")

    # Проверяем пользователя
    user = db.query(User).filter(User.telegram_id == telegram_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    # Проверяем баланс
    if amount > user.balance:
        return {"status": "error", "message": "Недостаточно средств для выплаты."}

    try:
        # Если карта привязана, сразу выполняем выплату
        if user.account_token:
            payout_data = {
                "amount": {
                    "value": f"{amount:.2f}",
                    "currency": "RUB"
                },
                "payout_destination_data": {
                    "type": "bank_card",
                    "card": {
                        "token": user.account_token
                    }
                },
                "description": "Выплата за реферальную программу"
            }

            payout = PayoutRequest(payout_data)
            response = Payment.create(payout)

            if response.status == "succeeded":
                user.balance -= amount
                db.commit()
                return {"status": "success", "message": f"Выплата на сумму {amount:.2f} выполнена успешно"}
            else:
                return {"status": "error", "message": "Не удалось выполнить выплату"}

        else:
            # Если карта не привязана, генерируем ссылку для ввода карты на YooKassa
            payout_data = {
                "amount": {
                    "value": f"{amount:.2f}",
                    "currency": "RUB"
                },
                "capture_mode": "AUTOMATIC",
                "description": "Выплата за реферальную программу",
                "payer": {
                    "type": "individual",
                    "telegram_id": user.telegram_id  # передаем telegram_id
                }
            }

            headers = {
                "Authorization": f"Bearer {YOOKASSA_SECRET_KEY}",
                "Content-Type": "application/json"
            }

            # Делаем запрос в YooKassa для создания платежа
            response = requests.post("https://api.yookassa.ru/v3/payments", json=payout_data, headers=headers)

            if response.status_code == 200:
                payment_data = response.json()
                payment_url = payment_data["confirmation"]["confirmation_url"]

                # Сохраняем статус запроса на выплату
                payout_request = Payout(telegram_id=user.telegram_id, amount=amount, status="awaiting_card")
                db.add(payout_request)
                db.commit()

                return {"status": "awaiting_card", "payment_url": payment_url}

            else:
                raise HTTPException(status_code=500, detail="Ошибка при создании запроса на выплату")

    except Exception as e:
        logging.error(f"Ошибка при создании запроса на выплату: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при создании запроса на выплату: {e}")

@app.post("/process_successful_payout")
async def process_successful_payout(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    payment_id = data.get("payment_id")  # ID платежа от YooKassa
    payout_request_id = data.get("payout_request_id")  # ID запроса на выплату, который мы генерировали
    transaction_status = data.get("status")  # Статус выплаты (например, "succeeded")

    if not payment_id or not payout_request_id or not transaction_status:
        raise HTTPException(status_code=400, detail="Недостаточно данных для обработки выплаты")

    # Получаем запись выплаты по payout_request_id
    payout_request = db.query(Payout).filter(Payout.id == payout_request_id).first()
    if not payout_request:
        raise HTTPException(status_code=404, detail="Запрос на выплату не найден")

    # Проверяем, что статус выплаты успешный
    if transaction_status != "succeeded":
        return {"status": "error", "message": "Выплата не прошла успешно"}

    # Получаем пользователя, связанного с этим запросом на выплату
    user = db.query(User).filter(User.telegram_id == payout_request.telegram_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    # Обновляем статус выплаты и записываем транзакционный ID
    payout_request.status = "completed"
    payout_request.transaction_id = payment_id  # Сохраняем ID транзакции

    # Уменьшаем баланс пользователя
    user.balance -= payout_request.amount

    # Если карта была введена, сохраняем токен карты
    if data.get("card_token"):  # Токен карты, который возвращается после ввода карты пользователем
        user.account_token = data["card_token"]

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка при обновлении данных: {e}")

    return {"status": "success", "message": f"Выплата на сумму {payout_request.amount:.2f} выполнена успешно"}


@app.post("/webhook_yookassa")
async def webhook_yookassa(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    
    # Здесь происходит обработка данных, переданных в webhook
    payment_id = data.get("payment_id")
    status = data.get("status")
    payout_request_id = data.get("payout_request_id")

    if status == "succeeded":
        # Обработка успешной выплаты
        return await process_successful_payout(request)
    
    # Обработка других статусов (например, ошибка, отмена)
    return {"status": "error", "message": "Статус выплаты не успешен"}















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