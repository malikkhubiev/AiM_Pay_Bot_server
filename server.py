from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
import requests
import datetime
from config import (
    REFERRAL_AMOUNT,
    YOOKASSA_SECRET_KEY,
    YOOKASSA_PAYMENTS_URL,
    YOOKASSA_AGENT_ID,
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

template_env = Environment(loader=FileSystemLoader("templates"))

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
    if not(user):
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
            user = await check_user(db, user_telegram_id)
            
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
    try:
        data = await request.json()
        telegram_id = data.get("telegram_id")
        username = data.get("username")
        referrer_id = data.get("referrer_id")

        logging.info(f"Получены данные: telegram_id={telegram_id}, username={username}, referrer_id={referrer_id}")
        logging.info(f"Результат проверки параметров: {check}")
        logging.info(f"Пользователь найден: {user}")

        check = check_parameters(username=username, telegram_id=telegram_id)
        logging.info(f"check = {check}")
        if not(check["result"]):
            return check["message"]

        logging.info(f"checknuli")
        user = get_user_by_telegram_id(db, telegram_id, to_throw=False)
        logging.info(f"user есть")
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
    return HTMLResponse("<h1 style='text-align: center'>Операция прошла успешно. Вы можете возвращаться в бота</h1>")

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

        check = check_parameters(amount=amount, telegram_id=telegram_id, card_synonym=card_synonym)
        if not(check["result"]):
            return {"status": "not_ready", "reason": check["message"]}

        # Находим пользователя
        user = get_user_by_telegram_id(db, telegram_id)
        card_synonym = user.card_synonym

        # Создаём запрос на выплату
        payout_request = Payout(
            telegram_id=telegram_id, 
            amount=amount, 
            card_synonym=card_synonym, 
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

        payout = Payout.create({
            "amount": {
            "value": f"{payout_request.amount}",
            "currency": "RUB"
            },
            "payout_token": f"{payout_request.card_synonym}",
            "description": "Выплата рефералу",
            "metadata": {
                "telegramId": f"{telegram_id}"
            }
        })

    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/payout_result")
async def payout_result(request: Request, db: Session = Depends(get_db)):
    try:
        # Получаем тело запроса
        body = await request.body()
        data = await request.json()

        # Проверяем, что уведомление содержит необходимую информацию
        event_type = data.get("event")
        payout_id = data.get("object", {}).get("id")
        amount = data.get("object", {}).get("amount", {}).get("value")
        telegram_id = data.get("object", {}).get("metadata", {}).get("telegram_id")
        card_synonym = data.get("object", {}).get("payment_method", {}).get("card_synonym")  # Синоним карты

        if not (event_type and payout_id and amount and telegram_id):
            logging.error("Некорректные данные вебхука: %s", data)
            raise HTTPException(status_code=400, detail="Invalid webhook data")

        # Проверяем статус события
        if event_type == "payout.succeeded":
            user = get_user_by_telegram_id(db, telegram_id)
            if not user:
                logging.error("Пользователь с telegram_id %s не найден", telegram_id)
                raise HTTPException(status_code=404, detail="User not found")

            payout_request = db.query(Payout).filter(telegram_id == telegram_id, status="pending").first()
            if not payout_request:
                raise HTTPException(status_code=404, detail="Запрос на выплату не найден")

            logging.error("Баланс до", user.balance)
            # Уменьшаем баланс пользователя
            user.balance -= float(amount)
            payout_request.status = "completed"
            db.add(payout_request)
            db.commit()
            logging.error("Баланс после", user.balance)
            logging.error("Пользователь с telegram_id %s не найден", telegram_id)
            user.card_synonym = card_synonym

            logging.info(f"Выплата {payout_id} успешно обработана на сумму {amount} для пользователя {telegram_id}")
            return {"status": "success", "message": f"Выплата {payout_id} успешно обработана"}

        elif event_type == "payout.canceled":
            logging.warning(f"Выплата {payout_id} отменена")
            return {"status": "canceled", "message": f"Выплата {payout_id} отменена"}

        else:
            logging.error("Неизвестное событие: %s", event_type)
            raise HTTPException(status_code=400, detail="Unknown event type")

    except Exception as e:
        logging.error("Ошибка обработки вебхука: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/bind_card")
async def bind_card(request: Request, db: Session = Depends(get_db)):
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

        if not(user.paid):
            return {"status": "error", "message": "Вы не можете стать партнёром по реферальной программе, не оплатив курс"}
        
        # Рендеринг шаблона
        template = template_env.get_template("bind_card.html")
        account_id = YOOKASSA_AGENT_ID  # Укажите ваш account_id
        rendered_html = template.render(account_id=account_id)

        return HTMLResponse(content=rendered_html)

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