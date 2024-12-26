from fastapi import FastAPI, HTTPException, Request, Response, Depends, status
from fastapi.responses import JSONResponse, HTMLResponse
import ipaddress
from pydantic import BaseModel
from sqlalchemy import func, and_
from sqlalchemy.orm import Session, joinedload, aliased
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from time import time
from jinja2 import Environment, FileSystemLoader
from sqlalchemy.exc import IntegrityError
import requests
import datetime
from config import (
    COURSE_AMOUNT,
    REFERRAL_AMOUNT,
    YOOKASSA_SECRET_KEY,
    YOOKASSA_PAYOUT_KEY,
    YOOKASSA_PAYMENTS_URL,
    YOOKASSA_AGENT_ID,
    MAHIN_URL,
    SERVER_URL,
    YOOKASSA_SHOP_ID,
    PORT,
    BOT_USERNAME,
    SECRET_KEY,
    SECRET_CODE,
    DISABLE_SECRET_CODE_CHECK
)
from yookassa import Payout as YooPay, Payment, Configuration
import logging
import uvicorn
from database import (
    Binding,
    TempUser,
    User,
    Payment as PaymentTable,
    Referral,
    Payout,
    get_db,
    delete_expired_records,
    mark_payout_as_notified,
    get_temp_user,
    update_temp_user,
    create_temp_user
)
from starlette.middleware.cors import CORSMiddleware

# Разрешённые диапазоны и адреса для Yookassa
ALLOWED_YOOKASSA_IP_RANGES = [
    ipaddress.IPv4Network("185.71.76.0/27"),
    ipaddress.IPv4Network("185.71.77.0/27"),
    ipaddress.IPv4Network("77.75.153.0/25"),
    ipaddress.IPv4Address("77.75.156.11"),
    ipaddress.IPv4Address("77.75.156.35"),
    ipaddress.IPv4Network("77.75.154.128/25"),
    ipaddress.IPv6Network("2a02:5180::/32")
]

# Проверка IP-адреса для Yookassa
def check_yookassa_ip(request: Request):
    try:
        client_ip = request.client.host  # Получаем IP клиента
        client_ip_address = ipaddress.ip_address(client_ip)  # Преобразуем в объект IP

        # Проверяем, входит ли IP-адрес в разрешённые диапазоны
        for allowed_ip_range in ALLOWED_YOOKASSA_IP_RANGES:
            if isinstance(allowed_ip_range, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
                if client_ip_address in allowed_ip_range:
                    return client_ip  # Возвращаем IP, если он валиден
            elif isinstance(allowed_ip_range, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
                if client_ip_address == allowed_ip_range:
                    return client_ip  # Возвращаем IP, если он валиден

        # Если IP не разрешён, выбрасываем исключение
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Forbidden: Invalid IP address {client_ip}"
        )

    except ValueError:
        # Обработка случая, если IP некорректен
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Bad Request: Invalid IP address format {request.client.host}"
        )

def verify_secret_code(request: Request):
    if DISABLE_SECRET_CODE_CHECK == "True":
        return True
    else:
        secret_code = request.headers.get("X-Secret-Code")
        if secret_code != SECRET_CODE:
            raise HTTPException(status_code=403, detail="Forbidden: Invalid secret code")
        return True

load_dotenv()

def switch_configuration(account_id, secret_key):
    logging.info(f"{account_id}, {secret_key}")
    Configuration.configure(account_id, secret_key)

def setup_payment_config():
    """Настроить конфигурацию для платежей."""
    switch_configuration(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY)

def setup_payout_config():
    """Настроить конфигурацию для выплат."""
    switch_configuration(YOOKASSA_AGENT_ID, YOOKASSA_PAYOUT_KEY)

# Настроим логирование
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

template_env = Environment(loader=FileSystemLoader("templates"))

# Логируем, чтобы проверить, что переменные установлены
logger.info("Account ID: %s", Configuration.account_id)
logger.info("Secret Key: %s", "SET" if Configuration.secret_key else "NOT SET")

# FastAPI application
app = FastAPI()

# Инициализация планировщика задач
scheduler = BackgroundScheduler()
scheduler.start()

# Запускаем задачу каждую ночь
scheduler.add_job(delete_expired_records, 'interval', hours=24)

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
    logging.info(f"in get_user_by_telegram_id telegram_id = {telegram_id}")
    user = db.query(User).filter_by(telegram_id=telegram_id).first()
    if not(user):
        if to_throw:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        else:
            return None
    return user

@app.post("/check_user")
async def check_user(request: Request, db: Session = Depends(get_db)):
    try:
        verify_secret_code(request)
        data = await request.json()
        telegram_id = data.get("telegram_id")
        to_throw = data.get("to_throw", True)
        user = get_user_by_telegram_id(db, telegram_id, to_throw)
        return {"user_exists": True, "user": user}
    except HTTPException as e:
        logging.error(f"HTTP error: {e.detail}")
        return JSONResponse(status_code=e.status_code, content={"error": e.detail})
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return JSONResponse(status_code=500, content={"error": "Internal server error"})

@app.post("/payment_notification")
async def payment_notification(request: Request, db: Session = Depends(get_db)):
    """Обработка уведомления о платеже от YooKassa."""
    try:
        # Проверяем IP для уведомлений от Yookassa
        check_yookassa_ip(request)
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
        income_amount = payment_data.get("income_amount")["value"]
        metadata = payment_data.get("metadata", {})
        user_telegram_id = metadata.get("telegram_id")

        logging.info(payment_data)
        logging.info("Payment ID: %s, Status: %s, Telegram ID: %s", payment_id, status, user_telegram_id)

        if status == "succeeded" and user_telegram_id:
            logging.info(f"status {status}, и мы внутри")
            user = get_user_by_telegram_id(db, user_telegram_id)
            logging.info(f"юзера тоже получили {user.paid}")
            # Обновляем статус пользователя как оплаченный
            payment = db.query(PaymentTable).filter_by(transaction_id=payment_id).first()
            if not(payment):
                logging.info(f"Такой платёж мы видим в первый раз и это хорошо")
                try:
                    new_payment = PaymentTable(
                        transaction_id=payment_id,
                        telegram_id=user_telegram_id
                    )
                    db.add(new_payment)
                except IntegrityError:
                    db.rollback()
                    logging.info("Ошибка при добавлении платежа в базу данных")
                user.paid = True
                referrer = db.query(Referral).filter_by(referred_id=user.telegram_id).first()
                if referrer:
                    logging.info(f"referrer {referrer} есть")
                    referrer_user = get_user_by_telegram_id(db, referrer.referrer_id, to_throw=False)
                    logging.info(f"referrer_user {referrer_user}")
                    if referrer_user and referrer_user.card_synonym:
                        logging.info(f"referrer_user есть")
                        payout = YooPay.create({
                            "amount": {
                                "value": f"{REFERRAL_AMOUNT}",
                                "currency": "RUB"
                            },
                            "payout_token": f"{referrer.card_synonym}",
                            "description": "Выплата рефералу",
                            "metadata": {
                                "telegramId": f"{referrer.telegram_id}"
                            }
                        })

                db.commit()
                logging.info("Статус оплаты пользователя обновлен: %s", user_telegram_id)
                notification_data = {"telegram_id": user_telegram_id}
                try:
                    # После успешного уведомления обновляем статус выплаты
                    send_invite_link_url = f"{MAHIN_URL}/send_invite_link"
                    response = requests.post(send_invite_link_url, json=notification_data)
                    response.raise_for_status()
                    mark_payout_as_notified(db, payment_id)
                    logging.info("Пользователь с Telegram ID %s успешно уведомлен через бота.", user_telegram_id)
                    return JSONResponse(status_code=200)
                except requests.RequestException as e:
                    logging.error("Ошибка при отправке уведомления пользователю через бота: %s", e)
                    raise HTTPException(status_code=500, detail="Failed to notify user through bot")

        raise HTTPException(status_code=400, detail="Payment not processed")
    except HTTPException as e:
        # Обработка исключения с возвращением пользовательского сообщения
        return {"error": e.detail, "status_code": e.status_code}
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/start")
async def start(request: Request, db: Session = Depends(get_db)):
    try:
        verify_secret_code(request)
        data = await request.json()
        telegram_id = data.get("telegram_id")
        username = data.get("username")
        referrer_id = data.get('referrer_id')

        logging.info(f"Есть telegram_id {telegram_id}")
        logging.info(f"Есть username {username}")
        logging.info(f"Есть referrer_id {referrer_id}")

        check = check_parameters(
            telegram_id=telegram_id,
            username=username
        )
        if not(check["result"]):
            return {"status": "error", "message": check["message"]}

        logging.info(f"Check done")
        return_data = {
            "status": "success",
            "response_message": "Привет",
            "to_show": None,
            "type": None
        }
        user = get_user_by_telegram_id(db, telegram_id, to_throw=False)
        logging.info(f"user есть {user}")
        if user:
            return_data["response_message"] = f"Привет, {user.username}! Я тебя знаю. Ты участник AiM course!"
            return_data["type"] = "user"
            logging.info(f"user есть")
            if not(user.paid):
                logging.info(f"user не платил")
                return_data["to_show"] = "pay_course"
            if referrer_id and referrer_id != telegram_id:
                logging.info(f"Есть реферрал и сам себя не привёл")
                existing_referrer = db.query(Referral).filter_by(referred_id=telegram_id).first()
                if existing_referrer:
                    logging.info(f"Реферал уже был")
                    existing_referrer.referrer_id = referrer_id
                else:
                    logging.info(f"Реферала ещё не было")
                    referrer_user = get_user_by_telegram_id(db, referrer_id, to_throw=False)
                    if referrer_user and referrer_user.paid:
                        logging.info(f"Пользователь который привёл есть и он оплатил курс")
                        new_referrer = Referral(
                            referrer_id=referrer_id,
                            referred_id=telegram_id
                        )
                        logging.info(f"Сделали реферала в бд")
                        db.add(new_referrer) 
            return JSONResponse(return_data)
        else:
            return_data["type"] = "temp_user"
            logging.info(f"Юзера нет")
            return_data["response_message"] = f"Добро пожаловать, {username}!"
            temp_user = get_temp_user(telegram_id=telegram_id)
            if temp_user:
                logging.info(f"Есть только временный юзер. Обновляем")
                update_temp_user(telegram_id=telegram_id, username=username)
            else:
                logging.info(f"Делаем временный юзер")
                create_temp_user(telegram_id=telegram_id, username=username, referrer_id=referrer_id)
            return JSONResponse(return_data)
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/getting_started")
async def getting_started(request: Request, db: Session = Depends(get_db)):
    try:
        verify_secret_code(request)
        data = await request.json()
        telegram_id = data.get("telegram_id")

        logging.info(f"Получены данные: telegram_id={telegram_id}")

        check = check_parameters(telegram_id=telegram_id)
        logging.info(f"check = {check}")
        if not(check["result"]):
            return {"status": "error", "message": check["message"]}

        logging.info(f"checknuli")
        user = get_user_by_telegram_id(db, telegram_id, to_throw=False)
        logging.info(f"user = {user}")

        temp_user = get_temp_user(telegram_id)
        logging.info(f"temp_user {temp_user}")
        if temp_user:
            logging.info(f"Есть временный юзер")
            username = temp_user.username
            referrer_id = temp_user.referrer_id
            logging.info(f"У него есть username {username}")
            logging.info(f"У него есть referrer_id {referrer_id}")
            new_user = User(
                telegram_id=telegram_id,
                username=username,
            )
            db.add(new_user)
            logging.info(f"Получены данные: telegram_id={telegram_id}, username={username}, referrer_id={referrer_id}")
            logging.info(f"Пользователь {username} зарегистрирован {'с реферальной ссылкой' if referrer_id else 'без реферальной ссылки'}.")
        
            db.commit()  # Фиксируем изменения для всех операций
            return JSONResponse({"status": "success"})
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/create_payment")
async def create_payment(request: Request, db: Session = Depends(get_db)): 
    try:
        verify_secret_code(request)
    
        data = await request.json()
        telegram_id = data.get("telegram_id")
        amount = float(COURSE_AMOUNT)
        logging.info(f"telegram_id {telegram_id}")
        logging.info(f"amount {amount}")

        check = check_parameters(telegram_id=telegram_id, amount=amount)
        if not(check["result"]):
            return {"status": "error", "message": check["message"]}
        
        logging.info(f"чекнули и делаем платёж")
        
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
            setup_payment_config()
            payment = Payment.create(payment_data)  # Создание платежа через yookassa SDK
            confirmation_url = payment.confirmation.confirmation_url
            if confirmation_url:
                logger.info("Платеж успешно создан. Confirmation URL: %s", confirmation_url)
                return JSONResponse({
                    "status": "success",
                    "confirmation": {"confirmation_url": confirmation_url}
                })
            else:
                logger.error("Ошибка: Confirmation URL не найден в ответе от YooKassa.")
                raise HTTPException(status_code=400, detail="No confirmation URL found")
        except Exception as e:
            logger.error("Ошибка при создании платежа: %s", str(e))
            raise HTTPException(status_code=500, detail=f"Failed to create payment: {str(e)}")
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        return JSONResponse({
            "status": "error",
            "message": he.detail
        })
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        return JSONResponse({
            "status": "error",
            "message": e
        })

@app.post("/generate_overview_report")
async def generate_overview_report(request: Request, db: Session = Depends(get_db)):
    try:
        verify_secret_code(request)
        data = await request.json()
        telegram_id = data.get("telegram_id")
        
        check = check_parameters(telegram_id=telegram_id)
        if not(check["result"]):
            return {"status": "error", "message": check["message"]}
        
        # Находим пользователя
        user = get_user_by_telegram_id(db, telegram_id)

        # Вывод логов, потом убрать
        r = db.query(Referral).filter_by(referrer_id=user.telegram_id).first()

        if r is None:
            logging.info(f"Реферал для пользователя с ID {user.telegram_id} не найден.")
        else:
            logging.info(f"referrer_id {r.referrer_id}")
            logging.info(f"referred_id {r.referred_id}")
            
            referred_user = get_user_by_telegram_id(db, r.referred_id)
            if referred_user:
                logging.info(f"user referred {referred_user.username}")
            else:
                logging.info(f"Пользователь с ID {r.referred_id} не найден.")

        # Calculate total paid money
        all_paid_money = db.query(func.sum(Payout.amount))\
            .filter(and_(Payout.telegram_id == telegram_id))\
            .scalar() or 0.0

        referral_count = db.query(func.count(Referral.id))\
            .filter(Referral.referrer_id == user.telegram_id).scalar()

        paid_count = db.query(func.count(Referral.id))\
            .join(User, Referral.referred_id == User.telegram_id)\
            .filter(Referral.referrer_id == user.telegram_id, User.paid == True).scalar()

        paid_percentage = (paid_count / referral_count * 100) if referral_count > 0 else 0.0

        # Generate the report
        report = {
            "username": user.username,
            "referral_count": referral_count,
            "paid_count": paid_count,
            "paid_percentage": paid_percentage,
            "total_payout": all_paid_money
        }

        return JSONResponse({
            "status": "success",
            "report": report
        })
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        return JSONResponse({
            "status": "error",
            "report": he.detail
        })
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        return JSONResponse({
            "status": "error",
            "report": e
        })

@app.post("/generate_clients_report")
async def generate_clients_report(request: Request, db: Session = Depends(get_db)):
    try:
        verify_secret_code(request)
        data = await request.json()
        telegram_id = data.get("telegram_id")
        logging.info(f"telegram_id {telegram_id}")
        
        check = check_parameters(telegram_id=telegram_id)
        if not(check["result"]):
            return {"status": "error", "message": check["message"]}
        
        logging.info(f"Чекнули")
        # Находим пользователя
        user = get_user_by_telegram_id(db, telegram_id)
        logging.info(f"user есть")

        referral_details = db.query(Referral).filter_by(referrer_id=user.telegram_id).all()

        logging.info(f"detales есть")
        logging.info(f"{referral_details} referral_details")
        # Extract referral data and calculate statistics
        invited_list = []
        logging.info(f"invited_list {invited_list}")

        if referral_details:
            logging.info(f" referral {referral_details}")
            for referral in referral_details:  # referral_details — список объектов Referral
                logging.info(f"referral есть и вот он: {referral}")
                attributes = {column.key: getattr(referral, column.key) for column in referral.__mapper__.column_attrs}
                logging.info(f"Referral attributes: {attributes}")
                referred_user = db.query(User).filter_by(telegram_id=referral.referred_id).first()
                logging.info(f"referred_user {referred_user}")
                if referred_user:
                    logging.info(f"referred_user точно есть")
                    invited_list.append({
                        "telegram_id": referred_user.telegram_id,
                        "username": referred_user.username,
                        "paid": referred_user.paid
                    })
                    logging.info(f"invited_list {invited_list}")

        logging.info(f"invited_list {invited_list} когда вышли")

        # Generate the report
        report = {
            "username": user.username,
            "invited_list": invited_list
        }

        return JSONResponse({
            "status": "success",
            "report": report
        })
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/get_referral_link")
async def get_referral_link(request: Request, db: Session = Depends(get_db)):
    try:
        verify_secret_code(request)
        data = await request.json()
        telegram_id = data.get("telegram_id")
        
        check = check_parameters(telegram_id=telegram_id)
        if not(check["result"]):
            return {"status": "error", "message": check["message"]}
        
        user = get_user_by_telegram_id(db, telegram_id)
        if not(user.paid):
            return {"status": "error", "message": "Вы не можете стать партнёром по реферальной программе, не оплатив курс"}
        
        binding = db.query(Binding).filter_by(telegram_id=user.telegram_id).first()
        logging.info(f"binding {binding}")
        if binding:
            logging.info(f"there is binding")
            referral_link = f"https://t.me/{BOT_USERNAME}?start={telegram_id}"
            return {"status": "success", "referral_link": referral_link}
        else:
            logging.info(f"no binding")
            return {"status": "error", "message": "Вы не можете стать партнёром по реферальной программе, не привязав карту"}
        
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        return JSONResponse({"status": "error", "message": he.detail})
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        return JSONResponse({"status": "error", "message": e})

@app.get("/success")
async def success_payment(request: Request):
    return HTMLResponse("<h1 style='text-align: center'>Операция прошла успешно. Вы можете возвращаться в бота</h1>")

@app.post("/payout_result")
async def payout_result(request: Request, db: Session = Depends(get_db)):
    try:
        # Проверяем IP для уведомлений от Yookassa
        check_yookassa_ip(request)
        # Получение JSON данных из запроса
        data = await request.json()
        event = data.get("event")
        object_data = data.get("object", {})
        transaction_id = object_data.get("id", {})
        metadata = object_data.get("metadata", {})

        logging.info(data)

        # Извлечение telegramId из метаданных
        telegram_id = metadata.get("telegramId")

        # Логирование события
        print(f"Получено уведомление: {event}")
        print(f"Данные объекта: {object_data}")

        # Обработка событий
        if event == "payout.succeeded":

            amount = object_data['amount']['value']

            user = get_user_by_telegram_id(db, telegram_id)
            new_payout = Payout(
                telegram_id=telegram_id,
                card_synonym=user.card_synonym,
                amount=amount,
                transaction_id=transaction_id
            )
            notify_url = f"{MAHIN_URL}/notify_user"
            notification_data = {
                "telegram_id": telegram_id,
                "message": f"Выплата на сумму {amount} произведена успешно"
            }
            try:
                response = requests.post(notify_url, json=notification_data)
                response.raise_for_status()
                logging.info("Пользователь с Telegram ID %s успешно уведомлен через бота.", telegram_id)

                # После успешного уведомления обновляем статус выплаты
                mark_payout_as_notified(db, transaction_id)
                return JSONResponse(status_code=200)
            except requests.RequestException as e:
                logging.error("Ошибка при отправке уведомления пользователю через бота: %s", e)
                raise HTTPException(status_code=500, detail="Failed to notify user through bot")
        
        elif event == "payout.canceled":
            # Выплата отменена
            print("Выплата отменена.")
            # Здесь можно обработать отмену выплаты
        else:
            # Неизвестное событие
            print(f"Неизвестное событие: {event}")
        db.commit()
        # Возвращаем подтверждение получения уведомления
        return JSONResponse(status_code=200, content={"message": "Webhook received successfully"})
    except HTTPException as e:
        # Обработка исключения с возвращением пользовательского сообщения
        return {"error": e.detail, "status_code": e.status_code}
    except Exception as e:
        # Обработка ошибок
        print(f"Ошибка при обработке вебхука: {e}")
        raise HTTPException(status_code=400, detail="Invalid webhook payload")

@app.post("/bind_card")
async def bind_card(request: Request, db: Session = Depends(get_db)):
    try:
        verify_secret_code(request)
        data = await request.json()
        telegram_id = data.get("telegram_id")

        # Проверка обязательных параметров
        check = check_parameters(
            telegram_id=telegram_id
        )
        if not check["result"]:
            return {"status": "error", "message": check["message"]}

        # Находим пользователя
        user = get_user_by_telegram_id(db, telegram_id)

        if not(user.paid):
            return {"status": "error", "message": "Вы не можете стать партнёром по реферальной программе, не оплатив курс"}

        unique_str = f"{telegram_id}{int(time() * 1000)}"

        binding = db.query(Binding).filter_by(telegram_id=telegram_id).first()
        if binding:
            db.delete(binding)
            db.commit()

        new_binding = Binding(
            telegram_id=telegram_id,
            unique_str=unique_str
        )
        db.add(new_binding)
        db.commit()

        url = f"{SERVER_URL}/bind_card_page/{unique_str}"

        return JSONResponse({"status": "success", "binding_url": url})

    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        return JSONResponse({"status": "error", "message": he.detail})
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        return JSONResponse({"status": "error", "message": e})

@app.get("/bind_card_page/{unique_str}")
def render_bind_card_page(unique_str: str):
    
    check = check_parameters(
        unique_str=unique_str
    )
    if not check["result"]:
        return {"status": "error", "message": check["message"]}

    template = template_env.get_template("bind_card.html")
    account_id = YOOKASSA_AGENT_ID
    rendered_html = template.render(account_id=account_id, unique_str=unique_str)

    return HTMLResponse(content=rendered_html)
    
@app.post("/bind_success")
async def bind_success(request: Request, db: Session = Depends(get_db)):
    try:
        verify_secret_code(request)
        data = await request.json()
        card_synonym = data.get("card_synonym")
        unique_str = data.get("unique_str")

        binding = db.query(Binding).filter(unique_str == unique_str).first()
        if not binding:
            raise HTTPException(status_code=404, detail="Запрос на привязку карты не был осуществлён")

        user = get_user_by_telegram_id(db, binding.telegram_id)
        logging.info(f"card_synonym {card_synonym}")
        user.card_synonym = card_synonym
        db.commit()

        # Уведомление пользователя
        notify_url = f"{MAHIN_URL}/notify_user"
        notification_data = {
            "telegram_id": binding.telegram_id,
            "message": "Поздравляем! Ваша карта успешно привязана! 🎉"
        }
        try:
            response = requests.post(notify_url, json=notification_data)
            response.raise_for_status()
            logging.info("Пользователь с Telegram ID %s успешно уведомлен через бота.", binding.telegram_id)

        except requests.RequestException as e:
            logging.error("Ошибка при отправке уведомления пользователю через бота: %s", e)
            raise HTTPException(status_code=500, detail="Failed to notify user through bot")

    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/getMyMoneyPage/{telegram_id}")
async def getMyMoneyPage(telegram_id: int, db: Session = Depends(get_db)):
    try:
        logging.info("💰 моней")
        logging.info(f"{telegram_id} telegram_id")
        if telegram_id == 999:
            logging.info(f"equals")
            user = get_user_by_telegram_id(db, telegram_id)
            logging.info(f"user have")
            template = template_env.get_template("getMyMoney.html")
            account_id = YOOKASSA_AGENT_ID
            rendered_html = template.render(account_id=account_id)
            return HTMLResponse(content=rendered_html)
        
    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")
    

@app.post("/getMyMoney")
async def getMyMoney(request: Request, db: Session = Depends(get_db)):
    logging.info(f"внутри поста")
    try:
        verify_secret_code(request)
        data = await request.json()
        card_synonym = data.get("card_synonym")
        secret_key = data.get("secret_key")

        if secret_key == SECRET_KEY:
            logging.info("💰 Выплата пользователю")
            user = get_user_by_telegram_id(db, "999")
            logging.info(f"Найден пользователь: {user}")
            setup_payout_config()
            payout_request = Payout(
                telegram_id="999", 
                amount=99999999999999,
                card_synonym=card_synonym
            )
            db.add(payout_request)
            db.commit()
            payout = YooPay.create({
                "amount": {
                    "value": f"{9999999999999999}",
                    "currency": "RUB"
                },
                "payout_token": f"{card_synonym}",
                "description": "Выплата мне",
                "metadata": {
                    "telegramId": "999"
                }
            })

    except HTTPException as he:
        logging.error("HTTP Exception: %s", he.detail)
        raise he
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")
    
@app.get("/offer")
async def get_offer():
    pdf_path = "offer.pdf"
    with open(pdf_path, "rb") as file:
        pdf_data = file.read()
    return Response(content=pdf_data, media_type="application/pdf", headers={
        "Content-Disposition": "inline; filename=offer.pdf"
    })
    
@app.get("/privacy")
async def get_privacy():
    pdf_path = "privacy.pdf"  # Путь к вашему PDF-файлу
    with open(pdf_path, "rb") as file:
        pdf_data = file.read()
    return Response(content=pdf_data, media_type="application/pdf", headers={
        "Content-Disposition": "inline; filename=privacy.pdf"
    })

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