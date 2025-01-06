from loader import *
from utils import *
from fastapi.responses import JSONResponse, HTMLResponse
from time import time
from yookassa import Payout, Settings, Payment
from config import (
    COURSE_AMOUNT,
    REFERRAL_AMOUNT,
    YOOKASSA_AGENT_ID,
    MAHIN_URL,
    SERVER_URL,
    SECRET_KEY
)
from jinja2 import Environment, FileSystemLoader
import logging
from database import (
    get_binding_by_unique_str,
    create_binding_and_delete_if_exists,
    get_payment,
    create_payout,
    create_payment_db,
    mark_payout_as_notified,
    update_user_paid,
    get_referrer
)

template_env = Environment(loader=FileSystemLoader("templates"))

@app.post("/create_payment")
@exception_handler
async def create_payment(request: Request): 
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

@app.post("/payment_notification")
@exception_handler
async def payment_notification(request: Request):
    """Обработка уведомления о платеже от YooKassa."""
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
        user = await get_user_by_telegram_id(user_telegram_id)
        logging.info(f"юзера тоже получили {user}")
        payment = await get_payment(user_telegram_id)
        if payment:
            logging.info(f"payment {payment}")
        else:
            logging.info(f"payment нет")

        if not(payment):
            logging.info(f"Такой платёж мы видим в первый раз и это хорошо. Делаем платёж")
            await create_payment_db(user_telegram_id, payment_id)
            await update_user_paid(user_telegram_id)
            logging.info(f"Ищём реферрала")
            referrer = await get_referrer(user_telegram_id)
            logging.info(f"referrer {referrer}")
            if referrer:
                logging.info(f"referrer {referrer} есть")
                referrer_user = await get_user_by_telegram_id(referrer.referrer_id, to_throw=False)
                logging.info(f"referrer_user {referrer_user}")
                if referrer_user and referrer_user.card_synonym:
                    logging.info(f"referrer_user есть")
                    payout = Payout.create({
                        "amount": {
                            "value": f"{REFERRAL_AMOUNT}",
                            "currency": "RUB"
                        },
                        "payout_token": f"{referrer_user.card_synonym}",
                        "description": "Выплата рефералу",
                        "metadata": {
                            "telegramId": f"{referrer.telegram_id}"
                        }
                    })
            logging.info("Статус оплаты пользователя обновлен: %s", user_telegram_id)
            notification_data = {"telegram_id": user_telegram_id}
            send_invite_link_url = f"{MAHIN_URL}/send_invite_link"
            await send_request(send_invite_link_url, notification_data)
            await mark_payout_as_notified(payment_id)
            return JSONResponse(status_code=200)
        
    if status == "canceled" and user_telegram_id:
        logging.info(f"status {status}, и мы внутри")
        cancellation_details = payment_data.get("cancellation_details")
        reason = cancellation_details["reason"]
        user = await get_user_by_telegram_id(user_telegram_id)
        logging.info(f"юзера тоже получили {user}")
        notify_url = f"{MAHIN_URL}/notify_user"
        notification_data = {
            "telegram_id": user_telegram_id,
            "message": payment_responces[reason]
        }
        await send_request(notify_url, notification_data)
        await mark_payout_as_notified(payment_id)
        return JSONResponse(status_code=200)
        
    raise HTTPException(status_code=400, detail="Payment not processed")

@app.post("/payout_result")
@exception_handler
async def payout_result(request: Request):
    # Проверяем IP для уведомлений от Yookassa
    check_yookassa_ip(request)
    # Получение JSON данных из запроса
    data = await request.json()
    event = data.get("event")
    object_data = data.get("object", {})
    transaction_id = object_data.get("id", {})
    metadata = object_data.get("metadata", {})

    if metadata.get("author") == "me":
        return JSONResponse(status_code=200)
    else:
        logging.info(data)

        # Извлечение telegramId из метаданных
        telegram_id = metadata.get("telegramId")

        # Логирование события
        print(f"Получено уведомление: {event}")
        print(f"Данные объекта: {object_data}")

        # Обработка событий
        if event == "payout.succeeded":

            amount = object_data['amount']['value']

            user = await get_user_by_telegram_id(telegram_id)
            await create_payout(
                telegram_id,
                user.card_synonym,
                amount,
                transaction_id
            )
            notify_url = f"{MAHIN_URL}/notify_user"
            notification_data = {
                "telegram_id": telegram_id,
                "message": f"Выплата на сумму {amount} произведена успешно"
            }
            response = await send_request(notify_url, notification_data)
            await mark_payout_as_notified(transaction_id)
            return JSONResponse(status_code=200)
        
        elif event == "payout.canceled" and telegram_id:
            # Выплата отменена
            print("Выплата отменена.")
            logging.info(f"status {status}, и мы внутри")
            cancellation_details = object_data.get("cancellation_details")
            reason = cancellation_details["reason"]
            user = await get_user_by_telegram_id(telegram_id)
            logging.info(f"юзера тоже получили {user}")
            notify_url = f"{MAHIN_URL}/notify_user"
            notification_data = {
                "telegram_id": telegram_id,
                "message": payout_responces[reason]
            }
            response = await send_request(notify_url, notification_data)
            await mark_payout_as_notified(transaction_id)
            return JSONResponse(status_code=200)

        else:
            # Неизвестное событие
            print(f"Неизвестное событие: {event}")
    # Возвращаем подтверждение получения уведомления
    return JSONResponse(status_code=200, content={"message": "Webhook received successfully"})

@app.post("/bind_card")
@exception_handler
async def bind_card(request: Request):
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
    user = await get_user_by_telegram_id(telegram_id)

    if not(user.paid):
        return {"status": "error", "message": "Вы не можете стать партнёром по реферальной программе, не оплатив курс"}

    unique_str = f"{telegram_id}{int(time() * 1000)}"

    await create_binding_and_delete_if_exists(telegram_id, unique_str)

    url = f"{SERVER_URL}/bind_card_page/{unique_str}"

    return JSONResponse({"status": "success", "binding_url": url})

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
@exception_handler
async def bind_success(request: Request):
    verify_secret_code(request)
    data = await request.json()
    card_synonym = data.get("card_synonym")
    unique_str = data.get("unique_str")

    binding = await get_binding_by_unique_str(unique_str)
    if not binding:
        raise HTTPException(status_code=404, detail="Запрос на привязку карты не был осуществлён")

    user = await get_user_by_telegram_id(binding.telegram_id)
    logging.info(f"card_synonym {card_synonym}")
    user.card_synonym = card_synonym

    # Уведомление пользователя
    notify_url = f"{MAHIN_URL}/notify_user"
    notification_data = {
        "telegram_id": binding.telegram_id,
        "message": "Поздравляем! Ваша карта успешно привязана! 🎉"
    }
    response = await send_request(notify_url, notification_data)
    return JSONResponse(status_code=200)

@app.get("/getMyMoneyPage/")
@exception_handler
async def getMyMoneyPage(telegram_id: str):
    logging.info("💰 моней")
    logging.info(f"equals")
    template = template_env.get_template("getMyMoney.html")
    account_id = YOOKASSA_AGENT_ID

    # Получение настроек аккаунта
    me = Settings.get_account_settings()

    # Вывод баланса выплат
    print(me.payout_balance)
    rendered_html = template.render(account_id=account_id)
    return HTMLResponse(content=rendered_html)   
        
@app.post("/getMyMoney")
@exception_handler
async def getMyMoney(request: Request):
    logging.info(f"внутри поста")

    verify_secret_code(request)
    data = await request.json()
    card_synonym = data.get("card_synonym")
    secret_key = data.get("secret_key")
    amount = data.get("amount")

    if secret_key == SECRET_KEY:
        logging.info("💰 Выплата пользователю")
        user = await get_user_by_telegram_id("999")
        logging.info(f"Найден пользователь: {user}")
        setup_payout_config()
        payout = Payment.create({
            "amount": {
                "value": f"{amount}",
                "currency": "RUB"
            },
            "payout_token": f"{card_synonym}",
            "description": "Выплата мне",
            "metadata": {
                "author": "me"
            }
        })

@app.get("/success")
async def success_payment(request: Request):
    return HTMLResponse("<h1 style='text-align: center'>Операция прошла успешно. Вы можете возвращаться в бота</h1>")