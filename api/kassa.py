from loader import *
from utils import *
import uuid
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
    update_user_card_synonym,
    update_payment_done,
    get_binding_by_unique_str,
    create_binding_and_delete_if_exists,
    get_pending_payment,
    create_payout,
    update_payment_idempotence_key,
    get_users_with_positive_balance,
    create_payment_db,
    mark_payout_as_notified,
    get_referrer,
    update_referral_success,
    create_pending_payout,
    update_payout_transaction,
    update_payout_status,
    update_user_balance,
    get_payout,
    get_pending_payout
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

    user = await get_user_by_telegram_id(telegram_id)
    
    if not(user):
        return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и зарегистрируйтесь в боте"}
    if user.paid:
        return {"status": "error", "message": "Вы уже оплатили курс и являетесь его полноценым участником. Введите команду /start, затем получите пригласительную ссылку, если вдруг потеряли группу среди чатов"}

    existing_payment = await get_pending_payment(telegram_id)

    idempotence_key = ""

    if not(existing_payment):
        idempotence_key = str(uuid.uuid4())

        payment = await create_payment_db(
            telegram_id=telegram_id,
            payment_id=None,
            idempotence_key=idempotence_key,
            status="pending"
        )
    else:
        idempotence_key = existing_payment.idempotence_key

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
        payment = Payment.create(payment_data, idempotence_key)
        logger.info("payment")
        logger.info(payment)
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
        return {"status": "error", "message": "Ошибка при создании платежа. Попробуйте ещё раз"}

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
        payment = await get_pending_payment(user_telegram_id)
        if payment:
            logging.info(f"payment {payment}")
        else:
            logging.info(f"payment нет")

        if payment:
            logging.info(f"Есть платёж в режиме ожидания. Завершаем операцию")
            await update_payment_done(user_telegram_id, payment_id)

            user = await get_user(user_telegram_id)
            logging.info(f"user {user}")
            logging.info(f"user.paid {user.paid}")

            logging.info(f"Ищём реферрала")
            referrer = await get_referrer(user_telegram_id)
            logging.info(f"referrer {referrer}")
            if referrer:
                logging.info(f"referrer {referrer} есть")
                referrer_user = await get_user_by_telegram_id(referrer.referrer_id, to_throw=False)
                logging.info(f"referrer_user {referrer_user}")
                if referrer_user:
                    await update_referral_success(user_telegram_id, referrer_user.telegram_id)
                    logging.info(f"referrer_user есть")
                    new_balance = referrer_user.balance + float(REFERRAL_AMOUNT)
                    logging.info(f"new_balance {new_balance}")
                    await update_user_balance(referrer_user.telegram_id, new_balance)
                    logging.info(f"баланс для {referrer_user.telegram_id} обновили")

            logging.info("Статус оплаты пользователя обновлен: %s", user_telegram_id)
            notification_data = {"telegram_id": user_telegram_id}
            send_invite_link_url = f"{MAHIN_URL}/send_invite_link"
            await send_request(send_invite_link_url, notification_data)
            await mark_payout_as_notified(payment_id)
            return JSONResponse({"status": "success"})
    
        return JSONResponse({"status": "success"})
    
    if status == "canceled" and user_telegram_id:
        logging.info(f"status {status}, и мы внутри")
        cancellation_details = payment_data.get("cancellation_details")
        reason = cancellation_details["reason"]
        user = await get_user_by_telegram_id(user_telegram_id)
        logging.info(f"юзера тоже получили {user}")
        
        if reason in ["expired_on_confirmation", "internal_timeout"]:
            idempotence_key = str(uuid.uuid4())
            await update_payment_idempotence_key(user_telegram_id, idempotence_key)
        
        notify_url = f"{MAHIN_URL}/notify_user"
        notification_data = {
            "telegram_id": user_telegram_id,
            "message": payment_responces[reason]
        }
        await send_request(notify_url, notification_data)
        await mark_payout_as_notified(payment_id)
        return JSONResponse({"status": "success"})
        
    raise HTTPException(status_code=400, detail="Payment not processed")

@app.post("/create_payout")
@exception_handler 
async def create_payout(request: Request): 
    verify_secret_code(request)
    # Получаем всех пользователей с балансом > 0
    users_with_balance = await get_users_with_positive_balance() 

    for user in users_with_balance: 
        telegram_id = user['telegram_id']
        payout_amount = user['balance']  # Получаем баланс пользователя 

        existing_payout = await get_pending_payout(telegram_id)
        logging.info(f"existing_payout получили {existing_payout}")

        idempotence_key = ""

        if not(existing_payout):
            logging.info(f"existing_payout не бывает")
            idempotence_key = str(uuid.uuid4())
            logging.info(f"сделали ключик {idempotence_key}")

            await create_pending_payout(
                telegram_id,
                user['card_synonym'],
                idempotence_key,
                payout_amount
            )
        else:
            idempotence_key = existing_payout.idempotence_key

        logging.info(f"С бд поработали, делаем выплату")
        setup_payout_config()
        # Создаем запрос на выплату через YooKassa 
        payout = Payout.create({ 
            "amount": { 
                "value": f"{payout_amount}",  # Сумма выплаты 
                "currency": "RUB" 
            }, 
            "payout_token": f"{user['card_synonym']}",  # Карта пользователя 
            "description": "Выплата рефералу", 
            "metadata": { 
                "telegramId": f"{user['telegram_id']}"  # Дополнительная информация 
            } 
        }) 

        # Обновляем запись в базе, добавляем transaction_id 
        transaction_id = payout['id'] 
        logging.info(f"transaction_id {transaction_id}")

        await update_payout_transaction(user['telegram_id'], transaction_id) 
        logging.info(f"transaction_id в бд засунули")
        
        logging.info(f"Выплата пользователю {user['telegram_id']} успешно инициирована.") 

    return {"message": "Выплаты успешно инициированы."} 

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

    logging.info(data)

    payout_record = await get_payout(transaction_id)
    if not payout_record: 
        raise HTTPException(status_code=404, detail="Запись о выплате не найдена") 

    # Извлечение telegramId из метаданных
    telegram_id = metadata.get("telegramId")

    # Логирование события
    print(f"Получено уведомление: {event}")
    print(f"Данные объекта: {object_data}")

    # Обработка событий
    if event == "payout.succeeded":

        amount = object_data['amount']['value']
        await update_payout_status(transaction_id, "success")
        await update_user_balance(telegram_id, 0)

        notify_url = f"{MAHIN_URL}/notify_user"
        notification_data = {
            "telegram_id": telegram_id,
            "message": f"Выплата на сумму {amount} произведена успешно"
        }
        await send_request(notify_url, notification_data)
        await mark_payout_as_notified(transaction_id)
        return JSONResponse({"status": "success"})
    
    elif event == "payout.canceled" and telegram_id:
        # Выплата отменена
        print("Выплата отменена.")
        update_payout_status(transaction_id, "canceled")
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
        await send_request(notify_url, notification_data)
        await mark_payout_as_notified(transaction_id)
        return JSONResponse({"status": "success"})

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

    if not(user):
        return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и нажмите на кнопку 'Начало работы' для регистрации в боте"}

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
    data = await request.json()
    card_synonym = data.get("card_synonym")
    unique_str = data.get("unique_str")

    binding = await get_binding_by_unique_str(unique_str)
    if not binding:
        raise HTTPException(status_code=404, detail="Запрос на привязку карты не был осуществлён")

    await get_user_by_telegram_id(binding.telegram_id)
    logging.info(f"card_synonym {card_synonym}")
    await update_user_card_synonym(binding.telegram_id, card_synonym)

    # Уведомление пользователя
    notify_url = f"{MAHIN_URL}/notify_user"
    notification_data = {
        "telegram_id": binding.telegram_id,
        "message": "Поздравляем! Ваша карта успешно привязана! 🎉"
    }
    await send_request(notify_url, notification_data)
    return JSONResponse({"status": "success"})

@app.get("/success")
async def success_payment(request: Request):
    template = template_env.get_template("success.html")
    rendered_html = template.render()
    return HTMLResponse(content=rendered_html)
