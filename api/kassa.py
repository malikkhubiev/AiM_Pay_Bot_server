from loader import *
from utils import *
import uuid
from fastapi.responses import JSONResponse, HTMLResponse
from time import time
from yookassa import Payout, Settings, Payment
from config import (
    YOOKASSA_AGENT_ID
)
from jinja2 import Environment, FileSystemLoader
import logging
from database import (
    get_setting,
    get_binding_by_unique_str,
    get_pending_payment,
    get_users_with_positive_balance,
    get_referrer,
    get_payout,
    get_pending_payout,
    get_successful_referral_count,
    update_user_card_synonym,
    update_payment_done,
    create_binding_and_delete_if_exists,
    create_payout,
    update_payment_idempotence_key,
    create_payment_db,
    mark_payout_as_notified,
    update_referral_success,
    update_referral_rank,
    create_pending_payout,
    update_payout_transaction,
    update_payout_status,
    update_user_balance,
    get_all_settings,
    set_setting,
    get_user_pay_email,
    get_or_create_lead_by_email,
    record_lead_answer
)

template_env = Environment(loader=FileSystemLoader("templates"))

@app.post("/create_payment")
@exception_handler
async def create_payment(request: Request): 
    verify_secret_code(request)

    data = await request.json()
    telegram_id = data.get("telegram_id")
    amount = float(await get_setting("COURSE_AMOUNT"))
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
        return {"status": "error", "message": "Вы уже оплатили курс и являетесь его полноценым участником."}

    # Проверяем наличие email перед созданием платежа
    user_email = getattr(user, 'pay_email', None) if hasattr(user, 'pay_email') else (user.get('pay_email') if isinstance(user, dict) else None)
    if not user_email:
        user_email = await get_user_pay_email(telegram_id)
    
    if not user_email:
        return {"status": "error", "message": "Для создания платежа необходимо указать email. Пожалуйста, введите email в боте"}

    # Создаем или обновляем лид для действия "нажатие кнопки оплатить"
    try:
        # user может быть объектом Row или dict в зависимости от реализации БД
        username = getattr(user, 'username', None) if hasattr(user, 'username') else (user.get('username') if isinstance(user, dict) else None)
        lead_id = await get_or_create_lead_by_email(
            email=user_email,
            telegram_id=str(telegram_id),
            username=username
        )
        # Записываем действие
        if lead_id:
            await record_lead_answer(lead_id, 'bot_action_pay_course_clicked', 'true')
            logging.info(f"Лид {lead_id} обновлен для действия 'нажатие кнопки оплатить'")
    except Exception as e:
        logging.error(f"Ошибка при обновлении лида в create_payment: {e}")

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
            "return_url": f"{str(await get_setting('SERVER_URL'))}/success"
        },
        "capture": True,
        "description": "Оплата курса",
        "metadata": {
            "telegram_id": telegram_id
        }
    }

    # Добавляем данные для автоматического формирования чека, если есть email
    if user_email:
        payment_data["receipt"] = {
            "customer": {
                "email": user_email
            },
            "items": [
                {
                    "description": "Доступ к курсу",
                    "quantity": 1,
                    "amount": {
                        "value": f"{amount:.2f}",
                        "currency": "RUB"
                    },
                    "vat_code": 1
                }
            ]
        }
        logging.info(f"Добавлены данные для чека с email: {user_email}")
    else:
        logging.warning(f"Email пользователя не найден, чек не будет сформирован автоматически для telegram_id: {telegram_id}")
    
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

async def send_rank_notification(tg_id: str, message: str):
    logging.info(f"send_rank_notification called inside")
    
    notify_url = f"{str(await get_setting('MAHIN_URL'))}/notify_user"
    payload = {
        "telegram_id": tg_id,
        "message": message
    }
    try:
        await send_request(notify_url, payload)
        logging.info(f"Notification about rank sent to {tg_id}")
    except Exception as e:
        logging.error(f"Failed to notify about rank: {e}")

async def check_and_notify_rank_up(user):
    logging.info(f"check_and_notify_rank_up inside")
    
    successful_refs = await get_successful_referral_count(user.telegram_id)
    logging.info(f"successful_refs {successful_refs}")
    # Проверка на порог новых званий
    thresholds = [
        (60, "🧠 Архитектор мышления"),
        (50, "🌌 Духовный вдохновитель"),
        (40, "💎 Наставник Инноваций"),
        (30, "🚀 Вестник Эволюции"),
        (20, "🌎 Мастер экспансии"),
        (10, "🌱 Амбассадор развития"),
        (1, "🔥 Лидер роста"),
    ]

    for threshold, title in thresholds:
        # Если ровно достиг — поздравляем
        logging.info(f"successful_refs {successful_refs}")
        logging.info(f"threshold {threshold}")
        if successful_refs == threshold:
            logging.info(f"successful_refs = threshold")
            await update_referral_rank(user.telegram_id, title)
            message = (
                f"🎉 Поздравляем! Вы привлекли *{successful_refs}* новых участников!\n\n"
                f"🏆 Ваш новый статус: *{title}*\n\n"
                "Продолжайте делиться ссылкой и получайте бонусы 👇"
            )
            logging.info(f"message {message}")
            await send_rank_notification(user.telegram_id, message)
            logging.info(f"rank_notification sent")
            break  # Поздравляем только за одно достижение за раз

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
    income_amount = float(payment_data.get("income_amount")["value"])
    metadata = payment_data.get("metadata", {})
    user_telegram_id = metadata.get("telegram_id")

    logging.info(f"income_amount {income_amount}")
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
            await update_payment_done(
                user_telegram_id,
                payment_id,
                income_amount
            )
            all_settings = await get_all_settings()
            current_money = float(all_settings["MY_MONEY"])
            await set_setting("MY_MONEY", current_money + income_amount)

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
                    referral_current_amount = float(await get_setting("REFERRAL_AMOUNT"))
                    await update_referral_success(user_telegram_id, referrer_user.telegram_id)
                    logging.info(f"referrer_user есть")
                    new_balance = int((referrer_user.balance or 0) + referral_current_amount)
                    logging.info(f"referrer_user.balance {referrer_user.balance or 0}")
                    logging.info(f"float(REFERRAL_AMOUNT) {referral_current_amount}")
                    logging.info(f"new_balance {new_balance}")
                    await update_user_balance(referrer_user.telegram_id, new_balance)
                    logging.info(f"баланс для {referrer_user.telegram_id} обновили")
                    # 🔔 Проверка и отправка поздравления при новом звании
                    await check_and_notify_rank_up(referrer_user)
                    logging.info(f"check_and_notify_rank_up called")

            logging.info("Статус оплаты пользователя обновлен: %s", user_telegram_id)
            notification_data = {
                "telegram_id": user_telegram_id,
                "payment_id": payment_id
            }
            send_invite_link_url = f"{str(await get_setting('MAHIN_URL'))}/send_invite_link"
            try:
                invite_response = await send_request(send_invite_link_url, notification_data)
                
                # Получаем email пользователя и отправляем ссылку на email
                user_email = await get_user_pay_email(user_telegram_id)
                if user_email and invite_response and isinstance(invite_response, dict) and invite_response.get("invite_link"):
                    invite_link = invite_response.get("invite_link")
                    subject = "Поздравляем! Ваша оплата прошла успешно 🎉"
                    html = f"""
                    <p>Здравствуйте!</p>
                    <p>Ваша оплата курса прошла успешно! 🎉</p>
                    <p>Вот ссылка для присоединения к нашей группе в Telegram:</p>
                    <p><a href="{invite_link}">{invite_link}</a></p>
                    <p><b>Важно:</b> Ссылка одноразовая, действует 30 минут. Используйте её аккуратно!</p>
                    <p>Если возникнут вопросы, обращайтесь к нам.</p>
                    <p>С уважением,<br>Команда AiM Course</p>
                    """
                    text = f"""
                        Здравствуйте!

                        Ваша оплата курса прошла успешно! 🎉

                        Вот ссылка для присоединения к нашей группе в Telegram:
                        {invite_link}

                        Важно: Ссылка одноразовая, действует 30 минут. Используйте её аккуратно!

                        Если возникнут вопросы, обращайтесь к нам.

                        С уважением,
                        Команда AiM Course
                    """
                    try:
                        from utils import send_email_async
                        await send_email_async(user_email, subject, html, text)
                        logging.info(f"Email со ссылкой отправлен на {user_email}")
                        
                        # Уведомляем пользователя в Telegram о том, что чек отправлен на email и отправляем ссылку
                        notify_url = f"{str(await get_setting('MAHIN_URL'))}/notify_user"
                        notification_data1 = {
                            "telegram_id": user_telegram_id,
                            "message": f"✅ Ваша оплата успешно обработана!\n📧 Чек об оплате отправлен на вашу электронную почту: {user_email}"
                        }
                        notification_data2 = {
                            "telegram_id": user_telegram_id,
                            "message": f"💎 Пригласительная ссылка на материалы курса:\n{invite_link}\n\n🧠 <b>Важно:</b> Ссылка одноразовая, действует 30 минут. Используйте её аккуратно!"
                        }
                        try:
                            await send_request(notify_url, notification_data1)
                            await send_request(notify_url, notification_data2)
                            logging.info(f"Уведомление с ссылкой отправлено пользователю {user_telegram_id} в Telegram")
                        except Exception as notify_e:
                            logging.error(f"Ошибка при отправке уведомления о чеке: {notify_e}")
                    except Exception as e:
                        logging.error(f"Ошибка при отправке email на {user_email}: {e}")
            except Exception as e:
                logging.error(f"Ошибка при получении ссылки от бота: {e}")
            
            # Отправляем цель purchase_confirmed в Яндекс Метрику
            try:
                from utils import send_yandex_metrika_goal
                await send_yandex_metrika_goal("purchase_confirmed")
                logging.info(f"Yandex Metrika goal 'purchase_confirmed' sent for user {user_telegram_id}")
            except Exception as e:
                logging.error(f"Error sending Yandex Metrika goal: {e}")
            
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
        
        notify_url = f"{str(await get_setting('MAHIN_URL'))}/notify_user"
        notification_data = {
            "telegram_id": user_telegram_id,
            "message": payment_responces[reason]
        }
        await send_request(notify_url, notification_data)
        await mark_payout_as_notified(payment_id)
        return JSONResponse({"status": "success"})
        
    raise HTTPException(status_code=400, detail="Payment not processed")

# Payout функционал закомментирован - теперь выплаты делаются вручную через CRM
# @app.post("/create_payout")
# @exception_handler 
# async def create_payout(request: Request): 
#     verify_secret_code(request)
#     # Получаем всех пользователей с балансом > 0
#     users_with_balance = await get_users_with_positive_balance() 
#
#     for user in users_with_balance: 
#         telegram_id = user['telegram_id']
#         payout_amount = user['balance']  # Получаем баланс пользователя 
#
#         existing_payout = await get_pending_payout(telegram_id)
#         logging.info(f"existing_payout получили {existing_payout}")
#
#         idempotence_key = ""
#
#         if not(existing_payout):
#             logging.info(f"existing_payout не бывает")
#             idempotence_key = str(uuid.uuid4())
#             logging.info(f"сделали ключик {idempotence_key}")
#
#             await create_pending_payout(
#                 telegram_id,
#                 user['card_synonym'],
#                 idempotence_key,
#                 payout_amount
#             )
#         else:
#             idempotence_key = existing_payout.idempotence_key
#
#         logging.info(f"С бд поработали, делаем выплату")
#         setup_payout_config()
#         # Создаем запрос на выплату через YooKassa 
#         payout = Payout.create({ 
#             "amount": { 
#                 "value": f"{payout_amount}",  # Сумма выплаты 
#                 "currency": "RUB" 
#             }, 
#             "payout_token": f"{user['card_synonym']}",  # Карта пользователя 
#             "description": "Выплата рефералу", 
#             "metadata": { 
#                 "telegramId": f"{user['telegram_id']}"  # Дополнительная информация 
#             } 
#         }) 
#
#         # Обновляем запись в базе, добавляем transaction_id 
#         transaction_id = payout['id'] 
#         logging.info(f"transaction_id {transaction_id}")
#
#         await update_payout_transaction(user['telegram_id'], transaction_id) 
#         logging.info(f"transaction_id в бд засунули")
#         
#         logging.info(f"Выплата пользователю {user['telegram_id']} успешно инициирована.") 
#
#     return {"message": "Выплаты успешно инициированы."} 
#
# @app.post("/payout_result")
# @exception_handler
# async def payout_result(request: Request):
#     # Проверяем IP для уведомлений от Yookassa
#     check_yookassa_ip(request)
#     # Получение JSON данных из запроса
#     data = await request.json()
#     event = data.get("event")
#     object_data = data.get("object", {})
#     transaction_id = object_data.get("id", {})
#     metadata = object_data.get("metadata", {})
#
#     logging.info(data)
#
#     payout_record = await get_payout(transaction_id)
#     if not payout_record: 
#         raise HTTPException(status_code=404, detail="Запись о выплате не найдена") 
#
#     # Извлечение telegramId из метаданных
#     telegram_id = metadata.get("telegramId")
#
#     # Логирование события
#     print(f"Получено уведомление: {event}")
#     print(f"Данные объекта: {object_data}")

#     # Обработка событий
#     if event == "payout.succeeded":
#
#         amount = object_data['amount']['value']
#         await update_payout_status(transaction_id, "success")
#         await update_user_balance(telegram_id, 0)
#
#         notify_url = f"{str(await get_setting('MAHIN_URL'))}/notify_user"
#         notification_data = {
#             "telegram_id": telegram_id,
#             "message": f"Выплата на сумму {amount} произведена успешно"
#         }
#         await send_request(notify_url, notification_data)
#         await mark_payout_as_notified(transaction_id)
#         return JSONResponse({"status": "success"})
#     
#     elif event == "payout.canceled" and telegram_id:
#         # Выплата отменена
#         print("Выплата отменена.")
#         update_payout_status(transaction_id, "canceled")
#         logging.info(f"status {status}, и мы внутри")
#         cancellation_details = object_data.get("cancellation_details")
#         reason = cancellation_details["reason"]
#         user = await get_user_by_telegram_id(telegram_id)
#         logging.info(f"юзера тоже получили {user}")
#         notify_url = f"{str(await get_setting('MAHIN_URL'))}/notify_user"
#         notification_data = {
#             "telegram_id": telegram_id,
#             "message": payout_responces[reason]
#         }
#         await send_request(notify_url, notification_data)
#         await mark_payout_as_notified(transaction_id)
#         return JSONResponse({"status": "success"})
#
#     else:
#         # Неизвестное событие
#         print(f"Неизвестное событие: {event}")
#     # Возвращаем подтверждение получения уведомления
#     return JSONResponse(status_code=200, content={"message": "Webhook received successfully"})

# Старый функционал привязки карты через ссылку - закомментирован
# @app.post("/bind_card")
# @exception_handler
# async def bind_card(request: Request):
#     verify_secret_code(request)
#     data = await request.json()
#     telegram_id = data.get("telegram_id")
#
#     # Проверка обязательных параметров
#     check = check_parameters(
#         telegram_id=telegram_id
#     )
#     if not check["result"]:
#         return {"status": "error", "message": check["message"]}
#
#     # Находим пользователя
#     user = await get_user_by_telegram_id(telegram_id)
#
#     if not(user):
#         return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и нажмите на кнопку 'Начало работы' для регистрации в боте"}
#
#     unique_str = f"{telegram_id}{int(time() * 1000)}"
#
#     await create_binding_and_delete_if_exists(telegram_id, unique_str)
#
#     url = f"{str(await get_setting('SERVER_URL'))}/bind_card_page/{unique_str}"
#
#     return JSONResponse({"status": "success", "binding_url": url})
#
# @app.get("/bind_card_page/{unique_str}")
# def render_bind_card_page(unique_str: str):
#     check = check_parameters(
#         unique_str=unique_str
#     )
#     if not check["result"]:
#         return {"status": "error", "message": check["message"]}
#
#     template = template_env.get_template("bind_card.html")
#     account_id = YOOKASSA_AGENT_ID
#     rendered_html = template.render(account_id=account_id, unique_str=unique_str)
#
#     return HTMLResponse(content=rendered_html)
#     
# @app.post("/bind_success")
# @exception_handler
# async def bind_success(request: Request):
#     data = await request.json()
#     card_synonym = data.get("card_synonym")
#     unique_str = data.get("unique_str")
#
#     binding = await get_binding_by_unique_str(unique_str)
#     if not binding:
#         raise HTTPException(status_code=404, detail="Запрос на привязку карты не был осуществлён")
#
#     await get_user_by_telegram_id(binding.telegram_id)
#     logging.info(f"card_synonym {card_synonym}")
#     await update_user_card_synonym(binding.telegram_id, card_synonym)
#
#     # Уведомление пользователя
#     notify_url = f"{str(await get_setting('MAHIN_URL'))}/notify_user"
#     notification_data = {
#         "telegram_id": binding.telegram_id,
#         "message": "Поздравляем! Ваша карта успешно привязана! 🎉"
#     }
#     await send_request(notify_url, notification_data)
#     return JSONResponse({"status": "success"})

# Новый упрощенный функционал: сохранение номера карты напрямую
@app.post("/set_card_number")
@exception_handler
async def set_card_number(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")
    card_number = data.get("card_number")

    check = check_parameters(
        telegram_id=telegram_id,
        card_number=card_number
    )
    if not check["result"]:
        return {"status": "error", "message": check["message"]}

    # Валидация номера карты
    card_number_clean = card_number.replace(' ', '').replace('-', '')
    if not card_number_clean.isdigit() or len(card_number_clean) != 16:
        return {"status": "error", "message": "Номер карты должен содержать 16 цифр"}

    # Находим пользователя
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    if not user:
        return {"status": "error", "message": "Пользователь не найден"}

    # Сохраняем номер карты в поле card_synonym
    # При привязке карты пользователь становится реферером (будет отображаться в CRM даже если никого не привёл)
    await update_user_card_synonym(telegram_id, card_number_clean)
    
    logging.info(f"Номер карты сохранён для пользователя {telegram_id}. Пользователь теперь реферер.")
    return JSONResponse({"status": "success", "message": "Номер карты успешно сохранён"})

@app.post("/check_card")
@exception_handler
async def check_card(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")

    check = check_parameters(telegram_id=telegram_id)
    if not check["result"]:
        return {"status": "error", "message": check["message"]}

    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    if not user:
        return {"status": "error", "message": "Пользователь не найден"}

    has_card = bool(user.card_synonym and len(user.card_synonym) > 0)
    return JSONResponse({"status": "success", "has_card": has_card})

@app.get("/success")
async def success_payment(request: Request):
    template = template_env.get_template("success.html")
    rendered_html = template.render()
    return HTMLResponse(content=rendered_html)
