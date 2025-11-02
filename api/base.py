from loader import *
from utils import *
import json
import plotly.graph_objects as go
import plotly.io as pio
from fastapi import BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse, HTMLResponse
import random
from io import BytesIO
import shutil
import qrcode
from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
import logging
from fastapi.staticfiles import StaticFiles
from database import (
    get_setting,
    set_setting,
    get_all_settings,
    get_registered_user,
    get_temp_user,
    get_users_with_positive_balance,
    get_payment_date,
    get_start_working_date,
    get_user_by_cert_id,
    get_promo_users_count,
    get_payments_frequency_db,
    get_pending_referrer,
    get_referred_user,
    get_all_paid_money,
    get_paid_count,
    get_all_referred,
    get_promo_user,
    get_promo_user_count,
    get_user_by_unique_str,
    get_paid_referrals_by_user,
    get_conversion_stats_by_source,
    get_referral_conversion_stats,
    get_top_referrers_from_db,
    get_expired_users,
    save_invite_link_db,
    create_referral,
    create_temp_user,
    add_promo_user,
    set_user_fake_paid,
    set_user_trial_end,
    update_pending_referral,
    update_temp_user_registered,
    update_temp_user,
    update_referrer,
    ultra_excute,
    update_fio_and_date_of_cert,
    update_passed_exam_in_db,
    get_all_settings,
    create_lead,
    set_user_pay_email,
    get_user_pay_email,
    get_unnotified_abandoned_leads,
    set_lead_notified,
    get_user,
    get_lead_by_id,
    get_lead_progress,
    record_lead_answer,
    update_lead_answer,
    get_leads,
    get_leads_total_count,
    get_or_create_lead_by_email,
    save_chat_message,
    get_chat_history,
    get_chat_message_count,
    Lead,
    database
)
from sqlalchemy import select
from config import (
    BOT_USERNAME,
    SMTP_PASSWORD,
    WHAPI_TOKEN,
    DEEPSEEK_TOKEN
)
import pandas as pd
from datetime import datetime, timezone, timedelta
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi import Query
import httpx
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re

TG_BOT_LINK = "https://t.me/AiM_Pay_Bot"  # TODO: поменять на вашего бота
FROM_EMAIL = "01_AiM_01@mail.ru"
SMTP_SERVER = "smtp.mail.ru"
SMTP_PORT = 587
SMTP_USER = "AiM"

WHAPI_URL = "https://gate.whapi.cloud/messages/text"  # отправка текстового сообщения Whapi.Cloud

templates = Jinja2Templates(directory="templates")

def is_valid_email(email):
    # примитивный, но разумный валидатор
    return re.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", email) is not None

def normalize_and_validate_phone_for_whapi(phone: str) -> str:
    """Normalize input phone to digits for Whapi (E.164 without '+').
    Rules:
    - Strip all non-digits
    - If starts with '00' → drop leading international prefix
    - If 11 digits starting with '8' → replace leading '8' with '7' (RU)
    - If 10 digits → assume RU, prefix '7'
    - Validate length 11..15
    Returns only digits.
    """
    digits = ''.join(ch for ch in (phone or '') if ch.isdigit())
    if digits.startswith('00'):
        digits = digits[2:]
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        # Assume RU if no country code provided
        digits = '7' + digits
    if not (11 <= len(digits) <= 15):
        raise ValueError("Некорректный номер телефона: ожидался международный формат")
    return digits

@app.post("/check_user")
@exception_handler
async def check_user(request: Request):
    verify_secret_code(request)
    logging.info("in check user")
    data = await request.json()
    telegram_id = data.get("telegram_id")
    to_throw = data.get("to_throw", True)
    logging.info(f"telegramId {telegram_id}")
    logging.info(f"to_throw {to_throw}")
    user = await get_user_by_telegram_id(telegram_id, to_throw)
    logging.info(f"user {user}")
    return {"status": "success", "user": user}

# @app.post("/save_invite_link")
# @exception_handler
# async def save_invite_link(request: Request):
#     verify_secret_code(request)
#     data = await request.json()
#     telegram_id = data.get("telegram_id")
#     invite_link = data.get("invite_link")

#     logging.info(f"Получены данные: telegram_id={telegram_id}, invite_link={invite_link}")

#     check = check_parameters(telegram_id=telegram_id, invite_link=invite_link)
#     logging.info(f"check = {check}")
#     if not(check["result"]):
#         return {"status": "error", "message": check["message"]}

#     logging.info(f"checknuli")
#     await save_invite_link_db(telegram_id, invite_link)
#     return {"status": "success"}

@app.post("/start")
@exception_handler
async def start(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")
    username = data.get("username")
    referrer_id = data.get('referrer_id')

    logging.info(f"Есть telegram_id {telegram_id}")
    logging.info(f"Есть username {username}")
    
    settings = await get_all_settings()
    logging.info(f"settings")
    logging.info(settings)

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
        "with_promo": None,
        "type": None
    }
    user = await get_registered_user(telegram_id)
    logging.info(f"user есть {user}")
    temp_user = None
    if user:
        greet_message = ""
        if user.referral_rank:
            greet_message = f"{user.referral_rank}\n\nЗдравствуй, почётный участник реферальной программы и AiM course!"
        else:
            greet_message = f"Привет, {user.username}! Я тебя знаю. Ты участник AiM course!"

        return_data["response_message"] = greet_message
        return_data["type"] = "user"
        logging.info(f"user есть")
        if not(user.paid):
            logging.info(f"user не платил")
            return_data["to_show"] = "pay_course"
        if not(user.date_of_trial_ends):
            logging.info(f"пробный период не использовался")
            return_data["to_show"] = "trial"
        
        promo_user = await get_promo_user(user.telegram_id)
        number_of_promo = await get_promo_user_count() 
        logging.info(f"promo_num_limit = {int(await get_setting('PROMO_NUM_LIMIT'))}")
        logging.info(f"promo_num_left = {int(await get_setting('PROMO_NUM_LIMIT')) - number_of_promo}")
        if not(promo_user) and number_of_promo < int(await get_setting("PROMO_NUM_LIMIT")):
            return_data["with_promo"] = True

        logging.info(f"/start in base api return_data {return_data}")
        return JSONResponse(return_data)
    else:
        return_data["type"] = "temp_user"
        logging.info(f"Юзера нет")
        return_data["response_message"] = f"Добро пожаловать, {username}!"
        temp_user = await get_temp_user(telegram_id=telegram_id)
        if temp_user:
            logging.info(f"Есть только временный юзер. Обновляем")
            logging.info(f"Его зовут {temp_user.username}")
            await update_temp_user(telegram_id=telegram_id, username=username)
            logging.info(f"created_at {temp_user.created_at}")
        else:
            logging.info(f"Делаем временный юзер")
            logging.info(f"telegram_id {telegram_id}")
            logging.info(f"username {username}")
            temp_user = await create_temp_user(telegram_id=telegram_id, username=username)
    
    logging.info(f"temp_user {temp_user}")
    logging.info(f"user {user}")
    
    if referrer_id and referrer_id != telegram_id and (temp_user or (user and not(user.paid))):
        logging.info(f"Есть реферрал и сам себя не привёл")
        existing_referrer = await get_pending_referrer(telegram_id)
        if existing_referrer:
            logging.info(f"Реферал уже был")
            await update_referrer(telegram_id, referrer_id)
        else:
            logging.info(f"Реферала ещё не было")
            referrer_user = await get_user_by_telegram_id(referrer_id, to_throw=False)
            if referrer_user and referrer_user.card_synonym: 
                logging.info(f"Пользователь который привёл есть")
                await create_referral(telegram_id, referrer_id)
                logging.info(f"Сделали реферала в бд")
    return JSONResponse(return_data)

@app.post("/send_demo_link")
@exception_handler
async def send_demo_link(request: Request, background_tasks: BackgroundTasks):
    # Публичная форма — без verify_secret_code
    data = await request.json()
    name = data.get("name")
    email = data.get("email")
    phone = data.get("phone")
    chat_history = data.get("chat_history", [])  # История чата из localStorage
    chat_session_id = data.get("chat_session_id", "")  # ID сессии чата
    
    if not (name and email and phone):
        return JSONResponse({"status": "error", "message": "Заполните все поля"}, status_code=400)

    subject = "AiM Course — ссылка на демо"
    channel_url = "https://rutube.ru/channel/62003781/"
    html = (
        f"<p>Здравствуй, {name}!, мы очень рады с тобой познакомиться)</p>"
        f"<p>От лица команды AiM отправляю тебе ссылку на 150+ видео-уроков демо курса: <a href=\"{channel_url}\">RUTUBE канал</a>.</p>"
        f"<p>Если будут вопросы, пиши в любое время. Будем рады тебе помочь: 01_AiM_01@mail.ru</p>"
        f"<p>Мы желаем тебе отличного дня и приятного просмотра!)</p>"
    )
    text = (
        f"Здравствуй, {name}!, мы очень рады с тобой познакомиться)\n\n"
        f"От лица команды AiM отправляю тебе ссылку на 150+ видео-уроков демо курса: {channel_url}\n"
        f"Если будут вопросы, пиши в любое время. Будем рады тебе помочь: 01_AiM_01@mail.ru\n\n"
        f"Мы желаем тебе отличного дня и приятного просмотра!)"
    )

    # Асинхронная отправка в фоне, чтобы не держать запрос в pending
    background_tasks.add_task(send_email_async, email, subject, html, text)
    # Также создадим лида и отправим привет в WhatsApp в фоне, передавая историю чата
    background_tasks.add_task(_create_lead_and_notify_internal, name, email, phone, None, chat_history, chat_session_id)
    logging.info(f"Email and lead creation queued for {email} / {phone}, chat_history_length={len(chat_history)}")
    return JSONResponse({"status": "success", "queued": True})

async def _create_lead_and_notify_internal(name: str, email: str, phone: str, lead_id: int = None, chat_history: list = None, chat_session_id: str = None):
    if not (email and phone and name):
        return
    logging.info(f"notifying lead (internal): {name}, {email}, {phone}, lead_id={lead_id}")
    
    # Если lead_id не передан, пытаемся создать лид
    if lead_id is None:
        try:
            lead_id = await create_lead(name, email, phone)
            logging.info("lead created (internal)")
        except ValueError as e:
            # Дубликат лида - логируем, но не падаем
            logging.warning(f"Лид с такими данными уже существует: {e}")
            # Пытаемся найти существующий лид по email или phone
            try:
                # Ищем по email
                if email:
                    email_query = select(Lead).where(Lead.email == email)
                    async with database.transaction():
                        existing_lead = await database.fetch_one(email_query)
                        if existing_lead:
                            lead_id = existing_lead["id"]
                            logging.info(f"Found existing lead_id={lead_id} by email")
                        else:
                            # Ищем по phone
                            if phone:
                                phone_query = select(Lead).where(Lead.phone == phone)
                                existing_lead = await database.fetch_one(phone_query)
                                if existing_lead:
                                    lead_id = existing_lead["id"]
                                    logging.info(f"Found existing lead_id={lead_id} by phone")
                                else:
                                    logging.warning("Lead not found by email or phone")
                                    return
                            else:
                                return
            except Exception as e2:
                logging.error(f"Error finding existing lead: {e2}")
                return
        except Exception as e:
            logging.error(f"Ошибка при создании лида: {e}")
            return
    
    # Сохраняем историю чата в базу, если она передана
    if chat_history and chat_session_id:
        try:
            for msg in chat_history:
                if isinstance(msg, dict) and "message" in msg:
                    await save_chat_message(
                        session_id=chat_session_id,
                        message=msg.get("message", ""),
                        is_from_user=msg.get("is_from_user", True)
                    )
            logging.info(f"Saved {len(chat_history)} chat messages for lead_id={lead_id}")
        except Exception as e:
            logging.error(f"Error saving chat history: {e}")

    # Compose link to personalized landing
    server_url = "https://mind-testing.vercel.app"
    link_part = f"\n\n✅ Ссылка на 150+ видео-уроков была отправлена тебе на почту.\n🎁 Чтобы принести тебе максимальную пользу, отправляем тест (10 вопросов) на мышление инженера ML как небольшой подарок: {server_url}/?lead_id={lead_id}" if server_url else ""
    wa_message = f"Здравствуй, {name}!\nКоманда AiM очень рада с тобой познакомиться!)\nНапоминаем, твои данные:\nТвоя почта: {email}\nНомер телефона: {phone}{link_part}"
    logging.info(f"wa_message (full): {repr(wa_message)}")
    wa_phone = normalize_and_validate_phone_for_whapi(phone)
    headers = {
        "Authorization": f"Bearer {WHAPI_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "to": wa_phone,
        "body": wa_message
    }
    logging.info(f"payload {payload}")
    logging.info(f"payload['body'] (full): {repr(payload['body'])}")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(WHAPI_URL, headers=headers, json=payload)
            resp.raise_for_status()
        logging.info("WhatsApp sent (internal)")
    except Exception as e:
        logging.error(f"Ошибка при отправке WhatsApp (internal): {e}")
    await set_lead_notified(email)

@app.post("/getting_started")
@exception_handler
async def getting_started(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")

    logging.info(f"Получены данные: telegram_id={telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    logging.info(f"check = {check}")
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}

    logging.info(f"checknuli")
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    logging.info(f"user = {user}")

    if user.is_registered:
        return {"status": "error", "message": "Вы уже зарегистрированы в боте. Введите команду /start, затем оплатите курс для доступа к материалам или присоединяйтесь к реферальной системе"}

    temp_user = await get_temp_user(telegram_id)
    logging.info(f"temp_user {temp_user}")
    if temp_user:
        return_data = {
            "status": "success",
            "with_promo": None
        }
        logging.info(f"Есть временный юзер")
        username = temp_user.username
        logging.info(f"У него есть username {username}")
        await update_temp_user_registered(telegram_id)
        await update_pending_referral(telegram_id)
        logging.info(f"Получены данные: telegram_id={telegram_id}, username={username}")
        logging.info(f"Пользователь {username} зарегистрирован")
        
        # Создаем или обновляем лид для действия getting_started
        try:
            lead_id = await get_or_create_lead_by_email(
                email=None,  # Email пока нет, будет при set_pay_email
                telegram_id=str(telegram_id),
                username=username
            )
            # Записываем действие getting_started
            if lead_id:
                await record_lead_answer(lead_id, 'bot_action_getting_started', 'true')
            logging.info(f"Лид создан/обновлен для telegram_id={telegram_id}")
        except Exception as e:
            logging.error(f"Ошибка при создании лида: {e}")

        promo_user = await get_promo_user(telegram_id)
        number_of_promo = await get_promo_user_count() 
        if not(promo_user) and number_of_promo < int(await get_setting("PROMO_NUM_LIMIT")):
            return_data["with_promo"] = True

        return JSONResponse(return_data)

@app.post("/register_user_with_promo")
@exception_handler
async def register_user_with_promo(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")

    logging.info(f"Получены данные: telegram_id={telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    logging.info(f"check = {check}")
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}

    logging.info(f"checknuli")
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    logging.info(f"user = {user}")

    if not(user):
        return {"status": "error", "message": "Вы ещё не зарегистрированы в боте. Введите команду /start, затем оплатите курс для доступа к материалам или присоединяйтесь к реферальной системе"}
    is_already_promo_user = await get_promo_user(telegram_id)
    logging.info(f"is_already_promo_user {is_already_promo_user}")
    if is_already_promo_user:
        return {"status": "error", "message": "Вы уже были зарегистрированы по промокоду"}

    number_of_promo = await get_promo_user_count() 
    if number_of_promo < int(await get_setting("PROMO_NUM_LIMIT")):  
        await add_promo_user(telegram_id)
        notification_data = {"telegram_id": telegram_id}
        send_invite_link_url = f"{str(await get_setting('MAHIN_URL'))}/send_invite_link"
        await send_request(send_invite_link_url, notification_data)

        return JSONResponse({"status": "success"})
    else:
        return JSONResponse({
            "status": "error",
            "message": "Лимит пользователей, которые могут зарегистрироваться по промокоду, исчерпан"
        })

async def generate_clients_report_list_base(telegram_id, response_type):
    logging.info(f"telegram_id {telegram_id}")
    
    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    logging.info(f"Чекнули")
    # Находим пользователя
    user = await get_user_by_telegram_id(telegram_id)
    
    if not(user):
        return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и нажмите на кнопку 'Начало работы' для регистрации в боте"}

    logging.info(f"user есть")

    referred_details = await get_all_referred(telegram_id)

    logging.info(f"detales есть")
    logging.info(f"{referred_details} referred_details")
    
    invited_list = []
    logging.info(f"invited_list {invited_list}")

    if referred_details:
        logging.info("Referral details found.")
        
        referrals_with_payment = []

        for referral in referred_details:
            referred_user = await get_referred_user(referral.referred_id)
            if referred_user:
                payment_date = await get_payment_date(referral.referred_id)
                start_working_date = await get_start_working_date(referral.referred_id)

                referral_data = {
                    "telegram_id": referred_user.telegram_id,
                    "username": referred_user.username,
                    "payment_date": payment_date,
                    "start_working_date": start_working_date,
                    "time_for_pay": format_timedelta(payment_date - start_working_date) if payment_date and start_working_date else ""
                }
                
                if payment_date and start_working_date:
                    if response_type == "string":
                        payment_date_formatted = format_datetime(payment_date)
                        start_working_date_formatted = format_datetime(start_working_date)
                        referral_data["payment_date"] = payment_date_formatted
                        referral_data["start_working_date"] = start_working_date_formatted
                    elif response_type == "datetime":
                        referral_data["payment_date"] = format_datetime_for_excel(payment_date)
                        referral_data["start_working_date"] = format_datetime_for_excel(start_working_date)

                referrals_with_payment.append((payment_date, referral_data))

        # Сортируем список по убыванию даты платежа (самые последние оплаты в начале)
        sorted_referrals = sorted(referrals_with_payment, key=lambda x: x[0] or datetime.min, reverse=True)
        
        # Формируем окончательный список
        invited_list = [referral_data for _, referral_data in sorted_referrals]

    logging.info(f"invited_list {invited_list} когда вышли")

    return invited_list
    
@app.post("/generate_clients_report_list_as_is")
@exception_handler
async def generate_clients_report_list_as_is(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")

    invited_list = await generate_clients_report_list_base(telegram_id, "string")

    return JSONResponse({
        "status": "success",
        "invited_list": invited_list
    })
    
@app.post("/generate_clients_report_list_as_file")
@exception_handler
async def generate_clients_report_list_as_file(request: Request, background_tasks: BackgroundTasks):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")

    invited_list = await generate_clients_report_list_base(telegram_id, "datetime")

    df = pd.DataFrame(invited_list)

    # Убираем возможные ошибки кодировки
    df = df.astype(str).apply(lambda x: x.str.encode('utf-8', 'ignore').str.decode('utf-8'))

    EXPORT_FOLDER = 'exports'
    os.makedirs(EXPORT_FOLDER, exist_ok=True)

    file_path = os.path.join(EXPORT_FOLDER, f"report_{telegram_id}.xlsx")

    try:
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Report", index=False)

        if not os.path.exists(file_path):
            logging.error(f"Файл не найден после создания: {file_path}")
            raise HTTPException(status_code=500, detail="Не удалось создать отчет")

        logging.info(f"Отправка отчета: {file_path}")

        # Добавляем задачу на удаление файла после отправки
        background_tasks.add_task(delete_file, file_path)

        return FileResponse(
            path=file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename="clients_report.xlsx"
        )

    except Exception as e:
        logging.error(f"Ошибка генерации отчета: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка генерации отчета: {e}")

def delete_file(file_path: str):
    """Функция для удаления файла после отправки"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Файл {file_path} удалён после отправки")
    except Exception as e:
        logging.error(f"Ошибка при удалении файла {file_path}: {e}")

@app.post("/generate_clients_report")
@exception_handler
async def generate_clients_report(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")
    
    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    logging.info(f"Чекнули")
    # Находим пользователя
    user = await get_user_by_telegram_id(telegram_id)
    
    if not(user):
        return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и нажмите на кнопку 'Начало работы' для регистрации в боте"}

    logging.info(f"user есть")

    # Calculate total paid money
    all_paid_money = await get_all_paid_money(telegram_id)
    paid_count = await get_paid_count(telegram_id)

    # Generate the report
    report = {
        "username": user.username,
        "paid_count": paid_count,
        "total_payout": all_paid_money,
        "balance": user.balance or 0
    }

    return JSONResponse({
        "status": "success",
        "report": report
    })

@app.post("/get_referral_link")
@exception_handler
async def get_referral_link(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")
    
    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    user = await get_user_by_telegram_id(telegram_id)
    logging.info(f"user {user}")
    logging.info(f"paid {user.paid}")
    if not(user):
        return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и нажмите на кнопку 'Начало работы' для регистрации в боте"}
    if not(user.card_synonym):
       return {"status": "error", "message": "Вы не можете стать партнёром по реферальной программе, не привязав карту"}
    
    referral_link = f"https://t.me/{BOT_USERNAME}?start={telegram_id}"
    return {"status": "success", "referral_link": referral_link}

@app.post("/payout_balance")
async def get_payout_balance(request: Request):
    verify_secret_code(request)
    logging.info("inside_payout_balance")
    referral_statistics = await get_users_with_positive_balance()

    logging.info(f"referral_statistics {referral_statistics}")

    total_balance = 0
    users = []

    for user in referral_statistics:
        total_balance += user['balance']
        users.append({
            "id": user["telegram_id"],
            "name": user["username"]
        })

    logging.info(f"referral_statistics {referral_statistics}")
    
    total_extra = total_balance * 0.028
    logging.info(f"total_extra {total_extra}")

    num_of_users = len(referral_statistics)
    logging.info(f"num_of_users {num_of_users}")

    num_of_users_plus_30 = num_of_users*30
    logging.info(f"num_of_users_plus_30 {num_of_users_plus_30}")

    result = total_balance + total_extra + num_of_users_plus_30
    logging.info(f"result {result}")

    return JSONResponse({
        "status": "success",
        "data": {
            "total_balance": total_balance,
            "total_extra": total_extra,
            "num_of_users": num_of_users,
            "num_of_users_plus_30": num_of_users_plus_30,
            "result": result,
            "users": users
        }
    })

@app.post("/get_promo_users_frequency")
async def get_promo_users_frequency(request: Request):
    logging.info("inside get_promo_users_frequency")

    verify_secret_code(request)
    date = datetime.now(timezone.utc)
    logging.info(f"date {date}")
    
    promo_users_frequency = await get_promo_users_count()
    logging.info(f"promo_users_frequency {promo_users_frequency}")

    if promo_users_frequency:
        # Преобразуем объект Record в словарь или список, если нужно
        promo_users_frequency_values = [dict(record) for record in promo_users_frequency]
    else:
        promo_users_frequency_values = []
    
    number_of_promo = await get_promo_user_count() 
    promo_num_left = int(await get_setting("PROMO_NUM_LIMIT")) - number_of_promo

    # Формируем ответ
    return JSONResponse({
        "status": "success",
        "data": {
            "number_of_promo": number_of_promo,
            "promo_num_left": promo_num_left,
            "promo_users_frequency": promo_users_frequency_values
        }
    })

@app.post("/get_payments_frequency")
async def get_payments_frequency(request: Request):
    logging.info("inside get_payments_frequency")

    verify_secret_code(request)
    
    payments_frequency = await get_payments_frequency_db()
    logging.info(f"payments_frequency {payments_frequency}")

    # Проверяем, что ответ от базы данных не пустой
    if payments_frequency:
        # Преобразуем объект Record в словарь или список, если нужно
        payments_frequency_values = [dict(record) for record in payments_frequency]
    else:
        payments_frequency_values = []

    return JSONResponse({
        "status": "success",
        "data": {
            "payments_frequency": payments_frequency_values
        }
    })

@app.post("/generate_referral_chart_link")
async def generate_referral_chart_link(request: Request):
    """ Генерирует ссылку на график рефералов """

    logging.info("inside generate_referral_chart_link")
    verify_secret_code(request)
    
    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    if user:
        logging.info(f"user {user}")
        unique_str = user.unique_str

        chart_url = f"{str(await get_setting('SERVER_URL'))}/referral_chart/{unique_str}"
        logging.info(f"chart_url {chart_url}")
        return JSONResponse({
            "status": "success",
            "data": {
                "chart_url": chart_url
            }
        })
    else:
        return JSONResponse({
            "status": "error",
            "message": "Пользователь не найден"
        })

@app.get("/referral_chart/{unique_str}")
async def referral_chart(unique_str: str):
    """ Генерирует HTML с графиком Plotly для пользователя по unique_str """
    
    logging.info(f"inside referral_chart")
    
    user = await get_user_by_unique_str(unique_str)
    if not user:
        return HTMLResponse("<h3>Ссылка недействительна</h3>", status_code=404)

    referral_data = await get_paid_referrals_by_user(user.telegram_id)
    logging.info(f"referral_data {referral_data}")

    # Преобразуем ключи в строковый формат "дд.мм"
    formatted_dates = [datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m") for date_str in referral_data.keys()]

    # Создаем график
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=formatted_dates, y=list(referral_data.values()), mode='lines+markers', name='Рефералы'))

    # Устанавливаем форматирование для оси X
    fig.update_layout(
        title="График оплат рефералов",
        xaxis_title="Дата",
        yaxis_title="Количество",
        xaxis=dict(tickformat="%d.%m")  # Форматирование оси X
    )

    # Генерируем HTML
    html_content = pio.to_html(fig, full_html=True, include_plotlyjs='cdn')
    return HTMLResponse(html_content)

@app.post("/save_fio")
async def save_fio(request: Request):

    logging.info("inside save_fio")
    verify_secret_code(request)
    
    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    if user:
        logging.info(f"user {user}")

        if not(user.passed_exam):
            return JSONResponse({
                "status": "error",
                "message": "Тест не сдан"
            })
        if user.fio:
            return JSONResponse({
                "status": "error",
                "message": "Вы уже указали ФИО. Изменить ФИО невозможно"
            })
        
        logging.info(f"ФИО ещё не установлено")
        fio = data.get("fio")
        logging.info(f"полученное ФИО {fio}")

        await update_fio_and_date_of_cert(telegram_id, fio)

        logging.info(f"ФИО обновлено")

        return JSONResponse({
            "status": "success",
            "data": {
                "message": "Ваше ФИО установлено. Вы можете скачать сертификат, получить ссылку на просмотр или вернуться в главное меню"
            }
        })
    else:
        return JSONResponse({
            "status": "error",
            "message": "Пользователь не найден"
        })

@app.post("/update_passed_exam")
async def update_passed_exam(request: Request):

    logging.info("inside update_passed_exam")
    logging.info(f"request {request}")
    verify_secret_code(request)
    
    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    if user:
        logging.info(f"user {user}")

        await update_passed_exam_in_db(telegram_id)

        logging.info(f"Тест сдан")

        return JSONResponse({
            "status": "success"
        })
    else:
        return JSONResponse({
            "status": "error",
            "message": "Пользователь не найден"
        })

@app.post("/can_get_certificate")
async def can_get_certificate(request: Request, background_tasks: BackgroundTasks):

    logging.info("inside can_get_certificate")
    verify_secret_code(request)
    
    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    if not(user):
        return JSONResponse({
            "status": "error",
            "message": "Такого пользователя не существует"
        })
    
    promo = await get_promo_user(telegram_id)
    
    if not(user.paid) and not(promo):
        return JSONResponse({
            "status": "error",
            "message": "Для прохождения теста необходимо оплатить курс"
        })
    
    if not(user.passed_exam):
        return JSONResponse({
            "status": "success",
            "result": "test"
        })
    
    if not(user.fio):
        return JSONResponse({
            "status": "error",
            "message": "Вы не установили своё ФИО для получения сертификата. Введите ФИО в формате: 'ФИО: Иванов Иван Иванович'. Будьте аккуратны в написании, исправить ФИО невозможно. Дата установки ФИО считается датой формирования сертификата."
        })
    
    else:
        return JSONResponse({
            "status": "success",
            "result": "passed"
        })

@app.post("/get_multiplicators")
async def get_multiplicators(request: Request):

    logging.info("inside get_multiplicators")
    verify_secret_code(request)
    
    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    source_stats = await get_conversion_stats_by_source()
    referral_stats = await get_referral_conversion_stats()

    return JSONResponse({
        "status": "success",
        "result": {
            "source_stats": source_stats,
            "referral_stats": referral_stats
        }
    })

@app.post("/get_top_referrers")
async def get_top_referrers(request: Request):

    logging.info("inside get_top_referrers")
    verify_secret_code(request)
    
    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    top = await get_top_referrers_from_db()

    return JSONResponse({
        "status": "success",
        "top": top
    })
    
async def generate_certificate_file(user):
    EXPORT_FOLDER = 'exports'
    os.makedirs(EXPORT_FOLDER, exist_ok=True)
    
    name = user["fio"]
    cert_id = "CERT-" + user["telegram_id"][:10]

    current_dir = os.path.dirname(os.path.abspath(__file__))  # Папка, где находится скрипт
    template_dir = os.path.abspath(os.path.join(current_dir, "..", "templates"))
    template_path = os.path.join(template_dir, "cert_template.pdf")

    output_path = os.path.join(EXPORT_FOLDER, f"certificate_{cert_id}.pdf")

    # Генерируем QR-код
    qr_data = f"{str(await get_setting('SERVER_URL'))}/certifications?cert_id={cert_id}"
    qr = qrcode.make(qr_data)

    qr_path = os.path.join(EXPORT_FOLDER, f"qr_{cert_id}.png")
    qr.save(qr_path)

    # Генерируем PDF поверх шаблона
    buffer = BytesIO()
    c = canvas.Canvas(buffer)

    # Регистрируем шрифты
    font_path = os.path.join(current_dir, "..", "Jura.ttf")
    font = "Jura"
    pdfmetrics.registerFont(TTFont(font, font_path))
    
    c.setPageSize((842, 595))  # A4
    c.setFont(font, 36)

    # Вставляем дату
    date_str = user["date_of_certificate"].strftime("%d.%m.%Y")
    font_size = 20
    c.setFont(font, font_size)
    # text_width = c.stringWidth(name, font, font_size)
    x = (842 - 105) / 2  # Центр страницы по ширине
    c.drawString(x, 45, date_str)

    # Вставляем центрированное имя
    font_size = 36
    c.setFont(font, font_size)
    c.setFillColorRGB(1, 1, 1)  # Белый цвет
    text_width = c.stringWidth(name, font, font_size) + 13
    x = (842 - text_width) / 2  # Центр страницы по ширине
    c.drawString(x, 235, name)

    # Вставляем cert_id над QR-кодом
    c.setFillColorRGB(1, 1, 1)  # Белый цвет
    c.setFont(font, 17)
    c.drawString(35, 185, cert_id)  

    # Вставляем QR-код
    c.drawImage(ImageReader(qr_path), 35, 35, 138, 138)

    c.showPage()
    c.save()

    buffer.seek(0)

    # Накладываем текст и QR-код на шаблон
    template_pdf = PdfReader(template_path)
    overlay_pdf = PdfReader(buffer)
    output_pdf = PdfWriter()

    page = template_pdf.pages[0]
    page.merge_page(overlay_pdf.pages[0])
    output_pdf.add_page(page)

    # Сохраняем PDF во временную папку
    with open(output_path, "wb") as f:
        output_pdf.write(f)
    
    return output_path, qr_path, cert_id

@app.post("/generate_certificate")
async def generate_certificate(request: Request, background_tasks: BackgroundTasks):

    logging.info("inside generate_certificate")
    verify_secret_code(request)
    
    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    if not(user):
        return JSONResponse({
            "status": "error",
            "message": "Такого пользователя не существует"
        })
    if not(user.fio):
        return JSONResponse({
            "status": "error",
            "message": "Сертификационный тест не был сдан"
        })
    
    output_path, qr_path, cert_id = await generate_certificate_file(user)

    background_tasks.add_task(delete_file, output_path)
    background_tasks.add_task(delete_file, qr_path)

    return FileResponse(
        path=output_path,
        media_type="application/pdf",
        filename=f"certificate_{cert_id}.pdf"
    )

@app.get("/certifications", response_class=HTMLResponse)
async def certificate_page(request: Request, cert_id: str = None):
    
    logging.info("called certificate_page")

    if cert_id:
        logging.info(f"cert_id {cert_id}")
        
        user = await get_user_by_cert_id(cert_id)  # Получаем пользователя по cert_id
        
        logging.info(f"user getting query is done")
        logging.info(f"user {user}")
        logging.info(f"passed_exam {user.passed_exam}")

        if user and user.passed_exam:
            certificate = {
                "id": cert_id,
                "name": user.fio,
                "date": user.date_of_certificate.strftime("%d.%m.%Y")
            }
            # Передаем cert_id, данные сертификата и URL изображения в шаблон
            return templates.TemplateResponse("certificate_view.html", {
                "request": request,
                "certificate": certificate
            })

@app.post("/execute_sql")
async def execute_sql(request: Request):

    logging.info("inside execute_sql")
    verify_secret_code(request)
    
    data = await request.json()
    query = data.get("query")
    logging.info(f"query {query}")

    check = check_parameters(query=query)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    result = await ultra_excute(query)
    return JSONResponse({
        "status": result["status"],
        "result": result["result"]
    })

@app.post("/update_and_get_settings")
async def update_and_get_settings(request: Request):
    """ Обновляет/устанавливает значение настройки и возвращает все настройки """

    logging.info("inside update_and_get_settings")
    verify_secret_code(request)

    data = await request.json()
    key = data.get("key")
    value = data.get("value")

    if key and value:
        logging.info(f"Обновляем настройку: {key} = {value}")
        await set_setting(key, value)

    all_settings = await get_all_settings()
    logging.info(f"Текущие настройки: {all_settings}")

    return JSONResponse({
        "status": "success",
        "data": all_settings
    })









VERIFY_TOKEN = "AiMcourseEducation"
ACCESS_TOKEN = "1"


# Insta

# Верификация вебхука Instagram
@app.get("/webhook")
async def verify_webhook(request: Request):
    params = dict(request.query_params)
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == VERIFY_TOKEN:
        return int(params["hub.challenge"])
    return {"status": "Invalid verification"}

@app.post("/webhook")
async def receive_message(request: Request):
    logging.info(f"receive_message called")
    data = await request.json()
    logging.info(f"data {data}")
    try:
        for entry in data.get("entry", []):
            logging.info(f"in cycle")
            for change in entry.get("changes", []):
                field = change.get("field")
                value = change.get("value") or {}
                logging.info(f"field {field}")
                logging.info(f"value {value}")

                # WhatsApp messages (array of messages)
                for message in value.get("messages", []):
                    sender_id = message.get("from")
                    text = message.get("text", {}).get("body")

                    logging.info(f"📥 WA-сообщение от {sender_id}: '{text}'")

                    if sender_id and text:
                        response_text = await get_deepseek_response(text)
                        await send_text_message(sender_id, response_text)

                if field == "comments":
                    comment_id = value.get("id")
                    parent_id = value.get("parent_id")
                    comment_text = value.get("text")
                    username = value.get("from", {}).get("username")

                    logging.info(f"💬 Новый комментарий от @{username}: '{comment_text}' (comment_id={comment_id}, parent_id={parent_id})")

                elif field == "live_comments":
                    comment_id = value.get("id")
                    comment_text = value.get("text")
                    username = value.get("from", {}).get("username")
                    media_id = value.get("media", {}).get("id")

                    logging.info(f"🎥 Live-комментарий от @{username}: '{comment_text}' (comment_id={comment_id}, media_id={media_id})")

                elif field == "mentions":
                    media_id = value.get("media_id")
                    comment_id = value.get("comment_id")

                    logging.info(f"🔔 Упоминание в комментарии {comment_id} (media_id={media_id})")

                elif field == "message_reactions":
                    sender_id = value.get("sender", {}).get("id")
                    reaction = value.get("reaction", {})
                    emoji = reaction.get("emoji")
                    reaction_type = reaction.get("reaction")
                    mid = reaction.get("mid")

                    logging.info(f"👍 Реакция от пользователя {sender_id}: '{reaction_type}' ({emoji}) на сообщение {mid}")

                elif field == "messages":
                    sender_id = value.get("sender", {}).get("id")
                    text = value.get("message", {}).get("text")

                    logging.info(f"📩 Сообщение от пользователя {sender_id}: '{text}'")

                    if sender_id and text:
                        response_text = await get_deepseek_response(text)
                        await send_text_message(sender_id, response_text)

                elif field == "messaging_handover":
                    sender_id = value.get("sender", {}).get("id")
                    pass_thread = value.get("pass_thread_control", {})
                    prev_app = pass_thread.get("previous_owner_app_id")
                    new_app = pass_thread.get("new_owner_app_id")
                    metadata = pass_thread.get("metadata")

                    logging.info(f"📤 Handover от {sender_id}: передача от {prev_app} к {new_app} (мета: {metadata})")

                elif field == "messaging_postbacks":
                    sender_id = value.get("sender", {}).get("id")
                    postback = value.get("postback", {})
                    title = postback.get("title")
                    payload_data = postback.get("payload")

                    logging.info(f"🔁 Postback от {sender_id}: кнопка '{title}' (payload: {payload_data})")

                elif field == "messaging_referral":
                    sender_id = value.get("sender", {}).get("id")
                    referral = value.get("referral", {})
                    ref = referral.get("ref")
                    source = referral.get("source")
                    ref_type = referral.get("type")

                    logging.info(f"🔗 Referral от {sender_id}: source={source}, type={ref_type}, ref={ref}")

                elif field == "messaging_seen":
                    sender_id = value.get("sender", {}).get("id")
                    recipient_id = value.get("recipient", {}).get("id")
                    timestamp = value.get("timestamp")
                    last_message_id = value.get("read", {}).get("mid")

                    logging.info(f"👀 Сообщение прочитано пользователем {sender_id} (получатель: {recipient_id}) — ID последнего прочитанного: {last_message_id}, время: {timestamp}")

                elif field == "standby":
                    logging.info("⏸ Вошли в режим ожидания (standby).")

                elif field == "story_insights":
                    media_id = value.get("media_id")
                    impressions = value.get("impressions")
                    reach = value.get("reach")
                    taps_forward = value.get("taps_forward")
                    taps_back = value.get("taps_back")
                    exits = value.get("exits")
                    replies = value.get("replies")

                    logging.info(
                        f"📊 Story insights (media_id: {media_id}) — "
                        f"Impressions: {impressions}, Reach: {reach}, "
                        f"Taps Forward: {taps_forward}, Back: {taps_back}, "
                        f"Exits: {exits}, Replies: {replies}"
                    )

    except Exception as e:
        logging.exception("❌ Ошибка при обработке webhook")

    return {"status": "ok"}

# Генерация ответа через DeepSeek (через httpx)
async def get_deepseek_response(user_message):
    url = "https://api.intelligence.io.solutions/api/v1/chat/completions"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_TOKEN}",
    }

    data = {
        "model": "deepseek-ai/DeepSeek-R1",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant"},
            {"role": "user", "content": user_message}
        ],
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(url, headers=headers, json=data)
            response.raise_for_status()
            response_data = response.json()
            text = response_data['choices'][0]['message']['content']
            bot_text = text.split('</think>\n\n')[1] if '</think>\n\n' in text else text
        except Exception as e:
            print(f"Ошибка DeepSeek: {e}")
            bot_text = "Произошла ошибка при получении ответа от нейросети."

    return bot_text

# Отправка сообщения в Instagram (через httpx)
async def send_text_message(to_id, message_text):
    url = "https://graph.facebook.com/v19.0/me/messages"
    params = {
        "access_token": ACCESS_TOKEN
    }
    data = {
        "messaging_type": "RESPONSE",
        "recipient": {"id": to_id},
        "message": {"text": message_text}
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(url, params=params, json=data)
            response.raise_for_status()
            print(f"Ответ от Instagram: {response.json()}")
        except Exception as e:
            print(f"Ошибка отправки сообщения в Instagram: {e}")






@app.post("/start_trial")
@exception_handler
async def start_trial(request: Request): 
    logging.info(f"start_trial called")
    verify_secret_code(request)

    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    logging.info(f"чекнули")

    user = await get_user_by_telegram_id(telegram_id)
    
    if not(user):
        return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и зарегистрируйтесь в боте"}
    if user.date_of_trial_ends:
        return {"status": "error", "message": "Ваш пробный период уже был использован"}

    await set_user_trial_end(telegram_id)
    notification_data = {
        "telegram_id": telegram_id,
    }
    send_invite_link_url = f"{str(await get_setting('MAHIN_URL'))}/send_invite_link"
    await send_request(send_invite_link_url, notification_data)
    
    return JSONResponse({"status": "success"})

@app.post("/fake_payment")
@exception_handler
async def fake_payment(request: Request): 
    logging.info(f"fake_payment called")
    verify_secret_code(request)

    data = await request.json()
    telegram_id = data.get("telegram_id")
    logging.info(f"telegram_id {telegram_id}")

    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    logging.info(f"чекнули")

    user = await get_user_by_telegram_id(telegram_id)
    
    if not(user):
        return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и зарегистрируйтесь в боте"}

    await set_user_fake_paid(telegram_id)
    notification_data = {
        "telegram_id": telegram_id,
    }
    send_invite_link_url = f"{str(await get_setting('MAHIN_URL'))}/send_invite_link"
    await send_request(send_invite_link_url, notification_data)
    
    return JSONResponse({"status": "success"})

@app.post("/delete_expired_users")
@exception_handler
async def delete_expired_users(): 
    logging.info(f"delete_expired_users called")

    expired_users = await get_expired_users()
    logging.info(f"expired_users {expired_users}")
    
    for user in expired_users:
        if not(user.paid) and user.date_of_trial_ends:
            notification_data = {
                "telegram_id": user.telegram_id,
            }
            kick_user_url = f"{str(await get_setting('MAHIN_URL'))}/kick_user"
            await send_request(kick_user_url, notification_data)
    
    return JSONResponse({"status": "success"})

@app.post("/get_payment_data")
@exception_handler
async def get_payment_data(request: Request): 
    logging.info(f"get_payment_data called")
    verify_secret_code(request)

    price = float(await get_setting("COURSE_AMOUNT"))
    logging.info(f"price {price}")
    raw = await get_setting("CARDS")
    logging.info(f"raw {raw}")

    # price = price + (random.randint(1, 100) / 100)

    cards = json.loads(raw)
    logging.info(f"cards {cards}")

    card_number = random.choice(cards)
    logging.info(f"price {price}")
    logging.info(f"card_number {card_number}")
    
    return JSONResponse({
        "status": "success",
        "price": price,
        "card_number": card_number
    })

@app.post("/leads")
async def create_lead_and_notify(request: Request):
    data = await request.json()
    name = data.get("name")
    email = data.get("email")
    phone = data.get("phone")

    if not (email and phone and name):
        return JSONResponse({"status": "error", "message": "Заполните все поля"}, status_code=400)

    try:
        lead_id = await create_lead(name, email, phone)
        # После успешного создания вызываем уведомления
        await _create_lead_and_notify_internal(name, email, phone, lead_id=lead_id)
        return JSONResponse({"status": "success", "lead_id": lead_id})
    except ValueError as e:
        # Дубликат лида
        return JSONResponse({"status": "error", "message": str(e)}, status_code=409)
    except Exception as e:
        logging.error(f"Ошибка при создании лида: {e}")
        return JSONResponse({"status": "error", "message": "Ошибка при создании лида"}, status_code=500)

@app.post("/set_pay_email")
async def set_pay_email(request: Request):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    email = data.get("email")
    action_type = data.get("action_type", "entered")  # "entered" или "confirmed"
    if not (telegram_id and email):
        return JSONResponse({"status": "error", "message": "Нужен telegram_id и email"}, status_code=400)
    if not is_valid_email(email):
        return JSONResponse({"status": "error", "message": "Некорректный email"}, status_code=400)
    await set_user_pay_email(telegram_id, email, action_type)
    return JSONResponse({"status": "success"})

@app.post("/get_pay_email")
async def get_pay_email(request: Request):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    if not telegram_id:
        return JSONResponse({"status": "error", "message": "Нужен telegram_id"}, status_code=400)
    email = await get_user_pay_email(telegram_id)
    return JSONResponse({"status": "success", "email": email})

@app.post("/get_payment_status")
async def get_payment_status(request: Request):
    data = await request.json()
    telegram_id = data.get("telegram_id")
    email = data.get("email")
    user = None
    if telegram_id:
        user = await get_user(telegram_id)
    if not user and email:
        # ищем по email
        from database import User, database
        from sqlalchemy import select as sql_select
        query = sql_select(User).where(User.pay_email == email)
        async with database.transaction():
            user = await database.fetch_one(query)
    status = "not set"
    if user:
        if user['paid']:
            status = "paid"
        elif user['pay_email']:
            status = "pending"
    return JSONResponse({"status": "success", "payment_status": status})

# --- Form Warm endpoints ---
@app.get("/form_warm/clients")
@exception_handler
async def fw_list_clients(
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    q: str | None = Query(None),
    name: str | None = Query(None),
    email: str | None = Query(None),
    phone: str | None = Query(None),
    notified: bool | None = Query(None),
    created_from: str | None = Query(None),  # ISO string
    created_to: str | None = Query(None),    # ISO string
    sort_by: str = Query('created_at'),
    sort_dir: str = Query('desc')
):
    logging.info("[FW] list_clients called")
    logging.info(f"[FW] params offset={offset} limit={limit} q={q} name={name} email={email} phone={phone} notified={notified} created_from={created_from} created_to={created_to} sort_by={sort_by} sort_dir={sort_dir}")
    cf = None
    ct = None
    try:
        if created_from:
            cf = datetime.fromisoformat(created_from)
        if created_to:
            ct = datetime.fromisoformat(created_to)
    except Exception as e:
        logging.error(f"[FW] Error parsing dates: {e}")
        cf = None
        ct = None

    try:
        rows = await get_leads(
            offset=offset,
            limit=limit,
            q=q,
            name=name,
            email=email,
            phone=phone,
            notified=notified,
            created_from=cf,
            created_to=ct,
            sort_by=sort_by,
            sort_dir=sort_dir
        )
        total = await get_leads_total_count(
            q=q,
            name=name,
            email=email,
            phone=phone,
            notified=notified,
            created_from=cf,
            created_to=ct
        )
        items = [{
            "id": r["id"],
            "name": r["name"],
            "email": r["email"],
            "phone": r["phone"],
            "telegram_id": r.get("telegram_id"),
            "username": r.get("username"),
            "created_at": str(r["created_at"]),
            "notified": bool(r["notified"])
        } for r in rows]
        logging.info(f"[FW] list_clients result total={total} items={len(items)}")
        return JSONResponse({"status": "success", "total": total, "items": items})
    except Exception as e:
        logging.error(f"[FW] Error in list_clients: {e}", exc_info=True)
        raise

@app.get("/form_warm/clients/{lead_id}")
async def fw_get_client(lead_id: int):
    logging.info(f"[FW] get_client lead_id={lead_id}")
    lead = await get_lead_by_id(lead_id)
    if not lead:
        logging.info("[FW] get_client not found")
        return JSONResponse({"status": "error", "message": "Лид не найден"}, status_code=404)
    logging.info(f"[FW] get_client ok email={lead['email']} phone={lead['phone']}")
    return JSONResponse({
        "status": "success",
        "lead": {
            "id": lead["id"],
            "name": lead["name"],
            "email": lead["email"],
            "phone": lead["phone"],
            "telegram_id": lead.get("telegram_id"),
            "username": lead.get("username")
        }
    })

@app.get("/form_warm/clients/{lead_id}/progress")
async def fw_get_progress(lead_id: int):
    logging.info(f"[FW] get_progress lead_id={lead_id}")
    rows = await get_lead_progress(lead_id)
    progress = [{"id": r["id"], "step": r["step"], "answer": r["answer"], "created_at": str(r["created_at"]) } for r in rows]
    logging.info(f"[FW] get_progress count={len(progress)}")
    return JSONResponse({"status": "success", "progress": progress})

@app.post("/form_warm/clients/{lead_id}/answers")
async def fw_post_answer(lead_id: int, request: Request):
    logging.info(f"[FW] post_answer lead_id={lead_id}")
    data = await request.json()
    step = data.get("step")
    answer = data.get("answer")
    logging.info(f"[FW] post_answer payload step={step} answer={answer}")
    if not step:
        logging.info("[FW] post_answer missing step")
        return JSONResponse({"status": "error", "message": "Не указан шаг"}, status_code=400)
    ok = await record_lead_answer(lead_id, step, answer or "")
    if not ok:
        logging.info("[FW] post_answer duplicate")
        return JSONResponse({"status": "error", "message": "Ответ уже зафиксирован для этого шага"}, status_code=409)
    logging.info("[FW] post_answer ok")
    return JSONResponse({"status": "success"})

@app.post("/form_warm/schedule_final_test")
@exception_handler
async def schedule_final_test(request: Request, background_tasks: BackgroundTasks):
    """Планирует отправку ссылки на финальный тест через указанное время после закрытия первого теста"""
    try:
        data = await request.json()
        lead_id = data.get("lead_id")
        delay_seconds = data.get("delay_seconds", 30)
        
        if not lead_id:
            return JSONResponse({"status": "error", "message": "lead_id не указан"}, status_code=400)
        
        logging.info(f"[FW] schedule_final_test lead_id={lead_id}, delay={delay_seconds}s")
        
        # Планируем отправку через указанное время
        background_tasks.add_task(_send_final_test_after_delay, lead_id, delay_seconds)
        
        return JSONResponse({"status": "success", "message": f"Финальный тест будет отправлен через {delay_seconds} секунд"})
    except Exception as e:
        logging.exception("Error in schedule_final_test")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

async def _send_final_test_after_delay(lead_id: int, delay_seconds: int):
    """Отправляет ссылку на финальный тест после задержки"""
    import asyncio
    await asyncio.sleep(delay_seconds)
    
    try:
        logging.info(f"[FW] Sending final test link for lead_id={lead_id}")
        
        # Получаем данные лида
        lead = await get_lead_by_id(lead_id)
        if not lead:
            logging.error(f"[FW] Lead {lead_id} not found")
            return
        
        name = lead.get("name", "")
        email = lead.get("email", "")
        phone = lead.get("phone", "")
        
        if not phone:
            logging.error(f"[FW] No phone for lead {lead_id}")
            return
        
        # URL финального теста
        final_test_url = "https://mind-testing.vercel.app/final.html"
        final_test_link = f"{final_test_url}?lead_id={lead_id}"
        
        # Составляем сообщение для WhatsApp
        wa_message = (
            f"Здравствуй, {name}!\n\n"
            f"🎯 Ты прошёл первый тест на мышление инженера ML!\n\n"
            f"Теперь у тебя есть возможность узнать:\n"
            f"📊 Стоит ли тебе купить курс?\n\n"
            f"Пройди финальный тест из 3 вопросов и получи персональный вердикт:\n"
            f"{final_test_link}\n\n"
            f"Это поможет тебе принять правильное решение! 💪"
        )
        
        wa_phone = normalize_and_validate_phone_for_whapi(phone)
        headers = {
            "Authorization": f"Bearer {WHAPI_TOKEN}",
            "Content-Type": "application/json"
        }
        payload = {
            "to": wa_phone,
            "body": wa_message
        }
        
        logging.info(f"[FW] Sending WhatsApp for lead {lead_id} to {wa_phone}")
        
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(WHAPI_URL, headers=headers, json=payload)
            resp.raise_for_status()
        
        logging.info(f"[FW] Final test link sent successfully for lead {lead_id}")
        
    except Exception as e:
        logging.exception(f"[FW] Error sending final test link for lead {lead_id}: {e}")

@app.put("/form_warm/clients/{lead_id}/answers")
async def fw_update_answer(lead_id: int, request: Request):
    logging.info(f"[FW] update_answer lead_id={lead_id}")
    data = await request.json()
    step = data.get("step")
    answer = data.get("answer")
    logging.info(f"[FW] update_answer payload step={step} answer={answer}")
    if not step:
        logging.info("[FW] update_answer missing step")
        return JSONResponse({"status": "error", "message": "Не указан шаг"}, status_code=400)
    ok = await update_lead_answer(lead_id, step, answer or "")
    if not ok:
        logging.info("[FW] update_answer not found")
        return JSONResponse({"status": "error", "message": "Ответ не найден для этого шага"}, status_code=404)
    logging.info("[FW] update_answer ok")
    return JSONResponse({"status": "success"})

# === Chat Endpoints ===

# Загрузка данных сайта из data.txt
def load_site_data():
    """Загружает данные о сайте из файла data.txt"""
    import os
    data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data.txt")
    try:
        with open(data_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        logging.warning(f"data.txt not found at {data_path}, using default")
        return "Информация о курсе 'Машинное обучение без математики'."

SITE_DATA = load_site_data()

# Функция для запроса к DeepSeek для чата
async def get_chat_deepseek_response(user_message: str, chat_history: list = None):
    """Генерирует ответ через DeepSeek с температурой 0 и призывом к краткости"""
    try:
        import asyncio
        from openai import OpenAI
        
        # Проверяем наличие токена
        if not DEEPSEEK_TOKEN:
            logging.error("DEEPSEEK_TOKEN не установлен")
            return "Произошла ошибка конфигурации. Попробуйте позже."
        
        # Формируем системный промпт с данными о сайте
        system_prompt = (
            f"Ты - AI-помощник курса 'Машинное обучение без математики'. "
            f"Используй ТОЛЬКО эти данные для ответа:\n\n{SITE_DATA}\n\n"
            f"ПРАВИЛА ОТВЕТОВ:\n"
            f"1. Отвечай КРАТКО (максимум 3-4 предложения)\n"
            f"2. Отвечай КОНКРЕТНО на вопрос клиента, используя данные из каталога\n"
            f"3. Температура: 1 (отвечай строго по данным)\n"
            f"4. НЕ выдумывай информацию\n"
            f"5. Если вопрос не по теме курса - пошути и кратко переведи в тему курса\n"
            f"6. БЕЗ markdown разметки в ответе"
        )
        
        # Формируем сообщения для контекста
        messages = [{"role": "system", "content": system_prompt}]
        
        # Добавляем историю (если есть) - последние 5 сообщений для контекста
        if chat_history:
            for msg in chat_history[-5:]:
                role = "user" if msg["is_from_user"] else "assistant"
                messages.append({"role": role, "content": msg["message"]})
        
        # Добавляем текущее сообщение
        messages.append({"role": "user", "content": user_message})
        
        # Создаем клиент OpenAI для DeepSeek
        client = OpenAI(
            api_key=DEEPSEEK_TOKEN,
            base_url="https://api.deepseek.com/v1"
        )
        
        # Выполняем запрос к DeepSeek API в отдельном потоке (так как клиент синхронный)
        def make_request():
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=messages,
                temperature=1
            )
            return response.choices[0].message.content
        
        # Запускаем синхронный запрос в executor
        loop = asyncio.get_event_loop()
        text = await loop.run_in_executor(None, make_request)
        
        # Убираем reasoning если есть
        if '</think>\n\n' in text:
            bot_text = text.split('</think>\n\n')[1]
        elif '</think>\n\n' in text:
            bot_text = text.split('</think>\n\n')[1]
        else:
            bot_text = text
        
        return bot_text.strip()
        
    except Exception as e:
        logging.error(f"Ошибка DeepSeek для чата: {e}")
        logging.exception("Полная трассировка ошибки")
        return "Произошла ошибка при получении ответа. Попробуйте позже."

@app.post("/api/chat/send")
async def chat_send(request: Request):
    """Эндпоинт для отправки сообщения в чат"""
    try:
        data = await request.json()
        session_id = data.get("session_id")
        message = data.get("message", "").strip()
        
        if not session_id:
            return JSONResponse({"error": "session_id is required"}, status_code=400)
        
        if not message:
            return JSONResponse({"error": "message is required"}, status_code=400)
        
        # Проверка краткости на стороне сервера (200 символов)
        if len(message) > 200:
            return JSONResponse({
                "message": "Пожалуйста, задавайте краткие вопросы (до 200 символов). Будьте лаконичны!"
            })
        
        # Получаем историю для контекста из переданных данных (если есть) или из БД
        # Проверяем лимит сообщений из переданной истории или из БД
        chat_history_for_context = data.get("chat_history", [])
        
        # Подсчитываем количество сообщений пользователя из переданной истории
        user_messages_count = len([m for m in chat_history_for_context if m.get("is_from_user", True)])
        # Если есть сохраненные сообщения на сервере, учитываем их
        message_count_db = await get_chat_message_count(session_id)
        total_message_count = user_messages_count + message_count_db
        
        if total_message_count >= 10:
            return JSONResponse({
                "message": 'Лимит сообщений исчерпан (10 сообщений). Нажмите на кнопку "Демо курса" и сможете задать вопросы напрямую автору курса)'
            })
        
        # НЕ сохраняем сообщение на сервер - только в localStorage до выхода
        # Используем переданную историю для контекста, если она есть
        if chat_history_for_context:
            history_for_ai = chat_history_for_context[-10:]  # Последние 10 для контекста
        else:
            # Если история не передана, берем из БД (для обратной совместимости)
            history = await get_chat_history(session_id, limit=10)
            history_for_ai = [
                {
                    "message": msg["message"],
                    "is_from_user": msg["is_from_user"]
                }
                for msg in history
            ]
        
        # Получаем ответ от DeepSeek
        response_text = await get_chat_deepseek_response(message, history_for_ai)
        
        # НЕ сохраняем ответ AI на сервер - только в localStorage до выхода
        
        return JSONResponse({"response": response_text})
        
    except Exception as e:
        logging.exception("Ошибка в chat_send")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/chat/history")
async def chat_history(request: Request, session_id: str = Query(...)):
    """Эндпоинт для получения истории чата"""
    try:
        history = await get_chat_history(session_id, limit=50)
        messages = [
            {
                "message": msg["message"],
                "is_from_user": msg["is_from_user"],
                "created_at": msg["created_at"].isoformat() if msg.get("created_at") else None
            }
            for msg in history
        ]
        return JSONResponse({"messages": messages})
    except Exception as e:
        logging.exception("Ошибка в chat_history")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/chat/save_history")
async def save_chat_history(request: Request):
    """Эндпоинт для массового сохранения истории чата (используется при выходе или отправке формы)"""
    try:
        data = await request.json()
        session_id = data.get("session_id")
        chat_history = data.get("chat_history", [])
        
        if not session_id:
            return JSONResponse({"error": "session_id is required"}, status_code=400)
        
        if not chat_history or not isinstance(chat_history, list):
            return JSONResponse({"error": "chat_history is required and must be a list"}, status_code=400)
        
        # Получаем существующие сообщения на сервере для этой сессии
        existing_messages = await get_chat_history(session_id, limit=1000)
        existing_messages_set = set()
        for msg in existing_messages:
            # Создаем уникальный ключ для сообщения
            key = (msg["message"], msg["is_from_user"])
            existing_messages_set.add(key)
        
        # Сохраняем только новые сообщения (которых еще нет на сервере)
        saved_count = 0
        for msg in chat_history:
            if isinstance(msg, dict) and "message" in msg:
                message_text = msg.get("message", "").strip()
                is_from_user = msg.get("is_from_user", True)
                
                # Проверяем, нет ли такого сообщения уже на сервере
                key = (message_text, is_from_user)
                if key not in existing_messages_set:
                    await save_chat_message(
                        session_id=session_id,
                        message=message_text,
                        is_from_user=is_from_user
                    )
                    existing_messages_set.add(key)  # Добавляем в set чтобы не дублировать в этой сессии
                    saved_count += 1
        
        logging.info(f"Saved {saved_count} new chat messages for session_id={session_id} (total in request: {len(chat_history)})")
        
        return JSONResponse({
            "status": "success",
            "saved_count": saved_count,
            "total_count": len(chat_history)
        })
        
    except Exception as e:
        logging.exception("Ошибка в save_chat_history")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/chat/all_sessions")
async def get_all_chat_sessions(request: Request, limit: int = Query(100), offset: int = Query(0)):
    """Эндпоинт для получения всех сессий чата (для CRM)"""
    try:
        from database import ChatMessage, database
        from sqlalchemy import distinct
        
        # Получаем уникальные session_id
        from database import ChatMessage
        query = """
            SELECT DISTINCT session_id 
            FROM chat_messages 
            ORDER BY session_id
            LIMIT ? OFFSET ?
        """
        
        async with database.transaction():
            rows = await database.fetch_all(query, [limit, offset])
        
        # Для каждой сессии получаем количество сообщений и последнее сообщение
        result = []
        for row in rows:
            session_id = row["session_id"]
            history = await get_chat_history(session_id, limit=1)
            message_count = await get_chat_message_count(session_id)
            
            result.append({
                "session_id": session_id,
                "message_count": message_count,
                "last_message": history[-1]["message"] if history else None,
                "last_activity": history[-1]["created_at"].isoformat() if history and history[-1].get("created_at") else None
            })
        
        return JSONResponse({"sessions": result})
    except Exception as e:
        logging.exception("Ошибка в get_all_chat_sessions")
        return JSONResponse({"error": str(e)}, status_code=500)