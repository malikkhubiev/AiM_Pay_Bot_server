from loader import *
from utils import *
import os
from databases import Database
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from config import (
    BOT_USERNAME,
    DATABASE_URL
)
import logging
from database import (
    get_temp_user,
    save_invite_link_db,
    create_user,
    create_referral,
    update_temp_user,
    get_referred_user,
    create_temp_user,
    get_all_paid_money,
    get_referral_count,
    get_paid_count,
    get_referrer,
    get_all_referred
)

@app.post("/check_user")
@exception_handler
async def check_user(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")
    to_throw = data.get("to_throw", True)
    user = await get_user_by_telegram_id(telegram_id, to_throw)
    return {"status": "success", "user": user}

@app.post("/save_invite_link")
@exception_handler
async def save_invite_link(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")
    invite_link = data.get("invite_link")

    logging.info(f"Получены данные: telegram_id={telegram_id}, invite_link={invite_link}")

    check = check_parameters(telegram_id=telegram_id, invite_link=invite_link)
    logging.info(f"check = {check}")
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}

    logging.info(f"checknuli")
    await save_invite_link_db(telegram_id, invite_link)
    return {"status": "success"}

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
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    logging.info(f"user есть {user}")
    if user:
        return_data["response_message"] = f"Привет, {user.username}! Я тебя знаю. Ты участник AiM course!"
        return_data["type"] = "user"
        logging.info(f"user есть")
        if not(user.paid):
            logging.info(f"user не платил")
            return_data["to_show"] = "pay_course"
        return JSONResponse(return_data)
    else:
        return_data["type"] = "temp_user"
        logging.info(f"Юзера нет")
        return_data["response_message"] = f"Добро пожаловать, {username}!"
        temp_user = await get_temp_user(telegram_id=telegram_id)
        if temp_user:
            logging.info(f"Есть только временный юзер. Обновляем")
            await update_temp_user(telegram_id=telegram_id, username=username)
        else:
            logging.info(f"Делаем временный юзер")
            logging.info(f"telegram_id {telegram_id}")
            logging.info(f"username {username}")
            logging.info(f"referrer_id {referrer_id}")
            await create_temp_user(telegram_id=telegram_id, username=username, referrer_id=referrer_id)
    if referrer_id and referrer_id != telegram_id:
        logging.info(f"Есть реферрал и сам себя не привёл")
        existing_referrer = await get_referrer(telegram_id)
        if existing_referrer:
            logging.info(f"Реферал уже был")
            existing_referrer.referrer_id = referrer_id
        else:
            logging.info(f"Реферала ещё не было")
            referrer_user = await get_user_by_telegram_id(referrer_id, to_throw=False)
            if referrer_user and referrer_user.paid:
                logging.info(f"Пользователь который привёл есть и он оплатил курс")
                await create_referral(telegram_id, referrer_id)
                logging.info(f"Сделали реферала в бд")
    return JSONResponse(return_data)

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

    temp_user = await get_temp_user(telegram_id)
    logging.info(f"temp_user {temp_user}")
    if temp_user:
        logging.info(f"Есть временный юзер")
        username = temp_user.username
        referrer_id = temp_user.referrer_id
        logging.info(f"У него есть username {username}")
        logging.info(f"У него есть referrer_id {referrer_id}")
        await create_user(telegram_id, username)
        await create_referral(telegram_id, referrer_id)
        logging.info(f"Получены данные: telegram_id={telegram_id}, username={username}, referrer_id={referrer_id}")
        logging.info(f"Пользователь {username} зарегистрирован {'с реферальной ссылкой' if referrer_id else 'без реферальной ссылки'}.")
    
        return JSONResponse({"status": "success"})

@app.post("/generate_overview_report")
@exception_handler
async def generate_overview_report(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")
    
    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    # Находим пользователя
    user = await get_user_by_telegram_id(telegram_id)

    # Calculate total paid money
    all_paid_money = await get_all_paid_money(telegram_id)
    referral_count = await get_referral_count(telegram_id)
    paid_count = await get_paid_count(telegram_id)

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
    logging.info(f"user есть")

    referred_details = await get_all_referred(user.telegram_id)

    logging.info(f"detales есть")
    logging.info(f"{referred_details} referred_details")
    # Extract referral data and calculate statistics
    invited_list = []
    logging.info(f"invited_list {invited_list}")

    if referred_details:
        logging.info(f" referral {referred_details}")
        for referral in referred_details:
            logging.info(f"referral есть и вот он: {referral}")
            attributes = {column.key: getattr(referral, column.key) for column in referral.__mapper__.column_attrs}
            logging.info(f"Referral attributes: {attributes}")
            referred_user = await get_referred_user(referral.referred_id)
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
    if not(user.paid):
        return {"status": "error", "message": "Вы не можете стать партнёром по реферальной программе, не оплатив курс"}
    if not(user.card_synonym):
        return {"status": "error", "message": "Вы не можете стать партнёром по реферальной программе, не привязав карту"}
    
    referral_link = f"https://t.me/{BOT_USERNAME}?start={telegram_id}"
    return {"status": "success", "referral_link": referral_link}

@app.post("/get_invite_link")
@exception_handler
async def get_invite_link(request: Request):
    verify_secret_code(request)
    data = await request.json()
    telegram_id = data.get("telegram_id")
    
    check = check_parameters(telegram_id=telegram_id)
    if not(check["result"]):
        return {"status": "error", "message": check["message"]}
    
    user = await get_user_by_telegram_id(telegram_id)
    if not(user.invite_link):
        return {"status": "error", "message": "Вы не можете получить пригласительную ссылку, не оплатив курс"}
    
    return {"status": "success", "invite_link": user.invite_link}