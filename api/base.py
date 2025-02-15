from loader import *
from utils import *
from fastapi.responses import JSONResponse
from config import (
    BOT_USERNAME,
    MAHIN_URL,
    REFERRAL_AMOUNT,
    PROMO_NUM_LIMIT
)
import logging
from database import (
    get_temp_user,
    get_referral_statistics,
    save_invite_link_db,
    get_promo_users_count,
    get_payments_frequency_db,
    create_user,
    create_referral,
    get_pending_referrer,
    update_temp_user,
    get_referred_user,
    create_temp_user,
    get_all_paid_money,
    get_paid_count,
    update_referrer,
    get_all_referred,
    get_promo_user,
    get_promo_user_count,
    add_promo_user,
)
from datetime import datetime, timezone, timedelta

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
        "with_promo": None,
        "type": None
    }
    user = await get_user_by_telegram_id(telegram_id, to_throw=False)
    logging.info(f"user есть {user}")
    temp_user = None
    if user:
        return_data["response_message"] = f"Привет, {user.username}! Я тебя знаю. Ты участник AiM course!"
        return_data["type"] = "user"
        logging.info(f"user есть")
        if not(user.paid):
            logging.info(f"user не платил")
            return_data["to_show"] = "pay_course"
        
        promo_user = await get_promo_user(user.telegram_id)
        number_of_promo = await get_promo_user_count() 
        logging.info(f"promo_num_left = {int(PROMO_NUM_LIMIT) - number_of_promo}")
        if not(promo_user) and number_of_promo < int(PROMO_NUM_LIMIT):
            return_data["with_promo"] = True

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
            logging.info(f"referrer_id {referrer_id}")
            temp_user = await create_temp_user(telegram_id=telegram_id, username=username, referrer_id=referrer_id)
    
    logging.info(f"referrer_id {referrer_id}")
    logging.info(f"referrer_id != telegram_id {referrer_id != telegram_id}")
    logging.info(f"temp_user {temp_user}")
    logging.info(f"user {user}")
    if user:
        logging.info(f"not(user.paid) {not(user.paid)}")
    
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

    if user:
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
        referrer_id = temp_user.referrer_id
        logging.info(f"У него есть username {username}")
        logging.info(f"У него есть referrer_id {referrer_id}")
        await create_user(telegram_id, username)
        logging.info(f"Получены данные: telegram_id={telegram_id}, username={username}, referrer_id={referrer_id}")
        logging.info(f"Пользователь {username} зарегистрирован {'с реферальной ссылкой' if referrer_id else 'без реферальной ссылки'}.")

        promo_user = await get_promo_user(telegram_id)
        number_of_promo = await get_promo_user_count() 
        if not(promo_user) and number_of_promo <= 1000:
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
    if number_of_promo < int(PROMO_NUM_LIMIT):  
        await add_promo_user(telegram_id)
        notification_data = {"telegram_id": telegram_id}
        send_invite_link_url = f"{MAHIN_URL}/send_invite_link"
        await send_request(send_invite_link_url, notification_data)

        return JSONResponse({"status": "success"})
    else:
        return JSONResponse({
            "status": "error",
            "message": "Лимит пользователей, которые могут зарегистрироваться по промокоду, исчерпан"
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
    
    if not(user):
        return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и нажмите на кнопку 'Начало работы' для регистрации в боте"}

    logging.info(f"user есть")

    referred_details = await get_all_referred(user.telegram_id)

    logging.info(f"detales есть")
    logging.info(f"{referred_details} referred_details")
    # Extract referral data and calculate statistics
    invited_list = []
    logging.info(f"invited_list {invited_list}")

    if referred_details:
        logging.info("Referral details found.")  # Логируем только факт наличия рефералов
        for referral in referred_details:
            logging.debug(f"Processing referral: {referral}")  # Логируем только сам процесс обработки
            attributes = vars(referral)
            logging.debug(f"Referral attributes: {attributes}")  # Логируем атрибуты реферала, если это нужно
            
            referred_user = await get_referred_user(referral.referred_id)
            
            if referred_user:
                logging.info(f"Referred user found: {referred_user.telegram_id}")  # Информация о пользователе, если он найден
                invited_list.append({
                    "telegram_id": referred_user.telegram_id,
                    "username": referred_user.username
                })
                logging.debug(f"Invited list updated: {invited_list}")  # Логируем только обновление списка, если нужно
            else:
                logging.warning(f"Referred user with ID {referral.referred_id} not found.")  # Логируем предупреждение, если пользователь не найден


    logging.info(f"invited_list {invited_list} когда вышли")

    # Calculate total paid money
    all_paid_money = await get_all_paid_money(telegram_id)
    paid_count = await get_paid_count(telegram_id)

    # Generate the report
    report = {
        "username": user.username,
        "paid_count": paid_count,
        "total_payout": all_paid_money,
        "invited_list": invited_list,
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
    referral_statistics = await get_referral_statistics()

    logging.info(f"referral_statistics {referral_statistics}")

    total_balance = 0
    users = []

    for user in referral_statistics:
        payout_amount = user['paid_referrals'] * float(REFERRAL_AMOUNT)
        total_balance += payout_amount
        users.append({
            "id": user["telegram_id"],
            "name": user["username"],
            "paid_referrals": user['paid_referrals']
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
    promo_num_left = int(PROMO_NUM_LIMIT) - number_of_promo

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
# @app.post("/get_invite_link")
# @exception_handler
# async def get_invite_link(request: Request):
#     verify_secret_code(request)
#     data = await request.json()
#     telegram_id = data.get("telegram_id")
    
#     check = check_parameters(telegram_id=telegram_id)
#     if not(check["result"]):
#         return {"status": "error", "message": check["message"]}
    
#     user = await get_user_by_telegram_id(telegram_id)

#     if not(user):
#         return {"status": "error", "message": "Вы ещё не зарегистрированы. Введите команду /start, прочитайте документы и нажмите на кнопку 'Начало работы' для регистрации в боте"}
#     if not(user.paid):
#         return {"status": "error", "message": "Вы не можете получить пригласительную ссылку, не оплатив курс"}
    
#     return {"status": "success", "invite_link": user.invite_link}
