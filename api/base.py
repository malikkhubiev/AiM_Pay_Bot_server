from loader import *
from utils import *
import plotly.graph_objects as go
import plotly.io as pio
from fastapi import BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from config import (
    BOT_USERNAME,
    SERVER_URL,
    MAHIN_URL,
    REFERRAL_AMOUNT,
    PROMO_NUM_LIMIT
)
import logging
from database import (
    get_temp_user,
    get_referral_statistics,
    get_payment_date,
    get_start_working_date,
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
    get_user_by_unique_str,
    get_paid_referrals_by_user,
)
import pandas as pd
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
        if not(promo_user) and number_of_promo < int(PROMO_NUM_LIMIT):
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

        chart_url = f"{SERVER_URL}/referral_chart/{unique_str}"
        logging.info(f"chart_url {chart_url}")
        return JSONResponse({
            "status": "success",
            "data": {
                "chart_url": chart_url
            }
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

    # Создаем график
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=list(referral_data.keys()), y=list(referral_data.values()), mode='lines+markers', name='Рефералы'))
    fig.update_layout(title="Оплатившие рефералы по дням", xaxis_title="Дата", yaxis_title="Количество")

    # Генерируем HTML
    html_content = pio.to_html(fig, full_html=True, include_plotlyjs='cdn')
    return HTMLResponse(html_content)

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
