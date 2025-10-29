import os
import pandas as pd
from pdf2image import convert_from_path
from responses import *
from fastapi import HTTPException, Request, status
import ipaddress
from dotenv import load_dotenv
from functools import wraps
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
import httpx
import logging
from config import (
    YOOKASSA_SECRET_KEY,
    YOOKASSA_PAYOUT_KEY,
    YOOKASSA_AGENT_ID,
    YOOKASSA_SHOP_ID,
    SECRET_CODE,
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASSWORD,
    FROM_EMAIL,
    SMTP_TIMEOUT,
    EMAIL_PROVIDER,
    RESEND_API_KEY,
    RESEND_FROM
)
from yookassa import Configuration
import logging
from database import (
    get_user
)
import smtplib
import ssl
from email.message import EmailMessage
import asyncio

load_dotenv()

# Настроим логирование
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('aiosqlite').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

def exception_handler(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            logging.info("inside wrapper of exception handler")
            return await func(*args, **kwargs)
        except HTTPException as e:
            logging.error(f"HTTP error: {e.detail}")
            return JSONResponse({"status": "error", "message": e.detail}, status_code=e.status_code)
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return JSONResponse({"status": "error", "message": "Internal server error"}, status_code=500)
    return wrapper

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
    secret_code = request.headers.get("X-Secret-Code")
    if secret_code != SECRET_CODE:
        raise HTTPException(status_code=403, detail="Вам запрещён доступ к серверу")
    return True
        

def switch_configuration(account_id, secret_key):
    logging.info(f"{account_id}, {secret_key}")
    Configuration.configure(account_id, secret_key)

def setup_payment_config():
    """Настроить конфигурацию для платежей."""
    switch_configuration(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY)

def setup_payout_config():
    """Настроить конфигурацию для выплат."""
    switch_configuration(YOOKASSA_AGENT_ID, YOOKASSA_PAYOUT_KEY)

# Функция для проверки обязательных параметров
def check_parameters(**kwargs):
    missing_params = [param for param, value in kwargs.items() if value is None]
    if missing_params:
        logging.info(f"Не указаны следующие необходимые параметры: {', '.join(missing_params)}")
        return {"result": False, "message": "Введите команду /start и используйте кнопки для навигации"}
    return {"result": True}

async def get_user_by_telegram_id(telegram_id: str, to_throw: bool = True):
    logging.info(f"in get_user_by_telegram_id telegram_id = {telegram_id}")
    user = await get_user(telegram_id)
    if not(user):
        if to_throw:
            raise HTTPException(status_code=404, message="Пользователь не найден")
        else:
            return None
    return user

async def send_request(url: str, data: dict, method: str = "POST") -> dict:
    """Универсальная функция для отправки HTTP-запросов с обработкой ошибок."""
    try:
        async with httpx.AsyncClient() as client:
            if method.upper() == "POST":
                response = await client.post(url, json=data)
            elif method.upper() == "GET":
                response = await client.get(url, params=data)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()  # Проверка на ошибки HTTP

            logging.info(f"Запрос на {url} успешно отправлен.")
            return response.json()  # Возвращаем данные ответа в формате JSON

    except httpx.RequestError as e:
        logging.error(f"Ошибка при отправке запроса: {e}")
        raise HTTPException(status_code=500, message="Request failed")

    except httpx.HTTPStatusError as e:
        logging.error(f"Ошибка HTTP при отправке запроса: {e}")
        raise HTTPException(status_code=500, message=f"HTTP error: {e.response.status_code}")

    except Exception as e:
        logging.error(f"Неизвестная ошибка: {e}")
        raise HTTPException(status_code=500, message="An unknown error occurred")

def format_datetime(dt):
    return dt.strftime("%d.%m.%Y [%H:%M:%S]")

def format_timedelta(td):
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    time_str = []

    if days > 0:
        day_label = "день" if days == 1 else "дня" if 2 <= days <= 4 else "дней"
        time_str.append(f"{days} {day_label}")
    if hours > 0:
        hour_label = "час" if hours == 1 else "часа" if 2 <= hours <= 4 else "часов"
        time_str.append(f"{hours} {hour_label}")
    if minutes > 0:
        minute_label = "минута" if minutes == 1 else "минуты" if 2 <= minutes <= 4 else "минут"
        time_str.append(f"{minutes} {minute_label}")
    if seconds > 0:
        second_label = "секунда" if seconds == 1 else "секунды" if 2 <= seconds <= 4 else "секунд"
        time_str.append(f"{seconds} {second_label}")
    
    return ", ".join(time_str)

def format_datetime_for_excel(dt):
    """Преобразует datetime в формат, подходящий для Excel."""
    if dt is None:
        return None
    return pd.Timestamp(dt)  # Pandas автоматически конвертирует в Excel-friendly формат

def format_datetime_for_excel(dt):
    """Преобразует datetime в формат, подходящий для Excel."""
    if dt is None:
        return None
    return pd.Timestamp(dt)  # Pandas автоматически конвертирует в Excel-friendly формат

async def convert_pdf_to_image(pdf_path):
    # Преобразуем первую страницу PDF в изображение
    images = convert_from_path(pdf_path, first_page=1, last_page=1)
    
    # Сохраняем изображение в формате PNG
    image_path = pdf_path.replace('.pdf', '.png')  # Сохраняем как PNG
    images[0].save(image_path, 'PNG')
    
    return image_path

def send_email_sync(to_email: str, subject: str, html_body: str, text_body: str = None):
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASSWORD and FROM_EMAIL):
        raise HTTPException(status_code=500, detail="SMTP is not configured on the server")

    message = EmailMessage()
    message["From"] = FROM_EMAIL
    message["To"] = to_email
    message["Subject"] = subject

    if text_body:
        message.set_content(text_body)
        message.add_alternative(html_body, subtype="html")
    else:
        message.set_content(html_body, subtype="html")

    context = ssl.create_default_context()
    try:
        logger.debug(f"SMTP prepare: host={SMTP_HOST} port={SMTP_PORT} from={FROM_EMAIL} to={to_email} subject={subject}")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT) as server:
            # Enable low-level SMTP protocol logs to stderr (captured by process logs)
            server.set_debuglevel(1)
            logger.debug("SMTP connection opened")
            server.starttls(context=context)
            logger.debug("SMTP STARTTLS negotiated")
            server.login(SMTP_USER, SMTP_PASSWORD)
            logger.debug("SMTP authenticated successfully")
            server.send_message(message)
            logger.info(f"SMTP message sent to {to_email}")
    except Exception as e:
        logger.exception(f"SMTP send error: {e}")
        raise HTTPException(status_code=500, detail="Failed to send email")

async def send_email_async(to_email: str, subject: str, html_body: str, text_body: str = None):
    logger.info(f"send_email_async scheduled for {to_email} via {EMAIL_PROVIDER}")
    try:
        if EMAIL_PROVIDER == "RESEND":
            await send_email_via_resend(to_email, subject, html_body, text_body)
        else:
            # Default: SMTP (same sync function in a thread)
            await asyncio.to_thread(send_email_sync, to_email, subject, html_body, text_body)
        logger.info(f"send_email_async completed for {to_email}")
    except Exception as e:
        logger.exception(f"send_email_async failed for {to_email}: {e}")

async def send_email_via_resend(to_email: str, subject: str, html_body: str, text_body: str = None):
    if not RESEND_API_KEY:
        raise HTTPException(status_code=500, detail="RESEND_API_KEY is not configured on the server")

    payload = {
        "from": RESEND_FROM,
        "to": [to_email],
        "subject": subject,
        "html": html_body,
    }
    if text_body:
        payload["text"] = text_body

    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json"
    }

    logger.debug(f"RESEND prepare: from={RESEND_FROM} to={to_email} subject={subject}")
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post("https://api.resend.com/emails", headers=headers, json=payload)
            if response.is_error:
                logger.error(f"Resend API error {response.status_code}: {response.text}")
                raise HTTPException(status_code=502, detail="Resend API error")
            logger.info(f"Resend message accepted for {to_email}: {response.text}")
        except Exception as e:
            logger.exception(f"Resend send error: {e}")
            raise