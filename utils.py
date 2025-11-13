import os
import pandas as pd
from pdf2image import convert_from_path
from responses import *
from fastapi import HTTPException, Request, status
import ipaddress
from dotenv import load_dotenv
from functools import wraps
from fastapi.responses import JSONResponse
import httpx
import logging
from config import (
    YOOKASSA_SECRET_KEY,
    YOOKASSA_PAYOUT_KEY,
    YOOKASSA_AGENT_ID,
    YOOKASSA_SHOP_ID,
    SECRET_CODE,
    METRICS_GOAL,
    YANDEX_METRIKA_ID,
    RESEND_FROM,
    RESEND_API_1,
    RESEND_API_2,
    RESEND_API_3
)
from yookassa import Configuration
import logging
from database import (
    get_user
)
import asyncio
import re

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
            response = JSONResponse({"status": "error", "message": e.detail}, status_code=e.status_code)
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = "*"
            response.headers["Access-Control-Allow-Headers"] = "*"
            return response
        except Exception as e:
            logging.error(f"Unexpected error: {e}", exc_info=True)
            response = JSONResponse({"status": "error", "message": "Internal server error"}, status_code=500)
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = "*"
            response.headers["Access-Control-Allow-Headers"] = "*"
            return response
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
    """Проверяет IP-адрес запроса на соответствие разрешенным IP YooKassa.
    Учитывает заголовки X-Forwarded-For и X-Real-IP для работы за прокси.
    """
    try:
        # Получаем IP из заголовков (если запрос идет через прокси)
        forwarded_for = request.headers.get("X-Forwarded-For")
        real_ip = request.headers.get("X-Real-IP")
        client_ip = request.client.host if request.client else None
        
        # Определяем реальный IP клиента
        # X-Forwarded-For может содержать список IP через запятую
        if forwarded_for:
            # Берем первый IP из списка (реальный клиент)
            client_ip = forwarded_for.split(",")[0].strip()
            logging.info(f"IP from X-Forwarded-For: {client_ip}")
        elif real_ip:
            client_ip = real_ip.strip()
            logging.info(f"IP from X-Real-IP: {client_ip}")
        elif client_ip:
            logging.info(f"IP from request.client.host: {client_ip}")
        else:
            logging.error("Could not determine client IP address")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Bad Request: Could not determine client IP address"
            )
        
        # Преобразуем IP в объект для проверки
        try:
            client_ip_address = ipaddress.ip_address(client_ip)
        except ValueError as e:
            logging.error(f"Invalid IP address format: {client_ip}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Bad Request: Invalid IP address format {client_ip}"
            )

        # Проверяем, входит ли IP-адрес в разрешённые диапазоны
        for allowed_ip_range in ALLOWED_YOOKASSA_IP_RANGES:
            if isinstance(allowed_ip_range, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
                if client_ip_address in allowed_ip_range:
                    logging.info(f"IP {client_ip} is in allowed range {allowed_ip_range}")
                    return client_ip  # Возвращаем IP, если он валиден
            elif isinstance(allowed_ip_range, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
                if client_ip_address == allowed_ip_range:
                    logging.info(f"IP {client_ip} matches allowed IP {allowed_ip_range}")
                    return client_ip  # Возвращаем IP, если он валиден

        # Если IP не разрешён, выбрасываем исключение
        logging.warning(f"IP {client_ip} is not in allowed YooKassa IP ranges")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Forbidden: Invalid IP address {client_ip}"
        )

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error checking YooKassa IP: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error while checking IP: {str(e)}"
        )

def verify_secret_code(request: Request):
    secret_code = request.headers.get("X-Secret-Code")
    if secret_code != SECRET_CODE:
        raise HTTPException(status_code=403, detail="Вам запрещён доступ к серверу")
    return True

def is_valid_email(email):
    # примитивный, но разумный валидатор
    return re.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", email) is not None

def normalize_and_validate_phone(phone: str) -> str:
    """Normalize input phone to digits (E.164 without '+').
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
            raise HTTPException(status_code=404, detail="Пользователь не найден")
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
        raise HTTPException(status_code=500, detail="Request failed")

    except httpx.HTTPStatusError as e:
        logging.error(f"Ошибка HTTP при отправке запроса: {e}")
        raise HTTPException(status_code=500, detail=f"HTTP error: {e.response.status_code}")

    except Exception as e:
        logging.error(f"Неизвестная ошибка: {e}")
        raise HTTPException(status_code=500, detail="An unknown error occurred")

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

async def send_email_async(to_email: str, subject: str, html_body: str, text_body: str = None):
    """Отправка email через Resend API с перебором ключей.
    Использует RESEND_API_1, RESEND_API_2, RESEND_API_3 по очереди до успешной отправки.
    """
    logger.info(f"send_email_async scheduled for {to_email} via Resend")
    try:
        await send_email_via_resend(to_email, subject, html_body, text_body)
        logger.info(f"send_email_async completed for {to_email}")
    except Exception as e:
        logger.exception(f"send_email_async failed for {to_email}: {e}")

async def send_email_via_resend(to_email: str, subject: str, html_body: str, text_body: str = None):
    """Отправка письма через Resend API с перебором ключей.
    Пробует RESEND_API_1, RESEND_API_2, RESEND_API_3 по очереди до успешной отправки.
    """
    from_email = RESEND_FROM or "01_AiM_01@mail.ru"
    api_keys = [RESEND_API_1, RESEND_API_2, RESEND_API_3]
    
    # Фильтруем None значения
    api_keys = [key for key in api_keys if key]
    
    if not api_keys:
        logger.error("No Resend API keys configured. Email cannot be sent.")
        logger.error(f"Attempted to send email to {to_email} with subject: {subject}")
        raise ValueError("No Resend API keys available")
    
    logger.info(f"Attempting to send email via Resend to {to_email} from {from_email}")
    logger.info(f"Available API keys: {len(api_keys)}")
    
    last_error = None
    
    for idx, api_key in enumerate(api_keys, 1):
        try:
            logger.info(f"Trying Resend API key {idx}/{len(api_keys)}")
            
            url = "https://api.resend.com/emails"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "from": from_email,
                "to": [to_email],
                "subject": subject,
                "html": html_body
            }
            
            # Добавляем текстовую версию, если она есть
            if text_body:
                payload["text"] = text_body
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                
                result = response.json()
                logger.info(f"Email successfully sent to {to_email} using API key {idx}")
                logger.info(f"Resend response: {result}")
                return result
                
        except httpx.HTTPStatusError as e:
            error_msg = f"Resend API key {idx} failed: HTTP {e.response.status_code}"
            if e.response.status_code == 401:
                error_msg += " (Unauthorized - invalid API key)"
            elif e.response.status_code == 422:
                error_msg += f" (Validation error: {e.response.text})"
            else:
                error_msg += f" (Response: {e.response.text})"
            
            logger.warning(error_msg)
            last_error = e
            
            # Если это не ошибка авторизации, возможно проблема с данными - не пробуем другие ключи
            if e.response.status_code not in (401, 403):
                logger.error(f"Non-auth error with API key {idx}, stopping retry")
                raise
            
            # Продолжаем с следующим ключом
            continue
            
        except httpx.RequestError as e:
            error_msg = f"Network error with Resend API key {idx}: {e}"
            logger.warning(error_msg)
            last_error = e
            # Продолжаем с следующим ключом при сетевых ошибках
            continue
            
        except Exception as e:
            error_msg = f"Unexpected error with Resend API key {idx}: {e}"
            logger.warning(error_msg)
            last_error = e
            # Продолжаем с следующим ключом
            continue
    
    # Если все ключи не сработали
    logger.error(f"All Resend API keys failed for {to_email}")
    if last_error:
        raise last_error
    else:
        raise Exception("All Resend API keys failed without specific error")

async def send_yandex_metrika_goal(goal_name: str):
    """Отправка цели в Яндекс Метрику через API.
    
    Args:
        goal_name: Название цели (lead_form_sent, go_to_bot, purchase_confirmed)
    """
    if not YANDEX_METRIKA_ID or not METRICS_GOAL:
        logger.warning("Yandex Metrika not configured or METRICS_GOAL not set")
        return
    
    # Отправляем цель только если она соответствует METRICS_GOAL
    if METRICS_GOAL != goal_name:
        logger.info(f"Skipping goal {goal_name}, METRICS_GOAL is set to {METRICS_GOAL}")
        return
    
    try:
        # Яндекс Метрика API для отправки целей
        # Используем метод reachGoal через HTTP API
        url = f"https://mc.yandex.ru/watch/{YANDEX_METRIKA_ID}"
        
        # Для серверной отправки целей используем специальный endpoint
        # Но так как Яндекс Метрика работает через клиентский код,
        # мы можем использовать альтернативный подход - отправку через вебхук
        # или просто логировать событие для последующей обработки
        
        # В данном случае, так как Яндекс Метрика работает через клиентский код,
        # мы можем использовать альтернативный подход - отправку через вебхук
        # или просто логировать событие для последующей обработки
        
        logger.info(f"Yandex Metrika goal '{goal_name}' should be sent (METRICS_GOAL={METRICS_GOAL})")
        
        # Примечание: Яндекс Метрика работает через клиентский код (JavaScript),
        # поэтому для серверной отправки целей нужно использовать специальный API
        # или отправлять событие через вебхук. В данном случае мы логируем событие.
        
    except Exception as e:
        logger.exception(f"Error sending Yandex Metrika goal: {e}")