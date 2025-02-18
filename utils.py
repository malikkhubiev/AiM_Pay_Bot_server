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
    DISABLE_SECRET_CODE_CHECK
)
from yookassa import Configuration
import logging
from database import (
    get_user
)

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
    if DISABLE_SECRET_CODE_CHECK == "True":
        return True
    else:
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
    return dt.strftime("%d.%m.%Y [%H:%M]")

def format_timedelta(td):
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    time_str = []
    if days > 0:
        time_str.append(f"{days} дней")
    if hours > 0:
        time_str.append(f"{hours} часов")
    if minutes > 0:
        time_str.append(f"{minutes} минут")
    if seconds > 0:
        time_str.append(f"{seconds} секунд")
    
    return ", ".join(time_str)
