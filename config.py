import os
from dotenv import load_dotenv

load_dotenv()  # Загружает переменные окружения из файла .env

SERVER_URL = os.getenv("SERVER_URL")
MAHIN_URL = os.getenv("MAHIN_URL")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///bot_database.db")

REFERRAL_AMOUNT = os.getenv("REFERRAL_AMOUNT")

# YooKassa configuration
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")
YOOKASSA_PAYMENTS_URL = os.getenv("YOOKASSA_PAYMENTS_URL")

PORT = os.getenv("PORT", 8000)
BOT_USERNAME = os.getenv("BOT_USERNAME")