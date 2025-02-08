import os
from dotenv import load_dotenv

load_dotenv()  # Загружает переменные окружения из файла .env

SERVER_URL = os.getenv("SERVER_URL")
MAHIN_URL = os.getenv("MAHIN_URL")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///bot_database.db")

COURSE_AMOUNT = os.getenv("COURSE_AMOUNT")
REFERRAL_AMOUNT = os.getenv("REFERRAL_AMOUNT")
PROMO_NUM_LIMIT = os.getenv("PROMO_NUM_LIMIT")

# YooKassa configuration
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")
YOOKASSA_PAYOUT_KEY = os.getenv("YOOKASSA_PAYOUT_KEY")
YOOKASSA_PAYMENTS_URL = os.getenv("YOOKASSA_PAYMENTS_URL")
YOOKASSA_AGENT_ID = os.getenv("YOOKASSA_AGENT_ID")

PORT = os.getenv("PORT", 8000)
BOT_USERNAME = os.getenv("BOT_USERNAME")

SECRET_KEY = os.getenv("SECRET_KEY")
SECRET_CODE = os.getenv("SECRET_CODE")
DISABLE_SECRET_CODE_CHECK = os.getenv("DISABLE_SECRET_CODE_CHECK")
FILE_ID = os.getenv("FILE_ID")
