import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///bot_database.db")

PORT = os.getenv("PORT", 8000)
BOT_USERNAME = os.getenv("BOT_USERNAME")
SECRET_KEY = os.getenv("SECRET_KEY")
SECRET_CODE = os.getenv("SECRET_CODE")
FILE_ID = os.getenv("FILE_ID")
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")
YOOKASSA_PAYOUT_KEY = os.getenv("YOOKASSA_PAYOUT_KEY")
YOOKASSA_PAYMENTS_URL = os.getenv("YOOKASSA_PAYMENTS_URL")
YOOKASSA_AGENT_ID = os.getenv("YOOKASSA_AGENT_ID")

DEFAULT_SETTINGS = {
    "MY_MONEY": 0,
    # Project configuration
    "SERVER_URL": os.getenv("SERVER_URL"),
    "MAHIN_URL": os.getenv("MAHIN_URL"),
    # Money configuration
    "COURSE_AMOUNT": os.getenv("COURSE_AMOUNT"),
    "COURSE_CEILING": os.getenv("COURSE_CEILING"),
    "REFERRAL_AMOUNT": os.getenv("REFERRAL_AMOUNT"),
    "PROMO_NUM_LIMIT": os.getenv("PROMO_NUM_LIMIT"),
    "CARDS": "cards = ['2200 3005 6476 2126', '2200 7702 9733 5855', '2202 2050 3989 7050']"
}