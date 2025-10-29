import os
import json
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
    "REFERRAL_AMOUNT": os.getenv("REFERRAL_AMOUNT"),
    "PROMO_NUM_LIMIT": os.getenv("PROMO_NUM_LIMIT"),
    "CARDS": json.dumps(['2200 3005 6476 2126', '2200 7702 9733 5855', '2202 2050 3989 7050'])
}

# SMTP/Email configuration for sending demo links
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)
SMTP_TIMEOUT = int(os.getenv("SMTP_TIMEOUT", "15"))

# Email provider selection: SMTP (default) or RESEND
EMAIL_PROVIDER = os.getenv("EMAIL_PROVIDER", "SMTP").upper()

# Resend (HTTP API) configuration
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
RESEND_FROM = os.getenv("RESEND_FROM", FROM_EMAIL)