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

# Whapi.Cloud API token (supports WHAPI_TOKEN or WHAPI_API_KEY)
WHAPI_TOKEN = os.getenv("WHAPI_TOKEN") or os.getenv("WHAPI_API_KEY")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

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

# Email provider selection: SMTP or RESEND (default to RESEND for render.com)
EMAIL_PROVIDER = os.getenv("EMAIL_PROVIDER", "RESEND").upper()

# Resend (HTTP API) configuration
# Prefer RESEND_API (as requested), fallback to legacy RESEND_API_KEY if present
RESEND_API_KEY = os.getenv("RESEND_API") or os.getenv("RESEND_API_KEY")
RESEND_FROM = os.getenv("RESEND_FROM")