import uuid
from sqlalchemy import insert, create_engine, func, and_, Column, Integer, String, ForeignKey, DateTime, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone, timedelta
import databases
from sqlalchemy import select
from typing import Optional
import logging
from config import (DEFAULT_SETTINGS)

# Создание базы данных и её подключение
DATABASE_URL = "sqlite+aiosqlite:///bot_database.db"
database = databases.Database(DATABASE_URL)

Base = declarative_base()

class Setting(Base):
    __tablename__ = 'settings'

    key = Column(String, primary_key=True)
    value = Column(String, nullable=False)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
 
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=True)
    telegram_id = Column(String, unique=True, nullable=True)
    fio = Column(String(255), unique=False, nullable=True)
    date_of_trial_ends = Column(DateTime, nullable=True)
    date_of_certificate = Column(DateTime, nullable=True)
    unique_str = Column(String, unique=True, nullable=False)
    passed_exam = Column(Boolean, default=False)
    paid = Column(Boolean, default=False)
    balance = Column(Integer, default=0)
    card_synonym = Column(String, unique=True, nullable=True)
    referral_rank = Column(String)

    invite_link = Column(String, nullable=True)

    source = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    pay_email = Column(String, nullable=True)  # Новое поле для email оплаты

    payments = relationship("Payment", back_populates="user")
    payouts = relationship("Payout", back_populates="user", foreign_keys="[Payout.telegram_id]")

class Payment(Base):
    __tablename__ = 'payments'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'), nullable=False)  # Ссылка на пользователя
    transaction_id = Column(String, default=None)  # Идентификатор транзакции
    idempotence_key = Column(String, nullable=False, unique=True)
    amount = Column(Integer, default=0)
    status = Column(String, nullable=False) # (success|pending)
    created_at = Column(DateTime, nullable=False, server_default=func.now())  # Дата создания

    user = relationship("User", back_populates="payments")  # Связь с пользователем

class Referral(Base):
    __tablename__ = 'referrals'

    id = Column(Integer, primary_key=True, index=True)
    referrer_id = Column(String, ForeignKey('users.telegram_id'))  # Кто пригласил
    referred_id = Column(String, ForeignKey('users.telegram_id'), unique=True)  # Кто был приглашён
    status = Column(String, nullable=False) # (success|registered|pending)
    created_at = Column(DateTime, nullable=False, server_default=func.now())

    referrer = relationship("User", foreign_keys=[referrer_id])  # Связь с пригласившим
    referred_user = relationship("User", foreign_keys=[referred_id])  # Связь с приглашённым
    payout = relationship("Payout", back_populates="referral")  # Связь с выплатами

class Payout(Base):
    __tablename__ = 'payouts'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'))  # Ссылка на пользователя
    card_synonym = Column(String, nullable=False)
    idempotence_key = Column(String, nullable=False, unique=True)
    amount = Column(Float)
    status = Column(String, nullable=False) # (pending|success)
    notified = Column(Boolean, default=False)
    transaction_id = Column(String, nullable=True)  # Идентификатор транзакции
    created_at = Column(DateTime, nullable=False, server_default=func.now())

    referral_id = Column(Integer, ForeignKey('referrals.id'))  # Не знаю нафига, но без этого не работает

    user = relationship("User", back_populates="payouts", foreign_keys=[telegram_id])
    referral = relationship("Referral", back_populates="payout") # Не знаю нафига

class Binding(Base):
    __tablename__ = 'bindings'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'))  
    unique_str = Column(String, unique=True, nullable=False)

class Lead(Base):
    __tablename__ = 'leads'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=True)
    email = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    telegram_id = Column(String, nullable=True)  # Telegram ID пользователя
    username = Column(String, nullable=True)  # Telegram username
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    notified = Column(Boolean, default=False)  # Новый флаг рассылки
    source_id = Column(Integer, ForeignKey('sources.id'), nullable=True)  # Связь с источником трафика

class LeadProgress(Base):
    __tablename__ = 'lead_progress'

    id = Column(Integer, primary_key=True)
    lead_id = Column(Integer, ForeignKey('leads.id'), nullable=False)
    step = Column(String, nullable=False)  # logical step/question id or title
    answer = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())

class ChatMessage(Base):
    __tablename__ = 'chat_messages'

    id = Column(Integer, primary_key=True)
    session_id = Column(String, nullable=False, index=True)  # Уникальный ID сессии пользователя
    message = Column(String, nullable=False)  # Сообщение пользователя или ответ AI
    is_from_user = Column(Boolean, default=True)  # True - от пользователя, False - от AI
    created_at = Column(DateTime, nullable=False, server_default=func.now())

class Source(Base):
    __tablename__ = 'sources'

    id = Column(Integer, primary_key=True)
    utm_source = Column(String, nullable=True)  # UTM source (может быть пустым для прямых заходов)
    utm_medium = Column(String, nullable=True)  # UTM medium
    utm_campaign = Column(String, nullable=True)  # UTM campaign
    utm_term = Column(String, nullable=True)  # UTM term
    utm_content = Column(String, nullable=True)  # UTM content
    session_id = Column(String, nullable=True)  # ID сессии посетителя
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    lead_id = Column(Integer, ForeignKey('leads.id'), nullable=True)  # Связь с лидом, если посетитель оставил данные

engine = create_engine(DATABASE_URL.replace("sqlite+aiosqlite", "sqlite"))

async def initialize_settings_once():
    existing_keys_query = select(Setting.key)
    existing_keys_result = await database.fetch_all(existing_keys_query)
    existing_keys = {row["key"] for row in existing_keys_result}

    # Insert only non-None values and cast to string to satisfy NOT NULL String column
    insert_values = []
    for key, value in DEFAULT_SETTINGS.items():
        if key in existing_keys:
            continue
        if value is None:
            # skip undefined env-backed settings to avoid NOT NULL violation
            continue
        insert_values.append({"key": key, "value": str(value)})

    if insert_values:
        query = Setting.__table__.insert().values(insert_values)
        await database.execute(query)

# Функция для получения значения настройки по ключу
async def get_setting(key: str):
    query = select(Setting).filter_by(key=key)
    async with database.transaction():
        row = await database.fetch_one(query)
    return row["value"] if row else None

# Функция для получения всех настроек
async def get_all_settings():
    query = select(Setting)
    async with database.transaction():
        rows = await database.fetch_all(query)
    return {row["key"]: row["value"] for row in rows}

# Асинхронная функция для установки значения настройки
async def set_setting(key: str, value: str):
    # Данные для обновления
    update_data = {'value': value}
    
    # Запрос на обновление, если запись с таким ключом существует
    update_query = Setting.__table__.update().where(Setting.key == key).values(update_data)
    
    # Запрос на вставку новой записи, если её нет
    insert_query = Setting.__table__.insert().values(key=key, value=value)
    
    # Работаем в контексте транзакции
    async with database.transaction():
        # Пробуем выполнить обновление
        result = await database.execute(update_query)
        
        if result == 0:  # Если строк не обновлено (значит, записи не было), выполняем вставку
            await database.execute(insert_query)
            
def initialize_database():
    """Создает базу данных, если она еще не создана."""
    Base.metadata.create_all(bind=engine)

# async def create_pending_payout(
#         telegram_id: str,
#         card_synonym: str,
#         idempotence_key: str,
#         payout_amount: str
#     ):
#     query = """ 
#         INSERT INTO payouts (telegram_id, card_synonym, idempotence_key, amount, status) 
#         VALUES (:telegram_id, :card_synonym, :idempotence_key, :amount, :status) 
#     """
#     values = { 
#         "telegram_id": telegram_id, 
#         "card_synonym": card_synonym,
#         "idempotence_key": idempotence_key,
#         "amount": payout_amount, 
#         "status": "pending"
#     } 
#     async with database.transaction():  # Используем async with для выполнения транзакции
#         await database.execute(query, values)

async def get_user(telegram_id: str):
    query = select(User).filter_by(telegram_id=telegram_id)
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_one(query)

async def get_user_by_unique_str(unique_str: str):
    query = select(User).filter_by(unique_str=unique_str)
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_one(query)

async def get_registered_user(telegram_id: str):
    query = select(User).filter_by(
        telegram_id=telegram_id
    )
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_one(query)

async def create_user(telegram_id: str, username: str = None):
    """Создает нового пользователя в базе данных"""
    unique_str = str(uuid.uuid4())
    query = User.__table__.insert().values(
        telegram_id=telegram_id,
        username=username,
        unique_str=unique_str,
        paid=False,
        balance=0,
        passed_exam=False
    )
    async with database.transaction():
        await database.execute(query)
    # Получаем созданного пользователя
    return await get_registered_user(telegram_id)

async def get_users_with_positive_balance():
    query = "SELECT * FROM users WHERE balance > 0 ORDER BY balance DESC"  # Добавляем сортировку
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_all(query)

async def get_all_referred(telegram_id: str):
    query = select(Referral).filter_by(referrer_id=telegram_id, status="success")
    async with database.transaction():  # Используем async with для транзакции
        return await database.fetch_all(query)

async def get_referrer(telegram_id: str):
    query = select(Referral).filter_by(referred_id=telegram_id)
    async with database.transaction():  # Используем async with
        return await database.fetch_one(query)

async def get_pending_referrer(telegram_id: str):
    query = select(Referral).filter_by(referred_id=telegram_id, status="pending")
    async with database.transaction():  # Используем async with
        return await database.fetch_one(query)

async def get_referred(telegram_id: str):
    query = select(Referral).filter_by(referrer_id=telegram_id)
    async with database.transaction():  # Используем async with
        return await database.fetch_one(query)
    
async def get_referred_user(referred_id: int):
    query = select(User).filter_by(telegram_id=referred_id)
    async with database.transaction():  # Используем async with
        return await database.fetch_one(query)
    
async def get_payment_date(referred_id: int):
    query = select(Payment.created_at).filter_by(telegram_id=referred_id)
    async with database.transaction():
        result = await database.fetch_one(query)
        return result['created_at'] if result else None

async def get_start_working_date(referred_id: int):
    query = select(User.created_at).filter_by(telegram_id=referred_id)
    async with database.transaction():
        result = await database.fetch_one(query)
        return result['created_at'] if result else None

async def get_payments_frequency_db():
    """ Получает количество оплат, сгруппированных по датам, где статус 'success'. """
    query = (
        select(
            func.date(Payment.created_at).label("date"),
            func.count().label("payments_count")
        )
        .filter(Payment.status == "success")  # Фильтрация по статусу
        .group_by(func.date(Payment.created_at))
        .order_by(func.date(Payment.created_at))
    )
    async with database.transaction():
        return await database.fetch_all(query)

async def get_paid_referrals_by_user(telegram_id: str):
    """ Получает количество оплаченных рефералов по дням для пользователя """
    query = (
        select(Payment.created_at)
        .join(User, Payment.telegram_id == User.telegram_id)
        .join(Referral, Referral.referred_id == User.telegram_id)
        .where(Referral.referrer_id == telegram_id, Payment.status == "success")
    )
    payments = await database.fetch_all(query)
    
    referral_data = {}
    for row in payments:
        date_str = row["created_at"].strftime("%Y-%m-%d")
        referral_data[date_str] = referral_data.get(date_str, 0) + 1
    
    # Сортируем даты
    sorted_referral_data = dict(sorted(referral_data.items()))

    return sorted_referral_data

async def get_user_by_cert_id(cert_id: str):
    short_id = cert_id.replace("CERT-", "")
    query = select(User).where(User.telegram_id.like(f"{short_id}%")).limit(1)
    return await database.fetch_one(query)

async def get_conversion_stats_by_source():
    query = """ 
        SELECT 
            source,
            COUNT(*) AS total_users,
            COUNT(*) AS registered_users,
            COUNT(*) FILTER (WHERE paid = true) AS paid_users
        FROM users
        WHERE source IS NOT NULL
        GROUP BY source
        ORDER BY total_users DESC
    """
    async with database.transaction():
        rows = await database.fetch_all(query)

    results = []
    for row in rows:
        source = row["source"] or "unknown"
        total = row["total_users"]
        registered = row["registered_users"]
        paid = row["paid_users"]

        registered_pct = round((registered / total) * 100, 2) if total else 0
        paid_pct_total = round((paid / total) * 100, 2) if total else 0
        paid_pct_registered = round((paid / registered) * 100, 2) if registered else 0

        results.append({
            "Источник": source,
            "Всего": total,
            "Зарегистрировались": registered,
            "% Регистраций": f"{registered_pct}%",
            "Оплатили": paid,
            "% Оплат от всех": f"{paid_pct_total}%",
            "% Оплат от зарегистрированных": f"{paid_pct_registered}%"
        })

    return results

async def get_referral_conversion_stats():
    query = """ 
        SELECT 
            referrer_id,
            COUNT(*) AS total_referred,
            COUNT(*) AS registered_users,
            COUNT(*) FILTER (WHERE paid = true) AS paid_users
        FROM referrals
        JOIN users ON users.telegram_id = referrals.referred_id
        WHERE referrer_id IS NOT NULL
        GROUP BY referrer_id
        ORDER BY total_referred DESC
    """
    async with database.transaction():
        rows = await database.fetch_all(query)

    results = []
    for row in rows:
        referrer_id = row["referrer_id"]
        total = row["total_referred"]
        registered = row["registered_users"]
        paid = row["paid_users"]

        registered_pct = round((registered / total) * 100, 2) if total else 0
        paid_pct_total = round((paid / total) * 100, 2) if total else 0
        paid_pct_registered = round((paid / registered) * 100, 2) if registered else 0

        results.append({
            "Реферер ID": referrer_id,
            "Пришло по рефералке": total,
            "Зарегистрировались": registered,
            "% Регистраций": f"{registered_pct}%",
            "Оплатили": paid,
            "% Оплат от всех": f"{paid_pct_total}%",
            "% Оплат от зарегистрированных": f"{paid_pct_registered}%"
        })

    return results

async def get_top_referrers_from_db():
    query = """
        SELECT 
            users.telegram_id,
            users.username,
            COUNT(referrals.id) AS total_referred
        FROM referrals
        JOIN users ON users.telegram_id = referrals.referrer_id
        WHERE referrals.status = 'success'
        GROUP BY users.telegram_id, users.username
        HAVING COUNT(referrals.id) >= 1
        ORDER BY total_referred DESC
    """

    async with database.transaction():
        rows = await database.fetch_all(query)

    def resolve_rank(ref_count: int) -> str:
        if ref_count >= 60:
            return "🧠 Архитектор мышления"
        elif ref_count >= 50:
            return "🌌 Духовный вдохновитель"
        elif ref_count >= 40:
            return "💎 Наставник Инноваций"
        elif ref_count >= 30:
            return "🚀 Вестник Эволюции"
        elif ref_count >= 20:
            return "🌎 Мастер экспансии"
        elif ref_count >= 10:
            return "🌱 Амбассадор развития"
        elif ref_count >= 1:
            return "🔥 Лидер роста"
        return "—"

    if not rows:
        return "Реферальная программа не приведена в исполнение"

    result = ["*🤴 ТОП ПРИГЛАСИВШИХ:*"]
    for index, row in enumerate(rows, start=1):
        telegram_id = row["telegram_id"]
        username = row["username"] or "—"
        total_referred = row["total_referred"]
        rank = resolve_rank(total_referred)

        result.append(f"{index}. `{telegram_id}` | @{username} — {total_referred} рефералов\n🎖 {rank}")

    return "\n\n".join(result)

async def get_successful_referral_count(telegram_id: str) -> int:
    query = """
        SELECT COUNT(*) AS count
        FROM referrals
        WHERE referrer_id = :referrer_id AND status = 'success'
    """
    values = {"referrer_id": telegram_id}

    async with database.transaction():
        result = await database.fetch_one(query, values)

    return result["count"] if result else 0

async def get_all_referrers_for_crm():
    """Получает всех рефереров с их статистикой для CRM"""
    # Получаем REFERRAL_AMOUNT из настроек
    referral_amount_str = await get_setting("REFERRAL_AMOUNT")
    referral_amount = float(referral_amount_str) if referral_amount_str else 0.0
    
    # Показываем всех кто привязал карту (card_synonym не пустой), даже если никого не привёл
    # Payout функционал закомментирован, используем ручные выплаты через mark_paid
    query = """
        SELECT 
            u.telegram_id,
            u.username,
            u.balance,
            u.referral_rank,
            u.card_synonym,
            COUNT(DISTINCT r.id) AS total_referred,
            COUNT(DISTINCT CASE WHEN r.status = 'success' THEN r.id END) AS paid_referrals,
            COUNT(DISTINCT CASE WHEN r.status = 'pending' THEN r.id END) AS pending_referrals,
            COUNT(DISTINCT CASE WHEN r.status = 'registered' THEN r.id END) AS registered_referrals,
            COALESCE((SELECT SUM(amount) FROM payouts WHERE telegram_id = u.telegram_id AND status = 'success'), 0) AS total_paid_out
        FROM users u
        LEFT JOIN referrals r ON r.referrer_id = u.telegram_id
        WHERE u.card_synonym IS NOT NULL AND u.card_synonym != ''
        GROUP BY u.telegram_id, u.username, u.balance, u.referral_rank, u.card_synonym
        ORDER BY paid_referrals DESC, total_referred DESC
    """
    async with database.transaction():
        rows = await database.fetch_all(query)
    
    results = []
    for row in rows:
        paid_referrals = row["paid_referrals"] or 0
        earned_amount = paid_referrals * referral_amount  # REFERRAL_AMOUNT * количество оплативших клиентов
        total_paid_out = row["total_paid_out"] or 0
        unpaid_amount = earned_amount - total_paid_out  # Сколько осталось выплатить
        
        results.append({
            "telegram_id": row["telegram_id"],
            "username": row["username"] or "—",
            "balance": row["balance"] or 0,
            "referral_rank": row["referral_rank"] or "—",
            "card_synonym": row["card_synonym"] or "—",
            "total_referred": row["total_referred"] or 0,
            "paid_referrals": paid_referrals,
            "pending_referrals": row["pending_referrals"] or 0,
            "registered_referrals": row["registered_referrals"] or 0,
            "earned_amount": earned_amount,  # Сколько заработали (REFERRAL_AMOUNT * paid_referrals)
            "total_paid_out": total_paid_out,  # Сколько уже выплатили
            "unpaid_amount": unpaid_amount  # Сколько осталось выплатить
        })
    
    return results


async def get_all_paid_money(telegram_id: str):
    query = select(func.sum(Payout.amount)).filter(Payout.telegram_id == telegram_id)
    async with database.transaction():
        result = await database.fetch_one(query)
        return result[0] if result[0] is not None else 0.0

async def get_paid_count(telegram_id: str):
    query = select(func.count(Referral.id)).join(User, Referral.referred_id == User.telegram_id)\
        .filter(Referral.referrer_id == telegram_id, Referral.status=="success", User.paid == True)
    async with database.transaction():  # Используем async with для транзакции
        result = await database.fetch_one(query)
        return result[0] or 0

async def get_pending_payment(telegram_id: str):
    query = select(Payment).filter_by(telegram_id=telegram_id, status="pending")
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_one(query)

async def get_payout(transaction_id: str):
    query = "SELECT * FROM payouts WHERE transaction_id = :transaction_id"
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_one(query, {"transaction_id": transaction_id}) 

async def get_pending_payout(telegram_id: str):
    query = "SELECT * FROM payouts WHERE telegram_id = :telegram_id AND status = 'pending'"
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_one(query, {"telegram_id": telegram_id})
    
async def create_referral(telegram_id: str, referrer_id: int):
    query = Referral.__table__.insert().values(referrer_id=referrer_id, referred_id=telegram_id, status="pending")
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(query)

async def mark_payout_as_notified(payout_id: int):
    query = select(Payout).filter_by(id=payout_id)
    async with database.transaction():  # Используем async with
        payout = await database.fetch_one(query)
        if payout:
            update_query = Payout.__table__.update().where(Payout.id == payout_id).values(notified=True)
            await database.execute(update_query)

async def update_referrer(telegram_id: str, referrer_id: str):
    update_data = {'referrer_id': referrer_id}
    update_query = Referral.__table__.update().where(Referral.referred_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_pending_referral(telegram_id: str):
    update_data = {"status": "registered"}
    update_query = Referral.__table__.update().where(
        Referral.referred_id == telegram_id,
        Referral.status == "pending"
    ).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_referral_rank(telegram_id: str, rank: str):
    update_data = {"referral_rank": rank}
    update_query = User.__table__.update().where(
        User.telegram_id == telegram_id
    ).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_passed_exam_in_db(telegram_id: str):
    update_data = {'passed_exam': True}
    update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_payout_transaction(telegram_id: str, transaction_id: str):
    query = """ 
        UPDATE payouts 
        SET transaction_id = :transaction_id 
        WHERE telegram_id = :telegram_id AND status = 'pending' 
    """ 
    update_data = {
        "transaction_id": transaction_id,
        "telegram_id": telegram_id
    }
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(query, update_data) 

async def update_payout_status(transaction_id: str, status: str):
    query = """ 
        UPDATE payouts 
        SET status = :status 
        WHERE transaction_id = :transaction_id 
    """ 
    update_data = {
        "transaction_id": transaction_id,
        "status": status,
    }
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(query, update_data) 

async def update_referral_success(telegram_id: str, referrer_id: str):
    update_data = {'status': "success"}
    update_query = Referral.__table__.update().where(
        Referral.referred_id == telegram_id, Referral.referrer_id == referrer_id
    ).values(update_data)
    
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_user_balance(telegram_id: str, balance: int):
    update_data = {'balance': balance}
    update_query = User.__table__.update().where(
        User.telegram_id == telegram_id
    ).values(update_data)
    
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_payment_done(telegram_id: str, transaction_id: str, income_amount: float):
    user_update_data = {'paid': True}
    user_update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(user_update_data)
    payment_update_data = {"status": "success", "transaction_id": transaction_id, "amount": income_amount}
    payment_update_query = Payment.__table__.update().where(Payment.telegram_id == telegram_id).values(payment_update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(user_update_query)
        await database.execute(payment_update_query)

async def update_payment_idempotence_key(telegram_id: str, idempotence_key: str):
    update_data = {'idempotence_key': idempotence_key}
    update_query = Payment.__table__.update().where(Payment.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_user_card_synonym(telegram_id: str, card_synonym: str):
    update_data = {'card_synonym': card_synonym}
    update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_fio_and_date_of_cert(telegram_id: str, fio: str):
    update_data = {
        'fio': fio,
        'date_of_certificate': datetime.now(timezone.utc)
    }
    update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)
 
async def create_payment_db(
        telegram_id: str,
        payment_id: str,
        idempotence_key: str,
        status: str
    ):
    query = Payment.__table__.insert().values(
        transaction_id=payment_id,
        telegram_id=telegram_id,
        idempotence_key=idempotence_key,
        status=status
    )
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(query)

async def create_payout(telegram_id: str, card_synonym: str, amount: int, transaction_id: str):
    query = Payout.__table__.insert().values(card_synonym=card_synonym, telegram_id=telegram_id, amount=amount, transaction_id=transaction_id)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(query)

async def create_binding_and_delete_if_exists(telegram_id: str, unique_str: str):
    query = select(Binding).filter_by(telegram_id=telegram_id)
    async with database.transaction():  # Используем async with для транзакции
        existing_binding = await database.fetch_one(query)
        if existing_binding:
            delete_query = Binding.__table__.delete().where(Binding.id == existing_binding['id'])
            await database.execute(delete_query)
        insert_query = Binding.__table__.insert().values(telegram_id=telegram_id, unique_str=unique_str)
        await database.execute(insert_query)

async def get_binding_by_unique_str(unique_str: str):
    query = select(Binding).filter(Binding.unique_str == unique_str)
    async with database.transaction():  # Используем async with для транзакции
        return await database.fetch_one(query)

async def ultra_excute(query: str):
    # Разбиваем скрипт на отдельные выражения
    statements = [stmt.strip() for stmt in query.strip().split(';') if stmt.strip()]
    
    async with database.transaction():
        for stmt in statements:
            await database.execute(stmt)
    
    return {"status": "success", "result": f"Executed {len(statements)} statements"}

async def create_lead(name: str, email: str, phone: str, source_id: int = None) -> int:
    # Проверка на дубликаты по email и phone
    # Нормализуем email (приводим к нижнему регистру)
    normalized_email = email.lower().strip() if email else None
    
    async with database.transaction():
        # Проверяем существующий лид с таким же email или phone (email без учета регистра)
        if normalized_email:
            email_query = select(Lead).where(func.lower(Lead.email) == normalized_email)
            existing_email = await database.fetch_one(email_query)
            if existing_email:
                raise ValueError(f"Лид с email {normalized_email} уже существует (ID: {existing_email['id']})")
        
        if phone:
            phone_query = select(Lead).where(Lead.phone == phone)
            existing_phone = await database.fetch_one(phone_query)
            if existing_phone:
                raise ValueError(f"Лид с телефоном {phone} уже существует (ID: {existing_phone['id']})")
        
        # Если дубликатов нет, создаем новый лид (используем нормализованный email)
        query = Lead.__table__.insert().values(
            name=name,
            email=normalized_email,
            phone=phone,
            source_id=source_id
        )
        inserted_id = await database.execute(query)
        return int(inserted_id)

async def get_lead_by_id(lead_id: int):
    query = select(Lead).filter_by(id=lead_id)
    async with database.transaction():
        return await database.fetch_one(query)

async def get_lead_progress(lead_id: int):
    query = select(LeadProgress).filter_by(lead_id=lead_id)
    async with database.transaction():
        rows = await database.fetch_all(query)
        return rows

async def record_lead_answer(lead_id: int, step: str, answer: str):
    # prevent duplicate per step
    check_query = select(LeadProgress).filter_by(lead_id=lead_id, step=step)
    async with database.transaction():
        existing = await database.fetch_one(check_query)
        if existing:
            return False
        ins = LeadProgress.__table__.insert().values(lead_id=lead_id, step=step, answer=answer)
        await database.execute(ins)
        return True

async def update_lead_answer(lead_id: int, step: str, answer: str):
    # update existing answer for a step
    check_query = select(LeadProgress).filter_by(lead_id=lead_id, step=step)
    async with database.transaction():
        existing = await database.fetch_one(check_query)
        if not existing:
            return False
        upd = LeadProgress.__table__.update().where(
            (LeadProgress.lead_id == lead_id) & (LeadProgress.step == step)
        ).values(answer=answer)
        await database.execute(upd)
        return True

async def get_leads(
    *,
    offset: int = 0,
    limit: int = 100,
    name: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    q: Optional[str] = None,
    notified: Optional[bool] = None,
    created_from: Optional[datetime] = None,
    created_to: Optional[datetime] = None,
    sort_by: str = 'created_at',
    sort_dir: str = 'desc'
):
    qry = select(Lead)
    # filters
    if name:
        qry = qry.where(Lead.name.ilike(f"%{name}%"))
    if email:
        qry = qry.where(Lead.email.ilike(f"%{email}%"))
    if phone:
        qry = qry.where(Lead.phone.ilike(f"%{phone}%"))
    if q:
        pattern = f"%{q}%"
        qry = qry.where(
            (Lead.name.ilike(pattern)) | (Lead.email.ilike(pattern)) | (Lead.phone.ilike(pattern))
        )
    if notified is not None:
        qry = qry.where(Lead.notified == notified)
    if created_from:
        qry = qry.where(Lead.created_at >= created_from)
    if created_to:
        qry = qry.where(Lead.created_at <= created_to)

    # sorting
    sort_map = {
        'id': Lead.id,
        'name': Lead.name,
        'email': Lead.email,
        'phone': Lead.phone,
        'created_at': Lead.created_at,
        'notified': Lead.notified
    }
    col = sort_map.get(sort_by, Lead.created_at)
    if sort_dir.lower() == 'asc':
        qry = qry.order_by(col.asc())
    else:
        qry = qry.order_by(col.desc())

    qry = qry.offset(offset).limit(limit)
    async with database.transaction():
        return await database.fetch_all(qry)

async def get_leads_total_count(
    *,
    name: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    q: Optional[str] = None,
    notified: Optional[bool] = None,
    created_from: Optional[datetime] = None,
    created_to: Optional[datetime] = None,
):
    qry = select(func.count(Lead.id))
    if name:
        qry = qry.where(Lead.name.ilike(f"%{name}%"))
    if email:
        qry = qry.where(Lead.email.ilike(f"%{email}%"))
    if phone:
        qry = qry.where(Lead.phone.ilike(f"%{phone}%"))
    if q:
        pattern = f"%{q}%"
        qry = qry.where((Lead.name.ilike(pattern)) | (Lead.email.ilike(pattern)) | (Lead.phone.ilike(pattern)))
    if notified is not None:
        qry = qry.where(Lead.notified == notified)
    if created_from:
        qry = qry.where(Lead.created_at >= created_from)
    if created_to:
        qry = qry.where(Lead.created_at <= created_to)
    async with database.transaction():
        row = await database.fetch_one(qry)
        return int(row[0]) if row is not None else 0

async def set_lead_notified(email: str):
    # Отметить лид как notified (email нормализуется для поиска без учета регистра)
    normalized_email = email.lower().strip() if email else None
    if normalized_email:
        update_query = Lead.__table__.update().where(func.lower(Lead.email) == normalized_email).values(notified=True)
        async with database.transaction():
            await database.execute(update_query)

async def get_unnotified_abandoned_leads():
    # Лиды >1 суток, не notified, и email не встречается у User.pay_email
    yesterday = datetime.utcnow() - timedelta(days=1)
    # Подзапрос на все pay_email
    from sqlalchemy import select as sql_select
    user_email_subquery = sql_select(User.pay_email).subquery()
    query = sql_select(Lead).where(
        Lead.notified == False,
        Lead.created_at < yesterday,
        ~Lead.email.in_(user_email_subquery)
    )
    async with database.transaction():
        return await database.fetch_all(query)

async def merge_lead_progress(from_lead_id: int, to_lead_id: int):
    """Переносит прогресс из одного лида в другой, избегая дубликатов"""
    # Получаем весь прогресс из старого лида (без транзакции, т.к. вызывается внутри транзакции)
    query = select(LeadProgress).filter_by(lead_id=from_lead_id)
    from_progress = await database.fetch_all(query)
    
    # Переносим каждый шаг прогресса
    for progress_item in from_progress:
        step = progress_item['step']
        answer = progress_item['answer']
        # Проверяем, нет ли уже такого шага в целевом лиде
        check_query = select(LeadProgress).filter_by(lead_id=to_lead_id, step=step)
        existing = await database.fetch_one(check_query)
        if not existing:
            # Переносим только если такого шага еще нет
            ins = LeadProgress.__table__.insert().values(lead_id=to_lead_id, step=step, answer=answer)
            await database.execute(ins)

async def get_or_create_lead_by_email(email: str = None, telegram_id: str = None, username: str = None, name: str = None, phone: str = None, source_id: int = None):
    """
    Получить или создать лид по email или telegram_id. Если лид существует, обновить его данными.
    Если существует лид с telegram_id и лид с email (разные лиды), объединить их.
    Email нормализуется (приводится к нижнему регистру) для избежания дубликатов.
    """
    # Нормализуем email (приводим к нижнему регистру)
    normalized_email = email.lower().strip() if email else None
    
    async with database.transaction():
        # Ищем лид по email (если email указан) - без учета регистра
        existing_by_email = None
        if normalized_email:
            # Используем func.lower для сравнения без учета регистра
            email_query = select(Lead).where(func.lower(Lead.email) == normalized_email)
            existing_by_email = await database.fetch_one(email_query)
        
        # Ищем лид по telegram_id (если telegram_id указан)
        existing_by_tg = None
        if telegram_id:
            tg_query = select(Lead).where(Lead.telegram_id == telegram_id)
            existing_by_tg = await database.fetch_one(tg_query)
        
        # Если найдены оба лида и это разные лиды - объединяем их
        if existing_by_email and existing_by_tg and existing_by_email['id'] != existing_by_tg['id']:
            # Выбираем основной лид (приоритет лиду с email, так как он более полный)
            main_lead = existing_by_email
            secondary_lead = existing_by_tg
            
            # Переносим прогресс из вторичного лида в основной
            await merge_lead_progress(secondary_lead['id'], main_lead['id'])
            
            # Обновляем основной лид всеми доступными данными
            update_values = {}
            if telegram_id and not main_lead['telegram_id']:
                update_values['telegram_id'] = telegram_id
            elif telegram_id and main_lead['telegram_id'] != telegram_id:
                # Если telegram_id отличается, используем переданный
                update_values['telegram_id'] = telegram_id
            if username and not main_lead['username']:
                update_values['username'] = username
            if name and not main_lead['name']:
                update_values['name'] = name
            if phone and not main_lead['phone']:
                update_values['phone'] = phone
            # Нормализуем email в основном лиде, если он отличается регистром
            if normalized_email and main_lead['email']:
                existing_email_lower = main_lead['email'].lower() if main_lead['email'] else None
                if existing_email_lower != normalized_email:
                    update_values['email'] = normalized_email
            # Переносим source_id если есть во вторичном лиде, но нет в основном
            if secondary_lead['source_id'] and not main_lead['source_id']:
                update_values['source_id'] = secondary_lead['source_id']
            # Если source_id передан, но не установлен в основном лиде, устанавливаем
            if source_id and not main_lead['source_id']:
                update_values['source_id'] = source_id
            
            if update_values:
                upd_query = Lead.__table__.update().where(Lead.id == main_lead['id']).values(**update_values)
                await database.execute(upd_query)
            
            # Удаляем вторичный лид
            delete_query = Lead.__table__.delete().where(Lead.id == secondary_lead['id'])
            await database.execute(delete_query)
            
            return int(main_lead['id'])
        
        # Если найден только лид по email
        if existing_by_email:
            # Обновляем существующий лид
            update_values = {}
            if telegram_id and not existing_by_email['telegram_id']:
                update_values['telegram_id'] = telegram_id
            if username and not existing_by_email['username']:
                update_values['username'] = username
            if name and not existing_by_email['name']:
                update_values['name'] = name
            if phone and not existing_by_email['phone']:
                update_values['phone'] = phone
            if source_id and not existing_by_email['source_id']:
                update_values['source_id'] = source_id
            # Нормализуем email, если он отличается регистром
            if normalized_email and existing_by_email['email']:
                existing_email_lower = existing_by_email['email'].lower() if existing_by_email['email'] else None
                if existing_email_lower != normalized_email:
                    update_values['email'] = normalized_email
            
            if update_values:
                upd_query = Lead.__table__.update().where(Lead.id == existing_by_email['id']).values(**update_values)
                await database.execute(upd_query)
            
            return int(existing_by_email['id'])
        
        # Если найден только лид по telegram_id
        if existing_by_tg:
            # Объединяем: обновляем email существующего лида (если email указан, используем нормализованный)
            update_values = {}
            if normalized_email and not existing_by_tg['email']:
                update_values['email'] = normalized_email
            elif normalized_email and existing_by_tg['email']:
                # Если email уже есть, но отличается регистром - нормализуем его
                existing_email_lower = existing_by_tg['email'].lower() if existing_by_tg['email'] else None
                if existing_email_lower != normalized_email:
                    update_values['email'] = normalized_email
            if username and not existing_by_tg['username']:
                update_values['username'] = username
            if name and not existing_by_tg['name']:
                update_values['name'] = name
            if phone and not existing_by_tg['phone']:
                update_values['phone'] = phone
            if source_id and not existing_by_tg['source_id']:
                update_values['source_id'] = source_id
            
            if update_values:
                upd_query = Lead.__table__.update().where(Lead.id == existing_by_tg['id']).values(**update_values)
                await database.execute(upd_query)
            return int(existing_by_tg['id'])
        
        # Создаем новый лид (используем нормализованный email)
        query = Lead.__table__.insert().values(
            name=name,
            email=normalized_email,
            phone=phone,
            telegram_id=telegram_id,
            username=username,
            source_id=source_id
        )
        inserted_id = await database.execute(query)
        return int(inserted_id)

async def set_user_pay_email(telegram_id: str, username:str, email: str, action_type: str = 'entered'):
    """Устанавливает pay_email для пользователя и схлопывает лиды по email"""
    async with database.transaction():
        # Обновляем pay_email у пользователя
        logging.info("telegram_id=", telegram_id,"username=", username,"email=", email,"action_type=", action_type)
        update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(username=username, pay_email=email)
        await database.execute(update_query)
        
        # Получаем данные пользователя
        user_query = select(User).where(User.telegram_id == telegram_id)
        user = await database.fetch_one(user_query)
        logging.info("user", user)

        if user:
            logging.info("user exists, updating lead")
            # Создаем или обновляем лид с объединением по email
            lead_id = await get_or_create_lead_by_email(
                email=email,
                telegram_id=telegram_id,
                username=username
            )
            # Записываем действие
            if lead_id:
                logging.info("there is lead, making bot_action_pay_email_entered")
                action_step = 'bot_action_pay_email_entered' if action_type == 'entered' else 'bot_action_pay_email_confirmed'
                await record_lead_answer(lead_id, action_step, email)
            return lead_id
        return None

async def get_user_pay_email(telegram_id: str):
    query = select(User.pay_email).filter_by(telegram_id=telegram_id)
    async with database.transaction():
        result = await database.fetch_one(query)
        return result[0] if result else None

async def merge_duplicate_leads_by_email():
    """
    Объединяет существующие дубликаты лидов с одинаковым email (разный регистр).
    Эта функция должна быть вызвана один раз для очистки существующих дубликатов.
    """
    # Получаем все лиды с email (без транзакции, т.к. будем работать внутри)
    all_leads_query = select(Lead).where(Lead.email.isnot(None))
    all_leads = await database.fetch_all(all_leads_query)
    
    # Группируем лиды по нормализованному email
    email_groups = {}
    for lead in all_leads:
        if lead['email']:
            normalized = lead['email'].lower().strip()
            if normalized not in email_groups:
                email_groups[normalized] = []
            email_groups[normalized].append(lead)
    
    merged_count = 0
    # Объединяем лиды в каждой группе
    async with database.transaction():
        for normalized_email, leads in email_groups.items():
            if len(leads) > 1:
                # Сортируем по ID, чтобы выбрать самый старый как основной
                leads_sorted = sorted(leads, key=lambda x: x['id'])
                main_lead = leads_sorted[0]
                secondary_leads = leads_sorted[1:]
                
                # Обновляем email основного лида до нормализованного
                if main_lead['email'].lower() != normalized_email:
                    update_query = Lead.__table__.update().where(Lead.id == main_lead['id']).values(email=normalized_email)
                    await database.execute(update_query)
                
                # Объединяем все вторичные лиды с основным
                for secondary in secondary_leads:
                    # Переносим прогресс (merge_lead_progress работает без транзакции)
                    await merge_lead_progress(secondary['id'], main_lead['id'])
                    
                    # Обновляем основной лид данными из вторичного
                    update_values = {}
                    if secondary['telegram_id'] and not main_lead['telegram_id']:
                        update_values['telegram_id'] = secondary['telegram_id']
                    if secondary['username'] and not main_lead['username']:
                        update_values['username'] = secondary['username']
                    if secondary['name'] and not main_lead['name']:
                        update_values['name'] = secondary['name']
                    if secondary['phone'] and not main_lead['phone']:
                        update_values['phone'] = secondary['phone']
                    if secondary['source_id'] and not main_lead['source_id']:
                        update_values['source_id'] = secondary['source_id']
                    
                    if update_values:
                        upd_query = Lead.__table__.update().where(Lead.id == main_lead['id']).values(**update_values)
                        await database.execute(upd_query)
                    
                    # Удаляем вторичный лид
                    delete_query = Lead.__table__.delete().where(Lead.id == secondary['id'])
                    await database.execute(delete_query)
                    merged_count += 1
    
    return merged_count

# Функции для работы с чатом
async def save_chat_message(session_id: str, message: str, is_from_user: bool):
    """Сохраняет сообщение чата в базу данных"""
    query = ChatMessage.__table__.insert().values(
        session_id=session_id,
        message=message,
        is_from_user=is_from_user
    )
    async with database.transaction():
        await database.execute(query)

async def get_chat_history(session_id: str, limit: int = 50):
    """Получает историю сообщений для сессии"""
    query = select(ChatMessage).filter_by(session_id=session_id).order_by(ChatMessage.created_at.asc()).limit(limit)
    async with database.transaction():
        return await database.fetch_all(query)

async def get_chat_message_count(session_id: str):
    """Получает количество сообщений от пользователя в сессии"""
    query = select(func.count(ChatMessage.id)).filter_by(session_id=session_id, is_from_user=True)
    async with database.transaction():
        result = await database.fetch_one(query)
        return result[0] if result else 0

# === Source functions ===

async def create_source(utm_source: str = None, utm_medium: str = None, utm_campaign: str = None, 
                        utm_term: str = None, utm_content: str = None, session_id: str = None) -> int:
    """Создает запись о посещении сайта с UTM-метками"""
    query = Source.__table__.insert().values(
        utm_source=utm_source,
        utm_medium=utm_medium,
        utm_campaign=utm_campaign,
        utm_term=utm_term,
        utm_content=utm_content,
        session_id=session_id
    )
    async with database.transaction():
        inserted_id = await database.execute(query)
        return int(inserted_id)

async def get_source_by_session_id(session_id: str):
    """Получает источник по session_id"""
    query = select(Source).filter_by(session_id=session_id).order_by(Source.created_at.desc()).limit(1)
    async with database.transaction():
        return await database.fetch_one(query)

async def link_source_to_lead(source_id: int, lead_id: int):
    """Связывает источник с лидом"""
    update_query = Source.__table__.update().where(Source.id == source_id).values(lead_id=lead_id)
    async with database.transaction():
        await database.execute(update_query)

async def get_source_statistics():
    """Получает статистику по источникам: сколько зашли, сколько стали лидами, сколько оплатили"""
    # Подзапрос для получения всех источников с их лидами
    query = """
        SELECT 
            COALESCE(s.utm_source, 'direct') AS source,
            COUNT(DISTINCT s.id) AS total_visits,
            COUNT(DISTINCT l.id) AS total_leads,
            COUNT(DISTINCT CASE WHEN u.paid = true THEN l.id END) AS paid_leads
        FROM sources s
        LEFT JOIN leads l ON l.source_id = s.id
        LEFT JOIN users u ON u.pay_email = l.email
        GROUP BY COALESCE(s.utm_source, 'direct')
        ORDER BY total_visits DESC
    """
    async with database.transaction():
        rows = await database.fetch_all(query)
    
    results = []
    for row in rows:
        source = row["source"] or "direct"
        total_visits = row["total_visits"] or 0
        total_leads = row["total_leads"] or 0
        paid_leads = row["paid_leads"] or 0
        
        lead_conversion = round((total_leads / total_visits) * 100, 2) if total_visits > 0 else 0
        paid_conversion = round((paid_leads / total_visits) * 100, 2) if total_visits > 0 else 0
        paid_from_leads = round((paid_leads / total_leads) * 100, 2) if total_leads > 0 else 0
        
        results.append({
            "source": source,
            "total_visits": total_visits,
            "total_leads": total_leads,
            "paid_leads": paid_leads,
            "lead_conversion_pct": f"{lead_conversion}%",
            "paid_conversion_pct": f"{paid_conversion}%",
            "paid_from_leads_pct": f"{paid_from_leads}%"
        })
    
    return results

async def get_all_sources(offset: int = 0, limit: int = 500, sort_by: str = 'created_at', sort_dir: str = 'desc'):
    """Получает все визиты (Source) с пагинацией"""
    from sqlalchemy import desc, asc
    order_by = desc(Source.created_at) if sort_dir == 'desc' else asc(Source.created_at)
    if sort_by == 'id':
        order_by = desc(Source.id) if sort_dir == 'desc' else asc(Source.id)
    elif sort_by == 'utm_source':
        order_by = desc(Source.utm_source) if sort_dir == 'desc' else asc(Source.utm_source)
    
    query = select(Source).order_by(order_by).offset(offset).limit(limit)
    async with database.transaction():
        rows = await database.fetch_all(query)
    
    return rows

async def get_sources_total_count():
    """Получает общее количество визитов"""
    query = select(func.count(Source.id))
    async with database.transaction():
        result = await database.fetch_one(query)
    return result[0] if result else 0

