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
    
class PromoUser(Base):
    __tablename__ = 'promousers'

    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'), unique=True, nullable=False)  # Внешний ключ
    created_at = Column(DateTime, nullable=False, server_default=func.now())  # Дата создания

    user = relationship("User", back_populates="promousers")  # Связь с пользователем

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

    is_registered = Column(Boolean, default=False)
    source = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())

    payments = relationship("Payment", back_populates="user")
    payouts = relationship("Payout", back_populates="user", foreign_keys="[Payout.telegram_id]")
    promousers = relationship("PromoUser", back_populates="user")

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

engine = create_engine(DATABASE_URL.replace("sqlite+aiosqlite", "sqlite"))

async def initialize_settings_once():
    existing_keys_query = select(Setting.key)
    existing_keys_result = await database.fetch_all(existing_keys_query)
    existing_keys = {row["key"] for row in existing_keys_result}

    insert_values = [
        {"key": key, "value": value}
        for key, value in DEFAULT_SETTINGS.items()
        if key not in existing_keys
    ]

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

# Асинхронные функции с типами
async def update_temp_user_registered(telegram_id: str):
    update_data = {'is_registered': True}
    update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def create_pending_payout(
        telegram_id: str,
        card_synonym: str,
        idempotence_key: str,
        payout_amount: str
    ):
    query = """ 
        INSERT INTO payouts (telegram_id, card_synonym, idempotence_key, amount, status) 
        VALUES (:telegram_id, :card_synonym, :idempotence_key, :amount, :status) 
    """
    values = { 
        "telegram_id": telegram_id, 
        "card_synonym": card_synonym,
        "idempotence_key": idempotence_key,
        "amount": payout_amount, 
        "status": "pending"
    } 
    async with database.transaction():  # Используем async with для выполнения транзакции
        await database.execute(query, values)

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
        telegram_id=telegram_id,
        is_registered=True
    )
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_one(query)

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
    
async def get_promo_user(referred_id: int):
    query = select(PromoUser).filter_by(telegram_id=referred_id)
    async with database.transaction():
        return await database.fetch_one(query)

async def get_promo_user_count():
    query = "SELECT COUNT(*) FROM promousers"
    async with database.transaction():
        return await database.fetch_val(query)

async def get_promo_users_count():
    """ Получает количество промокодеров, сгруппированных по датам. """
    query = (
        select(
            func.date(PromoUser.created_at).label("date"),
            func.count().label("promo_users_count")
        )
        .group_by(func.date(PromoUser.created_at))
        .order_by(func.date(PromoUser.created_at))
    )
    async with database.transaction():
        return await database.fetch_all(query)

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

async def get_temp_user(telegram_id: str):
    query = select(User).filter_by(telegram_id=telegram_id, is_registered=False)
    async with database.transaction():
        return await database.fetch_one(query)

async def get_conversion_stats_by_source():
    query = """ 
        SELECT 
            source,
            COUNT(*) AS total_users,
            COUNT(*) FILTER (WHERE is_registered = true) AS registered_users,
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
            COUNT(*) FILTER (WHERE is_registered = true) AS registered_users,
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
    
async def get_expired_users():
    """
    Возвращает список telegram_id пользователей, у которых истёк пробный период.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    query = select(User.telegram_id).filter(
        User.date_of_trial_ends <= now,
        User.date_of_trial_ends.is_not(None),
        User.paid == False  # Проверяем, что пользователь не оплатил
    )
    async with database.transaction():
        rows = await database.fetch_all(query)
    
    # Возвращаем список telegram_id
    return {
        "now": now,
        "rows": [row.telegram_id for row in rows]
    } 
        

async def add_promo_user(telegram_id: str):
    query = "INSERT INTO promousers (telegram_id) VALUES (:telegram_id)"
    values = {"telegram_id": telegram_id}
    async with database.transaction():
        await database.execute(query, values)

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

async def create_temp_user(telegram_id: str, username: str):
    unique_str = str(uuid.uuid4())
    query = insert(User).values(
        telegram_id=telegram_id,
        username=username,
        unique_str=unique_str,
        is_registered=False
    ).returning(User)
    async with database.transaction():
        result = await database.fetch_one(query)
        return result

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

async def set_user_trial_end(telegram_id: str):
    # Добавляем 24 часа и 15 секунд к текущему времени
    # new_end_time = current_time + timedelta(hours=24)
    new_end_time = datetime.now(timezone.utc) + timedelta(minutes=2)

    update_data = {
        "date_of_trial_ends": new_end_time
    }
    update_query = User.__table__.update().where(
        User.telegram_id == telegram_id
    ).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def set_user_fake_paid(telegram_id: str):
    update_data = {"paid": True}
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

async def update_temp_user(telegram_id: str, username: Optional[str] = None):
    query = select(User).filter_by(telegram_id=telegram_id, is_registered=False)
    async with database.transaction():
        temp_user = await database.fetch_one(query)
        if temp_user:
            update_data = {'created_at': datetime.now(timezone.utc)}
            if username:
                update_data['username'] = username
            update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
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

async def save_invite_link_db(telegram_id: str, invite_link: str):
    update_data = {'invite_link': invite_link}
    update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def delete_expired_records():
    expiration_date = datetime.now(timezone.utc) - timedelta(days=30)
    query = select(User).where(
        and_(
            User.created_at < expiration_date,
            User.is_registered == False
        )
    )
    async with database.transaction():
        expired_users = await database.fetch_all(query)
        expired_records_count = 0
        for user in expired_users:
            delete_query = User.__table__.update().where(
                User.id == user['id']
            ).values(
                telegram_id=None,
                username=None
            )
            await database.execute(delete_query)
            expired_records_count += 1
        return expired_records_count

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

