from sqlalchemy import create_engine, func, and_, Column, Integer, String, ForeignKey, DateTime, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone, timedelta
import databases
from sqlalchemy.future import select
from typing import Optional, List, Union

# Создание базы данных и её подключение
DATABASE_URL = "sqlite+aiosqlite:///bot_database.db"
database = databases.Database(DATABASE_URL)

Base = declarative_base()

class TempUser(Base):
    __tablename__ = 'tempusers'

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    telegram_id = Column(String, unique=True, nullable=False)
    referrer_id = Column(String, nullable=True)  # Кто пригласил
    created_at = Column(DateTime, default=datetime.now(timezone.utc))  # Дата создания

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    telegram_id = Column(String, unique=True, nullable=False)
    paid = Column(Boolean, default=False)
    card_synonym = Column(String, unique=True, nullable=True)
    invite_link = Column(String, nullable=True)

    payments = relationship("Payment", back_populates="user")  # Убедитесь, что имя таблицы и свойства совпадают
    payouts = relationship("Payout", back_populates="user", foreign_keys="[Payout.telegram_id]")  # Ссылка на выплаты
    
    created_at = Column(DateTime, default=datetime.now(timezone.utc))  # Дата создания

class Payment(Base):
    __tablename__ = 'payments'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'), nullable=False)  # Ссылка на пользователя
    transaction_id = Column(String, default=None)  # Идентификатор транзакции
    idempotence_key = Column(String, nullable=False, unique=True)
    status = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))  # Дата создания

    user = relationship("User", back_populates="payments")  # Связь с пользователем

class Referral(Base):
    __tablename__ = 'referrals'

    id = Column(Integer, primary_key=True, index=True)
    referrer_id = Column(String, ForeignKey('users.telegram_id'))  # Кто пригласил
    referred_id = Column(String, ForeignKey('users.telegram_id'))  # Кто был приглашён
    created_at = Column(DateTime, default=datetime.now(timezone.utc))

    referrer = relationship("User", foreign_keys=[referrer_id])  # Связь с пригласившим
    referred_user = relationship("User", foreign_keys=[referred_id])  # Связь с приглашённым
    payout = relationship("Payout", back_populates="referral")  # Связь с выплатами

class Payout(Base):
    __tablename__ = 'payouts'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'))  # Ссылка на пользователя
    card_synonym = Column(String, nullable=False)
    amount = Column(Float)
    notified = Column(Boolean, default=False)
    transaction_id = Column(String, nullable=True)  # Идентификатор транзакции
    created_at = Column(DateTime, default=datetime.now(timezone.utc))

    referral_id = Column(Integer, ForeignKey('referrals.id'))  # Не знаю нафига

    user = relationship("User", back_populates="payouts", foreign_keys=[telegram_id])
    referral = relationship("Referral", back_populates="payout") # Не знаю нафига

class Binding(Base):
    __tablename__ = 'bindings'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'))  
    unique_str = Column(String, unique=True, nullable=False)

engine = create_engine(DATABASE_URL.replace("sqlite+aiosqlite", "sqlite"))
Base.metadata.create_all(bind=engine)

# Асинхронные функции с типами
async def create_user(telegram_id: str, username: str):
    query = User.__table__.insert().values(telegram_id=telegram_id, username=username)
    async with database.transaction():  # Используем async with для выполнения транзакции
        await database.execute(query)

async def get_user(telegram_id: str):
    query = select(User).filter_by(telegram_id=telegram_id)
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_one(query)

async def get_all_referred(telegram_id: str):
    query = select(Referral).filter_by(referrer_id=telegram_id)
    async with database.transaction():  # Используем async with для транзакции
        return await database.fetch_all(query)

async def get_referrer(telegram_id: str):
    query = select(Referral).filter_by(referred_id=telegram_id)
    async with database.transaction():  # Используем async with
        return await database.fetch_one(query)

async def get_referred(telegram_id: str):
    query = select(Referral).filter_by(referrer_id=telegram_id)
    async with database.transaction():  # Используем async with
        return await database.fetch_one(query)
    
async def get_referred_user(referred_id: int):
    query = select(User).filter_by(telegram_id=referred_id)
    result = await database.fetch_one(query)
    return result

async def create_referral(telegram_id: str, referrer_id: int):
    query = Referral.__table__.insert().values(referrer_id=referrer_id, referred_id=telegram_id)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(query)

async def mark_payout_as_notified(payout_id: int):
    query = select(Payout).filter_by(id=payout_id)
    async with database.transaction():  # Используем async with
        payout = await database.fetch_one(query)
        if payout:
            update_query = Payout.__table__.update().where(Payout.id == payout_id).values(notified=True)
            await database.execute(update_query)

async def create_temp_user(telegram_id: str, username: str, referrer_id: Optional[int] = None):
    query = TempUser.__table__.insert().values(telegram_id=telegram_id, username=username, referrer_id=referrer_id)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(query)

async def get_temp_user(telegram_id: str):
    query = select(TempUser).filter_by(telegram_id=telegram_id)
    async with database.transaction():  # Используем async with для транзакции
        return await database.fetch_one(query)

async def update_temp_user(telegram_id: str, username: Optional[str] = None):
    query = select(TempUser).filter_by(telegram_id=telegram_id)
    async with database.transaction():  # Используем async with для транзакции
        temp_user = await database.fetch_one(query)
        if temp_user:
            update_data = {'created_at': datetime.now(timezone.utc)}
            if username:
                update_data['username'] = username
            update_query = TempUser.__table__.update().where(TempUser.telegram_id == telegram_id).values(update_data)
            await database.execute(update_query)

async def save_invite_link_db(telegram_id: str, invite_link: str):
    update_data = {'invite_link': invite_link}
    update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_user_paid(telegram_id: str):
    update_data = {'paid': True}
    update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_payment_status(telegram_id: str):
    update_data = {'status': "success"}
    update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def update_user_card_synonym(telegram_id: str, card_synonym: str):
    update_data = {'card_synonym': card_synonym}
    update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(update_data)
    async with database.transaction():  # Используем async with для транзакции
        await database.execute(update_query)

async def delete_expired_records():
    expiration_date = datetime.now(timezone.utc) - timedelta(days=30)
    query = select(TempUser).filter(TempUser.created_at < expiration_date)
    async with database.transaction():  # Используем async with для транзакции
        expired_users = await database.fetch_all(query)
        expired_records_count = 0
        for user in expired_users:
            delete_query = TempUser.__table__.delete().where(TempUser.id == user['id'])
            await database.execute(delete_query)
            expired_records_count += 1
        return expired_records_count

async def get_all_paid_money(telegram_id: str):
    query = select(func.sum(Payout.amount)).filter(Payout.telegram_id == telegram_id)
    async with database.transaction():  # Используем async with для транзакции
        result = await database.fetch_one(query)
        return result[0] or 0.0

async def get_referral_count(telegram_id: str):
    query = select(func.count(Referral.id)).filter(Referral.referrer_id == telegram_id)
    async with database.transaction():  # Используем async with для транзакции
        result = await database.fetch_one(query)
        return result[0] or 0

async def get_paid_count(telegram_id: str):
    query = select(func.count(Referral.id)).join(User, Referral.referred_id == User.telegram_id)\
        .filter(Referral.referrer_id == telegram_id, User.paid == True)
    async with database.transaction():  # Используем async with для транзакции
        result = await database.fetch_one(query)
        return result[0] or 0

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

async def get_payment(telegram_id: str):
    query = select(Payment).filter_by(telegram_id=telegram_id)
    async with database.transaction():  # Здесь используем async with
        return await database.fetch_one(query)

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