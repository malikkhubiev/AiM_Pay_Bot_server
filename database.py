from sqlalchemy import insert, create_engine, func, and_, Column, Integer, String, ForeignKey, DateTime, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone, timedelta
import databases
from sqlalchemy import select
from typing import Optional
import logging

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
    
class PromoUser(Base):
    __tablename__ = 'promousers'

    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'), unique=True, nullable=False)  # Внешний ключ

    user = relationship("User", back_populates="promousers")  # Связь с пользователем

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    telegram_id = Column(String, unique=True, nullable=False)
    paid = Column(Boolean, default=False)
    balance = Column(Integer, default=0)
    card_synonym = Column(String, unique=True, nullable=True)
    invite_link = Column(String, nullable=True)

    payments = relationship("Payment", back_populates="user")  # Убедитесь, что имя таблицы и свойства совпадают
    payouts = relationship("Payout", back_populates="user", foreign_keys="[Payout.telegram_id]")  # Ссылка на выплаты
    promousers = relationship("PromoUser", back_populates="user")  # Связь с таблицей PromoUser

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
    referred_id = Column(String, ForeignKey('users.telegram_id'), unique=True)  # Кто был приглашён
    status = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))

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
    status = Column(String, nullable=False)
    notified = Column(Boolean, default=False)
    transaction_id = Column(String, nullable=True)  # Идентификатор транзакции
    created_at = Column(DateTime, default=datetime.now(timezone.utc))

    referral_id = Column(Integer, ForeignKey('referrals.id'))  # Не знаю нафига, но без этого не работает

    user = relationship("User", back_populates="payouts", foreign_keys=[telegram_id])
    referral = relationship("Referral", back_populates="payout") # Не знаю нафига

class Binding(Base):
    __tablename__ = 'bindings'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'))  
    unique_str = Column(String, unique=True, nullable=False)

engine = create_engine(DATABASE_URL.replace("sqlite+aiosqlite", "sqlite"))
def initialize_database():
    """Создает базу данных, если она еще не создана."""
    Base.metadata.create_all(bind=engine)

# Асинхронные функции с типами
async def create_user(telegram_id: str, username: str):
    query = User.__table__.insert().values(telegram_id=telegram_id, username=username)
    async with database.transaction():  # Используем async with для выполнения транзакции
        await database.execute(query)

async def create_pending_payout(
        telegram_id: str,
        card_synonym: str,
        idempotence_key: str,
        payout_amount: str
    ):
    query = """ 
        INSERT INTO payouts (telegram_id, card_synonym, idempotence_key, amount, status, created_at) 
        VALUES (:telegram_id, :card_synonym, :idempotence_key, :amount, :status, :created_at) 
    """
    values = { 
        "telegram_id": telegram_id, 
        "card_synonym": card_synonym,
        "idempotence_key": idempotence_key,
        "amount": payout_amount, 
        "status": "pending", 
        "created_at": datetime.now(timezone.utc)
    } 
    async with database.transaction():  # Используем async with для выполнения транзакции
        await database.execute(query, values)

async def get_user(telegram_id: str):
    query = select(User).filter_by(telegram_id=telegram_id)
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
    result = await database.fetch_one(query)
    return result
    
async def get_promo_user(referred_id: int):
    query = select(PromoUser).filter_by(telegram_id=referred_id)
    result = await database.fetch_one(query)
    return result

async def get_promo_user_count():
    query = "SELECT COUNT(*) FROM promousers"
    async with database.transaction():
        return await database.fetch_val(query)

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

async def create_temp_user(telegram_id: str, username: str, referrer_id: Optional[int] = None):
    query = insert(TempUser).values(telegram_id=telegram_id, username=username, referrer_id=referrer_id).returning(TempUser)
    async with database.transaction():  # Используем async with для транзакции
        result = await database.fetch_one(query)  # Выполняем запрос и получаем одну запись
        return result  # Возвращаем результат, который является созданной записью

async def get_temp_user(telegram_id: str):
    query = select(TempUser).filter_by(telegram_id=telegram_id)
    async with database.transaction():  # Используем async with для транзакции
        return await database.fetch_one(query)

async def update_referrer(telegram_id: str, referrer_id: str):
    update_data = {'referrer_id': referrer_id}
    update_query = Referral.__table__.update().where(Referral.referred_id == telegram_id).values(update_data)
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

async def update_payment_done(telegram_id: str, transaction_id: str):
    user_update_data = {'paid': True}
    user_update_query = User.__table__.update().where(User.telegram_id == telegram_id).values(user_update_data)
    payment_update_data = {"status": "success", "transaction_id": transaction_id}
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

async def delete_expired_records():
    expiration_date = datetime.now(timezone.utc) - timedelta(days=30)
    logging.info(f"expiration_date = {expiration_date}")
    query = select(TempUser).where(TempUser.created_at < expiration_date)
    logging.info(f"query = {query}")
    async with database.transaction():  # Используем async with для транзакции
        expired_users = await database.fetch_all(query)
        logging.info(f"expired_users = {expired_users}")
        expired_records_count = 0
        for user in expired_users:
            delete_query = TempUser.__table__.delete().where(TempUser.telegram_id == user['telegram_id'])
            await database.execute(delete_query)
            expired_records_count += 1
        logging.info(f"expired_records_count = {expired_records_count}")
        return expired_records_count

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
