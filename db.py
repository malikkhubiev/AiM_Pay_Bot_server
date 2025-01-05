from sqlalchemy import create_engine, func, and_, Column, Integer, String, ForeignKey, DateTime, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone, timedelta

from functools import wraps

def session_manager(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        session = SessionLocal()  # Открываем сессию
        try:
            result = func(session, *args, **kwargs)  # Передаем сессию в функцию
            session.commit()  # Сохраняем изменения в базе данных
            return result  # Возвращаем результат функции
        except Exception as e:
            session.rollback()  # Откатываем изменения в случае ошибки
            raise e  # Перебрасываем исключение дальше
        finally:
            session.close()  # Закрываем сессию после выполнения функции
    return wrapper

# Создание базы данных и её подключение
DATABASE_URL = "sqlite+aiosqlite:///bot_database.db"

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
    transaction_id = Column(String, nullable=False, unique=True)  # Идентификатор транзакции
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

@session_manager
def create_user(session, telegram_id, username):
    new_user = User(
        telegram_id=telegram_id,
        username=username,
    )
    session.add(new_user)  # Добавляем пользователя в сессию

@session_manager
def get_user(session, telegram_id):
    return session.query(User).filter_by(telegram_id=telegram_id).first()

@session_manager
def get_all_referred(session, telegram_id):
    return session.query(Referral).filter_by(referrer_id=telegram_id).all()

@session_manager
def get_referrer(session, telegram_id):
    return session.query(Referral).filter_by(referred_id=telegram_id).first()

@session_manager
def get_referred(session, telegram_id):
    return session.query(Referral).filter_by(referrer_id=telegram_id).first()

@session_manager
def get_referred_user(session, referred_id):
    return session.query(User).filter_by(telegram_id=referred_id).first()

@session_manager
def create_referral(session, telegram_id, referrer_id):
    new_referrer = Referral(
        referrer_id=referrer_id,
        referred_id=telegram_id
    )
    session.add(new_referrer)

@session_manager
def mark_payout_as_notified(session, payout_id: int):
    """Обновление статуса выплаты как уведомленной"""
    payout = session.query(Payout).filter_by(id=payout_id).first()
    if payout:
        payout.notified = True

@session_manager
def create_temp_user(session, telegram_id, username, referrer_id=None):
    new_temp_user = TempUser(
        telegram_id=telegram_id,
        username=username,
        referrer_id=referrer_id
    )
    session.add(new_temp_user)

@session_manager
def get_temp_user(session, telegram_id):
    return session.query(TempUser).filter_by(telegram_id=telegram_id).first()  

@session_manager
def update_temp_user(session, telegram_id, username=None):
    temp_user = session.query(TempUser).filter_by(telegram_id=telegram_id).first()
    temp_user.createdAt = datetime.now(timezone.utc)
    if username:
        temp_user.username = username

@session_manager
def delete_expired_records(session):
    expiration_date = datetime.now(timezone.utc) - timedelta(days=30)
    expired_records_count = session.query(TempUser).filter(TempUser.created_at < expiration_date).delete()
    return expired_records_count

@session_manager  
def get_all_paid_money(session, telegram_id):
    return session.query(func.sum(Payout.amount))\
            .filter(and_(Payout.telegram_id == telegram_id))\
            .scalar() or 0.0

@session_manager  
def get_referral_count(session, telegram_id):
    return session.query(func.count(Referral.id))\
        .filter(Referral.referrer_id == telegram_id).scalar()

@session_manager  
def get_paid_count(session, telegram_id):
    return session.query(func.count(Referral.id))\
        .join(User, Referral.referred_id == User.telegram_id)\
        .filter(Referral.referrer_id == telegram_id, User.paid == True).scalar()

@session_manager
def get_payment(session, payment_id):
    return session.query(Payment).filter_by(transaction_id=payment_id).first()

@session_manager
def create_payment(session, telegram_id, payment_id):
    new_user = User(
        transaction_id=payment_id,
        telegram_id=telegram_id
    )
    session.add(new_user)

@session_manager
def create_payout(
    session,
    telegram_id,
    card_synonym,
    amount,
    transaction_id):
    new_user = Payout(
        card_synonym=card_synonym,
        telegram_id=telegram_id,
        amount=amount,
        transaction_id=transaction_id
    )
    session.add(new_user)

@session_manager
def create_binding_and_delete_if_exists(session, telegram_id, unique_str):
    binding = session.query(Binding).filter_by(telegram_id=telegram_id).first()
    if binding:
        session.delete(binding)
    binding = Binding(
        telegram_id=telegram_id,
        unique_str=unique_str
    )
    session.add(binding)

@session_manager
def get_binding_by_unique_str(session, unique_str):
    return session.query(Binding).filter(unique_str == unique_str).first()

@session_manager
def get_binding_by_unique_str(session, unique_str):
    return session.query(Binding).filter(unique_str == unique_str).first()