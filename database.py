from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from datetime import datetime
from typing import Generator

# Создание базы данных и её подключение
DATABASE_URL = "sqlite:///bot_database.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Функция для получения сессии базы данных
def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    telegram_id = Column(String, unique=True, nullable=False)
    balance = Column(Float, default=0.0)
    paid = Column(Boolean, default=False)
    card_synonym = Column(String, unique=True, nullable=True)

    payouts = relationship("Payout", back_populates="user", foreign_keys="[Payout.telegram_id]")  # Ссылка на выплаты


class Payout(Base):
    __tablename__ = 'payouts'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'))  # Ссылка на пользователя
    card_synonym = Column(String, ForeignKey('users.card_synonym'))  # Ссылка на карточный идентификатор
    amount = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    notified = Column(Boolean, default=False)
    referral_id = Column(Integer, ForeignKey('referrals.id'))  # Ссылка на реферал
    transaction_id = Column(String, nullable=True)  # Идентификатор транзакции
    status = Column(String, default="pending")  # Статус выплаты

    user = relationship("User", back_populates="payouts", foreign_keys=[telegram_id])
    referral = relationship("Referral", back_populates="payout")

class Referral(Base):
    __tablename__ = 'referrals'

    id = Column(Integer, primary_key=True, index=True)
    referrer_id = Column(Integer, ForeignKey('users.id'))  # Кто пригласил
    referred_id = Column(Integer, ForeignKey('users.id'))  # Кто был приглашён
    created_at = Column(DateTime, default=datetime.utcnow)

    referrer = relationship("User", foreign_keys=[referrer_id])  # Связь с пригласившим
    referred_user = relationship("User", foreign_keys=[referred_id])  # Связь с приглашённым
    payout = relationship("Payout", back_populates="referral")  # Связь с выплатами

class Binding(Base):
    __tablename__ = 'bindings'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String, ForeignKey('users.telegram_id'))  
    unique_str = Column(String, unique=True, nullable=False)

# Создание всех таблиц в базе данных
Base.metadata.create_all(bind=engine)

# Функции работы с пользователями и выплатами
def get_user(session: Session, telegram_id: str) -> User:
    """Получение пользователя по telegram_id"""
    return session.query(User).filter_by(telegram_id=telegram_id).first()

def create_payout(session: Session, user_id: int, amount: float):
    """Создание выплаты для пользователя"""
    payout = Payout(user_id=user_id, amount=amount)
    session.add(payout)
    session.commit()
    return payout

def mark_payout_as_notified(session: Session, payout_id: int):
    """Обновление статуса выплаты как уведомленной"""
    payout = session.query(Payout).filter_by(id=payout_id).first()
    if payout:
        payout.notified = True
        session.commit()

def create_referral(session: Session, referrer_id: int, referred_id: int):
    """Создание записи в таблице Referral (реферал)"""
    referral = Referral(referrer_id=referrer_id, referred_id=referred_id)
    session.add(referral)
    
    # Здесь мы не обновляем 'referral_count', так как его больше нет
    session.commit()
    return referral
