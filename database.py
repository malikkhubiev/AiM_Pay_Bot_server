from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from datetime import datetime, timezone, timedelta
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

# Создание всех таблиц в базе данных
Base.metadata.create_all(bind=engine)

def add_initial_user():
    session = SessionLocal()
    try:
        # Проверка, существует ли пользователь с заданным telegram_id
        existing_user = session.query(User).filter_by(telegram_id="999").first()
        if not existing_user:
            new_user = User(
                id=1000000,
                username="Malik_The_Author",
                telegram_id="999"
            )
            session.add(new_user)
            session.commit()
    finally:
        session.close()

add_initial_user()

def mark_payout_as_notified(session: Session, payout_id: int):
    """Обновление статуса выплаты как уведомленной"""
    payout = session.query(Payout).filter_by(id=payout_id).first()
    if payout:
        payout.notified = True
        session.commit()

# Функция для удаления старых записей
def create_temp_user(telegram_id, username, referrer_id=None):
    with SessionLocal() as session:  # Создаём сессию для работы с базой
        try:
            new_temp_user = TempUser(
                telegram_id=telegram_id,
                username=username,
                referrer_id=referrer_id
            )
            session.add(new_temp_user)
            session.commit()
        except Exception as e:
            session.rollback()
            raise ValueError(f"Ошибка при создании временного пользователя: {e}")

def get_temp_user(telegram_id):
    try:
        with SessionLocal() as session:  # Создаём сессию для работы с базой
            temp_user = session.query(TempUser).filter_by(telegram_id=telegram_id).first()  
            return temp_user
    except Exception as e:
        session.rollback()
        raise ValueError(f"Ошибка при получении временного пользователя: {e}")

# Функция для удаления старых записей
def update_temp_user(telegram_id, username=None):
    try:
        with SessionLocal() as session:  # Создаём сессию для работы с базой
            temp_user = session.query(TempUser).filter_by(telegram_id=telegram_id).first()
            temp_user.createdAt = datetime.now(timezone.utc)
            if username:
                temp_user.username = username
            session.commit()
    except Exception as e:
        session.rollback()
        raise ValueError(f"Ошибка при получении временного пользователя: {e}")

# Функция для удаления старых записей
def delete_expired_records():
    try:
        with SessionLocal() as session:  # Создаём сессию для работы с базой
            expiration_date = datetime.now(timezone.utc) - timedelta(days=30)
            expired_records_count = session.query(TempUser).filter(TempUser.created_at < expiration_date).delete()
            session.commit()
            return expired_records_count
    except Exception as e:
        session.rollback()
        raise ValueError(f"Ошибка при получении временного пользователя: {e}")