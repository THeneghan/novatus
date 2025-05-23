"""SQLalchemy models"""

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class CustomerTransactions(Base):
    __tablename__ = " customer_transactions"
    transaction_id = Column(String, primary_key=True)
    timestamp = Column(DateTime)
    amount = Column(Integer)
    currency = Column(String)
    instrument_type = Column(String)
    region = Column(String)
    trade_type = Column(String)
    status = Column(String)


class GlobalTradeRepository(Base):
    __tablename__ = " gtr_report"
    reported_id = Column(String, primary_key=True)
    timestamp = Column(DateTime)
    amount = Column(Integer)
    currency = Column(String)
    instrument_type = Column(String)
    region = Column(String)
    trade_type = Column(String)
    reported_status = Column(String)
