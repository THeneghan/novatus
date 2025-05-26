"""Reusable SQLalchemy utils"""

from __future__ import annotations

from novatus_utils.models import Base, CustomerTransactions
from sqlalchemy import Engine, create_engine, select
from sqlalchemy.orm import sessionmaker

POSTGRES_PASSWORD = "password"
POSTGRES_USER = "postgres"
engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost/postgres", echo=False)
Session = sessionmaker(engine)


def create_tables(engine: Engine):
    Base.metadata.create_all(engine)  # Non-destructive so if table already exists nothing happens


def delete_tables(engine: Engine):
    Base.metadata.drop_all(bind=engine)


eligible_customer_transactions_query = (
    select(CustomerTransactions)
    .where(CustomerTransactions.amount > 1000000)
    .where(CustomerTransactions.status == "Settled")
    .where(CustomerTransactions.trade_type.in_(["New", "Amend"]))
    .where(CustomerTransactions.region.in_(["EU", "APAC"]))
)
