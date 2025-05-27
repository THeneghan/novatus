"""Reusable SQLalchemy utils"""

from __future__ import annotations

from novatus_utils.models import Base
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker

POSTGRES_PASSWORD = "password"
POSTGRES_USER = "postgres"
engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost/postgres", echo=False)
Session = sessionmaker(engine)


def create_schema(engine: Engine):
    Base.metadata.create_all(engine)  # Non-destructive so if table already exists nothing happens


def delete_tables(engine: Engine):
    Base.metadata.drop_all(bind=engine)
