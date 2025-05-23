"""Reusable SQLalchemy utils"""

from __future__ import annotations

from novatus_utils.models import Base
from sqlalchemy import Engine


def create_tables(engine: Engine):
    Base.metadata.create_all(engine)  # Non-destructive so if table already exists nothing happens


def delete_tables(engine: Engine):
    Base.metadata.drop_all(bind=engine)
