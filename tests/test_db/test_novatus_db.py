"""Test suite for local database."""

import docker
import pytest
from novatus_utils.db import create_postgres_sqlalchemy_engine
from sqlalchemy import text


@pytest.fixture(scope="module")
def container_name():
    return "novatus_local_db"


@pytest.fixture(scope="module")
def db_name():
    return "novatus"


@pytest.fixture
def setup_simple_table(table_name, local_db_connection):
    with local_db_connection:
        with local_db_connection.cursor() as curs:
            curs.execute(f"""CREATE TABLE {table_name} (id serial PRIMARY KEY, num integer, data varchar);""")


@pytest.fixture
def simple_data_entry(table_name, setup_simple_table, local_db_connection):
    with local_db_connection:
        with local_db_connection.cursor() as curs:
            curs.execute(f"""INSERT INTO {table_name} (id, num,data) VALUES (%s, %s, %s)""", (1, 1, "def"))


def test_local_db(create_then_destroy_local_db, container_name):
    client = docker.from_env()
    assert container_name in [container.name for container in client.containers.list()]


def test_create_postgres_sqlalchemy_engine(create_then_destroy_local_db, pg_user, pg_password, local_host, db_name):
    engine = create_postgres_sqlalchemy_engine(user=pg_user, password=pg_password, db_name=db_name)
    with engine.connect() as conn:
        result = conn.execute(text("select 'hello world'"))
        collected_result = result.all()[0][0]
    assert collected_result == "hello world"
