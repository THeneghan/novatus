"""Reusable fixtures as per conftest"""

import psycopg2
import pytest
from novatus_utils.db import create_postgres_container, destroy_postgres_container


@pytest.fixture(scope="module")
def pg_password():
    return "password"


@pytest.fixture(scope="module")
def pg_user():
    return "postgres"


@pytest.fixture(scope="module")
def port():
    return 15432


@pytest.fixture(scope="module")
def local_host():
    return "127.0.0.1"


@pytest.fixture(scope="module")
def table_name():
    return "test_table"


@pytest.fixture(scope="module")
def create_then_destroy_local_db(pg_password, container_name, db_name, port):
    create_postgres_container(
        container_name=container_name, POSTGRES_PASSWORD=pg_password, POSTGRES_DB=db_name, host_port=port
    )
    yield
    destroy_postgres_container(container_name=container_name)


@pytest.fixture(scope="module")
def local_db_connection(pg_password, local_host, port, create_then_destroy_local_db, db_name):
    conn = psycopg2.connect(database=db_name, password=pg_password, host=local_host, port=port, user="postgres")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def container_name():
    return "novatus_test_db"


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
