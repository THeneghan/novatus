"""Conftest for all db related utils"""

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
    return 5432


@pytest.fixture(scope="module")
def local_host():
    return "127.0.0.1"


@pytest.fixture(scope="module")
def table_name():
    return "test_table"


@pytest.fixture(scope="module")
def create_then_destroy_local_db(pg_password, container_name, db_name):
    create_postgres_container(container_name=container_name, POSTGRES_PASSWORD=pg_password, POSTGRES_DB=db_name)
    yield
    destroy_postgres_container(container_name=container_name)


@pytest.fixture(scope="module")
def local_db_connection(pg_password, local_host, port, create_then_destroy_local_db, db_name):
    conn = psycopg2.connect(database=db_name, password=pg_password, host=local_host, port=port, user="postgres")
    yield conn
    conn.close()
