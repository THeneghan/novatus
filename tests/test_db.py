"""DB integration tests"""

import docker
from generate_fake_tables import generate_customer_transaction_data, generate_global_trade_repo_data
from novatus_utils.db import create_postgres_sqlalchemy_engine
from novatus_utils.models import CustomerTransactions, GlobalTradeRepository
from novatus_utils.sqlalchemy_utils import create_tables
from sqlalchemy import text
from sqlalchemy.orm import Session


def test_local_db(create_then_destroy_local_db, container_name):
    client = docker.from_env()
    assert container_name in [container.name for container in client.containers.list()]


def test_create_postgres_sqlalchemy_engine(
    create_then_destroy_local_db, pg_user, pg_password, local_host, db_name, port
):
    engine = create_postgres_sqlalchemy_engine(user=pg_user, password=pg_password, db_name=db_name, port=port)
    with engine.connect() as conn:
        result = conn.execute(text("select 'hello world'"))
        collected_result = result.all()[0][0]
    assert collected_result == "hello world"


def test_smoke(create_then_destroy_local_db, pg_user, pg_password, local_host, db_name, port):
    engine = create_postgres_sqlalchemy_engine(user=pg_user, password=pg_password, db_name=db_name, port=port)
    create_tables(engine)
    global_trade_repo_data = generate_global_trade_repo_data()
    customer_transaction_data = generate_customer_transaction_data(global_trade_repo_data)
    with Session(engine) as session:
        global_trade_repo_data.to_sql(
            name=GlobalTradeRepository.__tablename__, con=session.connection(), if_exists="append", index=False
        )
        customer_transaction_data.to_sql(
            name=CustomerTransactions.__tablename__, con=session.connection(), if_exists="append", index=False
        )
        session.commit()
