"""DB integration tests - slightly messy approach to fixtures, would ideally refactor"""

import datetime

import docker
from main import generate_customer_transaction_data, generate_global_trade_repo_data
from novatus_utils.db import create_postgres_sqlalchemy_engine
from novatus_utils.models import (
    CustomerTransactions,
    EligibleCustomerTransactions,
    GlobalTradeRepository,
    MismatchedRecords,
    MissingCustomerTransactionsGTR,
)
from novatus_utils.sqlalchemy_utils import create_schema, delete_tables
from report import generate_report_json
from sqlalchemy import func, select, text
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
    create_schema(engine)
    global_trade_repo_data = generate_global_trade_repo_data()
    customer_transaction_data = generate_customer_transaction_data(global_trade_repo_data)
    with Session(engine) as session:
        global_trade_repo_data.to_sql(
            name=GlobalTradeRepository.__tablename__, con=session.connection(), if_exists="append", index=False
        )
        customer_transaction_data.to_sql(
            name=CustomerTransactions.__tablename__, con=session.connection(), if_exists="append", index=False
        )
        generate_report_json(session)
        session.commit()
    delete_tables(engine)


def test_smoke_v2(
    create_then_destroy_local_db, pg_user, pg_password, local_host, db_name, port, create_then_destroy_tables
):
    """Messing around with a cleaner create then destroy tables process"""
    engine = create_postgres_sqlalchemy_engine(user=pg_user, password=pg_password, db_name=db_name, port=port)
    global_trade_repo_data = generate_global_trade_repo_data()
    customer_transaction_data = generate_customer_transaction_data(global_trade_repo_data)
    with Session(engine) as session:
        global_trade_repo_data.to_sql(
            name=GlobalTradeRepository.__tablename__, con=session.connection(), if_exists="append", index=False
        )
        customer_transaction_data.to_sql(
            name=CustomerTransactions.__tablename__, con=session.connection(), if_exists="append", index=False
        )
        generate_report_json(session)
        session.commit()


def test_eligible_customer_transaction_logic(
    create_then_destroy_local_db, pg_user, pg_password, local_host, db_name, port
):
    # TODO: Refactor heavily
    engine = create_postgres_sqlalchemy_engine(user=pg_user, password=pg_password, db_name=db_name, port=port)
    create_schema(engine)
    with Session(engine) as session:
        eligible_customer_transaction = CustomerTransactions.__table__.insert().values(
            transaction_id="TX1",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            status="Settled",
        )
        failing_on_amount_record = CustomerTransactions.__table__.insert().values(
            transaction_id="TX2",
            timestamp=datetime.datetime.now(),
            amount=5,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            status="Settled",
        )
        failing_on_trade_type = CustomerTransactions.__table__.insert().values(
            transaction_id="TX3",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Foo",
            status="Settled",
        )
        failing_on_status = CustomerTransactions.__table__.insert().values(
            transaction_id="TX4",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            status="bar",
        )
        failing_on_region = CustomerTransactions.__table__.insert().values(
            transaction_id="TX5",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="Bar",
            trade_type="Amend",
            status="Settled",
        )

        session.execute(eligible_customer_transaction)
        session.execute(failing_on_amount_record)
        session.execute(failing_on_trade_type)
        session.execute(failing_on_status)
        session.execute(failing_on_region)
        eligible_customer_transaction_count_stmnt = select(func.count()).select_from(EligibleCustomerTransactions)
        assert session.execute(eligible_customer_transaction_count_stmnt).scalar() == 1
        session.commit()
    delete_tables(engine)


def test_missing_customer_transaction(create_then_destroy_local_db, pg_user, pg_password, local_host, db_name, port):
    # TODO: Refactor heavily
    engine = create_postgres_sqlalchemy_engine(user=pg_user, password=pg_password, db_name=db_name, port=port)
    create_schema(engine)
    with Session(engine) as session:
        eligible_customer_transaction = CustomerTransactions.__table__.insert().values(
            transaction_id="TX1",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            status="Settled",
        )
        eligible_customer_transaction2 = CustomerTransactions.__table__.insert().values(
            transaction_id="TX2",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            status="Settled",
        )
        single_gtr_record = GlobalTradeRepository.__table__.insert().values(
            reported_id="TX2",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            reported_status="Settled",
        )

        session.execute(eligible_customer_transaction)
        session.execute(eligible_customer_transaction2)
        session.execute(single_gtr_record)

        eligible_customer_transaction_count_stmnt = select(func.count()).select_from(MissingCustomerTransactionsGTR)
        assert session.execute(eligible_customer_transaction_count_stmnt).scalar() == 1
        session.commit()
    delete_tables(engine)


def test_mismatched_records(create_then_destroy_local_db, pg_user, pg_password, local_host, db_name, port):
    # TODO: Refactor heavily
    engine = create_postgres_sqlalchemy_engine(user=pg_user, password=pg_password, db_name=db_name, port=port)
    create_schema(engine)
    with Session(engine) as session:
        eligible_customer_transaction = CustomerTransactions.__table__.insert().values(
            transaction_id="TX1",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            status="Settled",
        )
        matching_gtr_transaction = GlobalTradeRepository.__table__.insert().values(
            reported_id="TX1",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            reported_status="Settled",
        )
        eligible_customer_transaction2 = CustomerTransactions.__table__.insert().values(
            transaction_id="TX2",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            status="Settled",
        )
        differing_amount_gtr_transaction = GlobalTradeRepository.__table__.insert().values(
            reported_id="TX2",
            timestamp=datetime.datetime.now(),
            amount=5000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            reported_status="Settled",
        )
        eligible_customer_transaction3 = CustomerTransactions.__table__.insert().values(
            transaction_id="TX3",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            status="Settled",
        )
        differing_currency_gtr_transaction = GlobalTradeRepository.__table__.insert().values(
            reported_id="TX3",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="JPY",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            reported_status="Settled",
        )
        eligible_customer_transaction4 = CustomerTransactions.__table__.insert().values(
            transaction_id="TX4",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="IRS",
            region="EU",
            trade_type="Amend",
            status="Settled",
        )
        differing_instrument_type_gtr_transaction = GlobalTradeRepository.__table__.insert().values(
            reported_id="TX4",
            timestamp=datetime.datetime.now(),
            amount=1000001,
            currency="USD",
            instrument_type="HMRC",
            region="EU",
            trade_type="Amend",
            reported_status="Settled",
        )

        session.execute(eligible_customer_transaction)
        session.execute(matching_gtr_transaction)
        session.execute(eligible_customer_transaction2)
        session.execute(differing_amount_gtr_transaction)
        session.execute(eligible_customer_transaction3)
        session.execute(differing_currency_gtr_transaction)
        session.execute(eligible_customer_transaction4)
        session.execute(differing_instrument_type_gtr_transaction)
        missing_customer_transaction_stmnt = select(func.count()).select_from(MismatchedRecords)
        assert session.execute(missing_customer_transaction_stmnt).scalar() == 3
        session.commit()
    delete_tables(engine)
