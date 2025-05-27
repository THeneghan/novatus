"""Script to create schema, populate tables and create outputs"""

import datetime
import logging
import random

import pandas as pd
from faker import Faker
from novatus_utils.db import create_postgres_container
from novatus_utils.logging_utils import setup_logging, time_and_log
from novatus_utils.models import CustomerTransactions, GlobalTradeRepository
from novatus_utils.sqlalchemy_utils import POSTGRES_PASSWORD, POSTGRES_USER, Session, create_schema, engine
from report import generate_report_json

logger = logging.getLogger(__name__)

fake = Faker()

POSTGRES_CONTAINER_NAME = "novatus_local_db"


def _create_generic_dataframe(num_records: int, starting_point=1) -> pd.DataFrame:
    """Creates a typical dataset"""
    df = pd.DataFrame()
    for i in range(starting_point, num_records + starting_point):
        df.loc[i, "reported_id"] = "TX" + str(i)
        df.loc[i, "timestamp"] = datetime.datetime.now()
        df.loc[i, "amount"] = random.randint(500000, 1500000)
        df.loc[i, "currency"] = random.choice(["USD", "GBP", "EUR", "JPY", "HKD"])
        df.loc[i, "instrument_type"] = "IRS"
        df.loc[i, "region"] = random.choice(["EU", "APAC", "NA", "LATAM"])
        df.loc[i, "trade_type"] = random.choice(["New", "Amend", "Other"])
        df.loc[i, "reported_status"] = random.choice(["Settled", "Pending", "Deleted"])
    return df


@time_and_log(args_to_log=["num_records"])
def generate_global_trade_repo_data(num_records: int = 1000) -> pd.DataFrame:
    """Randomly generates data to populate the gtr_report table"""
    return _create_generic_dataframe(num_records)


@time_and_log(args_to_log=["fraction", "non_gtr_records"])
def generate_customer_transaction_data(df: pd.DataFrame, fraction=0.5, non_gtr_records=100) -> pd.DataFrame:
    """Takes global trade repo dataframe and generates customer transaction data by randomising etc"""
    sampled_df = df.sample(frac=fraction).rename(columns={"reported_id": "transaction_id", "reported_status": "status"})
    mismatched_records = sampled_df[sampled_df["amount"] % 2 == 0]
    # Introducing randomness into the records so we can look at mismatched records later
    for index, row in mismatched_records.iterrows():
        mismatched_records.loc[index, "amount"] = random.choice([row["amount"], random.randint(500000, 1500000)])
        mismatched_records.loc[index, "currency"] = random.choice(["USD", "GBP", "EUR", "JPY", "HKD"])
        mismatched_records.loc[index, "instrument_type"] = random.choice(["IRS", "HMRC"])
    sampled_df = pd.concat([sampled_df.drop(mismatched_records.index), mismatched_records])
    # Now adding customer transaction records to not be present in GTR,
    # to keep things simple will have these as IDs greater than any found in GTR
    non_gtr_data = _create_generic_dataframe(num_records=non_gtr_records, starting_point=len(df) + 1).rename(
        columns={"reported_id": "transaction_id", "reported_status": "status"}
    )
    customer_transaction_data = pd.concat([sampled_df, non_gtr_data])
    return customer_transaction_data


if __name__ == "__main__":
    setup_logging()
    create_postgres_container(POSTGRES_CONTAINER_NAME, POSTGRES_PASSWORD=POSTGRES_PASSWORD, POSTGRES_USER=POSTGRES_USER)
    create_schema(engine)
    global_trade_repo_data = generate_global_trade_repo_data()
    customer_transaction_data = generate_customer_transaction_data(global_trade_repo_data)
    with Session() as session:
        global_trade_repo_data.to_sql(
            name=GlobalTradeRepository.__tablename__, con=session.connection(), if_exists="append", index=False
        )
        customer_transaction_data.to_sql(
            name=CustomerTransactions.__tablename__, con=session.connection(), if_exists="append", index=False
        )
        print(generate_report_json(session))
        session.commit()
