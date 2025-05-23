import datetime
import logging
import random

import pandas as pd
from faker import Faker
from novatus_utils.db import create_postgres_container
from novatus_utils.logging_utils import setup_logging, time_and_log
from novatus_utils.models import CustomerTransactions, GlobalTradeRepository
from novatus_utils.sqlalchemy_utils import create_tables
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

fake = Faker()

POSTGRES_CONTAINER_NAME = "novatus_local_db"
POSTGRES_PASSWORD = "password"
POSTGRES_USER = "postgres"

engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost/postgres", echo=False)


def _create_generic_dataframe(num_records: int) -> pd.DataFrame:
    df = pd.DataFrame()
    for i in range(1, num_records):
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
    for index, row in mismatched_records.iterrows():
        mismatched_records.loc[index, "amount"] = random.choice([row["amount"], random.randint(500000, 1500000)])
        mismatched_records.loc[index, "currency"] = random.choice(["USD", "GBP", "EUR", "JPY", "HKD"])
        mismatched_records.loc[index, "instrument_type"] = random.choice(["IRS", "HMRC"])
    sampled_df = pd.concat([sampled_df.drop(mismatched_records.index), mismatched_records])
    # Now adding records to not be present in GTR
    non_gtr_data = _create_generic_dataframe(num_records=non_gtr_records).rename(
        columns={"reported_id": "transaction_id", "reported_status": "status"}
    )
    non_gtr_data['transaction_id'] = non_gtr_data['transaction_id'].map(lambda _: "TX" + str(random.randint(0, 1500000)))
    customer_transaction_data = pd.concat([sampled_df, non_gtr_data])
    return customer_transaction_data.drop_duplicates(subset=["transaction_id"])


if __name__ == "__main__":
    setup_logging()
    create_postgres_container(POSTGRES_CONTAINER_NAME, POSTGRES_PASSWORD=POSTGRES_PASSWORD, POSTGRES_USER=POSTGRES_USER)
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
