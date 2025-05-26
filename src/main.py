"""Main script"""

import logging

import pandas as pd
from eligibility import get_eligible_records
from novatus_utils.logging_utils import setup_logging
from novatus_utils.models import CustomerTransactions, GlobalTradeRepository
from novatus_utils.sqlalchemy_utils import POSTGRES_PASSWORD, POSTGRES_USER, Session
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost/postgres", echo=False)

CORE_FIELDS = {"amount", "currency", "instrument_type"}

if __name__ == "__main__":
    setup_logging()
    simple_pipeline = False
    if simple_pipeline:
        customer_transactions_df = pd.read_csv("../data/customer_transactions.csv")
        gtr_df = pd.read_csv("../data/gtr_report.csv")
    else:
        with Session() as session:
            customer_transactions_df = pd.read_sql_table(
                table_name=CustomerTransactions.__tablename__, con=session.connection()
            )
            gtr_df = pd.read_sql_table(table_name=GlobalTradeRepository.__tablename__, con=session.connection())
    eligible_transactions_df = customer_transactions_df.pipe(get_eligible_records)
    common_transactions = pd.merge(
        eligible_transactions_df,
        gtr_df,
        how="left",
        left_on="transaction_id",
        right_on="reported_id",
        indicator=True,
        suffixes=("_ct", "_gtr"),
    )
    missing_from_gtr = common_transactions[common_transactions["_merge"] == "left_only"]
    if not missing_from_gtr.empty:
        logger.warning("Missing transactions from GTR")
    reconciliatory_df = common_transactions[common_transactions["_merge"] == "both"]
    mismatches_df = pd.DataFrame()
    for field in CORE_FIELDS:
        missing_records = reconciliatory_df[reconciliatory_df[field + "_ct"] != reconciliatory_df[field + "_gtr"]]
        mismatches_df = pd.concat([mismatches_df, missing_records])
    cols_to_drop = [col for col in mismatches_df.columns if "_ct" in col]
    mismatches_df.df.drop(columns=["region_ct", "C"])
