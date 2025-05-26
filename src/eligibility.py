"""Placeholder for eligibility logic"""

import pandas as pd
from novatus_utils.models import CustomerTransactions, GlobalTradeRepository
from sqlalchemy import select
from yaml import SafeLoader, load


def get_eligible_records(df: pd.DataFrame) -> pd.DataFrame:
    eligibility_rules = load(open("../config.yaml", "r"), SafeLoader)["eligibility_rules"]
    return df[
        (df["amount"] > eligibility_rules["min_amount"])
        & df["trade_type"].isin(eligibility_rules["valid_trade_types"])
        & df["region"].isin(eligibility_rules["valid_regions"])
        & (df["status"] == "Settled")
    ]


eligible_customer_transactions_query = (
    select(CustomerTransactions)
    .where(CustomerTransactions.amount > 1000000)
    .where(CustomerTransactions.status == "Settled")
    .where(CustomerTransactions.trade_type.in_(["New", "Amend"]))
    .where(CustomerTransactions.region.in_(["EU", "APAC"]))
)
eligible_ctrs_in_gtr_query = (
    select(CustomerTransactions)
    .join(
        GlobalTradeRepository,
        GlobalTradeRepository.reported_id == CustomerTransactions.transaction_id,
        isouter=True,
    )
    .where(CustomerTransactions.amount > 1000000)
    .where(CustomerTransactions.status == "Settled")
    .where(CustomerTransactions.trade_type.in_(["New", "Amend"]))
    .where(CustomerTransactions.region.in_(["EU", "APAC"]))
)

ctrs_not_present_in_gtr_query = (
    select(CustomerTransactions)
    .join(
        GlobalTradeRepository,
        GlobalTradeRepository.reported_id == CustomerTransactions.transaction_id,
        isouter=True,
    )
    .where(CustomerTransactions.amount > 1000000)
    .where(CustomerTransactions.status == "Settled")
    .where(CustomerTransactions.trade_type.in_(["New", "Amend"]))
    .where(CustomerTransactions.region.in_(["EU", "APAC"]))
    .filter(GlobalTradeRepository.reported_id is None)
)
