"""SQL version of main"""

import pandas as pd
from novatus_utils.models import CustomerTransactions, GlobalTradeRepository
from novatus_utils.sqlalchemy_utils import Session, select
from sqlalchemy import or_

RECONCILABLE_CORE_FIELDS = {"amount", "currency", "instrument_type"}


with Session() as session:
    eligible_customer_transactions_query = (
        select(CustomerTransactions)
        .where(CustomerTransactions.amount > 1000000)  # Move these to constants
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

    mismatched_ctrs_query = (
        select(CustomerTransactions, GlobalTradeRepository)
        .join(
            GlobalTradeRepository,
            GlobalTradeRepository.reported_id == CustomerTransactions.transaction_id,
        )
        .where(CustomerTransactions.amount > 1000000)
        .where(CustomerTransactions.status == "Settled")
        .where(CustomerTransactions.trade_type.in_(["New", "Amend"]))
        .where(CustomerTransactions.region.in_(["EU", "APAC"]))
        .filter(
            or_(
                CustomerTransactions.amount != GlobalTradeRepository.amount,
                CustomerTransactions.currency != GlobalTradeRepository.currency,
                CustomerTransactions.instrument_type != GlobalTradeRepository.instrument_type,
            )
        )
    )

    # eligible_customer_results = session.execute(eligible_customer_transactions_query)
    # eligible_customer_join = session.execute(eligible_ctrs_in_gtr_query)
    # ctrs_not_present_in_gtr = session.execute(ctrs_not_present_in_gtr_query)
    mismatched_ctrs = session.execute(mismatched_ctrs_query)

    mismatched_records = pd.read_sql(mismatched_ctrs_query, session.bind)
    mismatched_records = mismatched_records.rename(
        columns={col: col.replace("_1", "_gtr") for col in mismatched_records.columns if "_1" in col}
    )
    mismatched_records = mismatched_records.pipe()

    mismatched_records.to_sql(
        name="mismatched_records", con=session.connection(), if_exists="append", index=False
    )  # Bit sloppy to dynamically define and write to table
    session.commit()
