"""Report generation"""

import json

from novatus_utils.models import (
    CustomerTransactions,
    EligibleCustomerTransactions,
    GlobalTradeRepository,
    MismatchedRecords,
    MissingCustomerTransactionsGTR,
)
from sqlalchemy import select, text
from sqlalchemy.orm import session


def generate_report_json(sess: session, no_sample_rows: int = 5) -> str:
    """Generates a reporting json"""
    customer_transaction_count = sess.execute(
        text(f"""SELECT count(*) FROM {CustomerTransactions.__tablename__}""")
    ).scalar()
    global_trade_repository_count = sess.execute(
        text(f"""SELECT count(*) FROM {GlobalTradeRepository.__tablename__}""")
    ).scalar()
    eligible_customer_transactions_count = sess.execute(
        text(f"""SELECT count(*) FROM {EligibleCustomerTransactions.__table__}""")
    ).scalar()
    mismatched_records_count = sess.execute(text(f"""SELECT count(*) FROM {MismatchedRecords.__table__}""")).scalar()
    missing_customer_transaction_count = sess.execute(
        text(f"""SELECT count(*) FROM {MissingCustomerTransactionsGTR.__table__}""")
    ).scalar()
    differing_amount_count = sess.execute(
        text(f"""SELECT count(*) FROM {MismatchedRecords.__table__} where amount_difference is True""")
    ).scalar()
    differing_currency_count = sess.execute(
        text(f"""SELECT count(*) FROM {MismatchedRecords.__table__} where currency_difference is True""")
    ).scalar()
    differing_instrument_count = sess.execute(
        text(f"""SELECT count(*) FROM {MismatchedRecords.__table__} where instrument_type_difference is True""")
    ).scalar()
    sample_discrepancy_rows = sess.execute(select(MismatchedRecords).fetch(no_sample_rows)).all()
    sample_dict_list = [row[0].to_dict() for row in sample_discrepancy_rows]
    return json.dumps(
        {
            "customer_transaction_count": customer_transaction_count,
            "eligible_customer_transactions_count": eligible_customer_transactions_count,
            "mismatched_records_count": mismatched_records_count,
            "global_trade_repository_count": global_trade_repository_count,
            "missing_customer_transaction_count": missing_customer_transaction_count,
            "sample_discrepancy_rows": sample_dict_list,
            "reconciliation_error_counts": [
                {
                    "amount": differing_amount_count,
                    "currency": differing_currency_count,
                    "instrument_type": differing_instrument_count,
                }
            ],
        }
    )
