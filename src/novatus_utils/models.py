"""SQLalchemy models"""

from sqlalchemy import Column, DateTime, Integer, String, case, or_, select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy_mixins.serialize import SerializeMixin
from sqlalchemy_utils import create_view


class Base(DeclarativeBase):
    """SQLAlchemy base model"""

    pass


class CustomerTransactions(Base):
    """Table to store customer transactions"""

    __tablename__ = "customer_transactions"
    transaction_id = Column(String, primary_key=True)
    timestamp = Column(DateTime)
    amount = Column(Integer)
    currency = Column(String)
    instrument_type = Column(String)
    region = Column(String)
    trade_type = Column(String)
    status = Column(String)


class GlobalTradeRepository(Base):
    """Table to store GTR data"""

    __tablename__ = "gtr_report"
    reported_id = Column(String, primary_key=True)
    timestamp = Column(DateTime)
    amount = Column(Integer)
    currency = Column(String)
    instrument_type = Column(String)
    region = Column(String)
    trade_type = Column(String)
    reported_status = Column(String)


class EligibleCustomerTransactions(Base):
    """View that displays transactions eligible for reconciliation"""

    __table__ = create_view(
        name="eligible_customer_transactions",
        selectable=select(
            CustomerTransactions.transaction_id,
            CustomerTransactions.timestamp,
            CustomerTransactions.amount,
            CustomerTransactions.currency,
            CustomerTransactions.instrument_type,
            CustomerTransactions.region,
            CustomerTransactions.trade_type,
            CustomerTransactions.status,
        )  # TODO: Not entirely sure whether Customer transaction ID needs to be present in GTR to be eligible if to
        # TODO: uncomment block below .select_from( CustomerTransactions.__table__.join( GlobalTradeRepository,
        # GlobalTradeRepository.reported_id == CustomerTransactions.transaction_id, isouter=True, ) )
        .where(CustomerTransactions.amount > 1000000)  # Move these to constants
        .where(CustomerTransactions.status == "Settled")
        .where(CustomerTransactions.trade_type.in_(["New", "Amend"]))
        .where(CustomerTransactions.region.in_(["EU", "APAC"])),
        metadata=Base.metadata,
    )


class MissingCustomerTransactionsGTR(Base):
    """View for customer transactions that are missing from GTR"""

    __table__ = create_view(
        name="missing_customer_transactions",
        selectable=select(
            CustomerTransactions.transaction_id,
            CustomerTransactions.timestamp,
            CustomerTransactions.amount,
            CustomerTransactions.currency,
            CustomerTransactions.instrument_type,
            CustomerTransactions.region,
            CustomerTransactions.trade_type,
            CustomerTransactions.status,
            GlobalTradeRepository.reported_id,
        )
        .select_from(
            CustomerTransactions.__table__.join(
                GlobalTradeRepository,
                GlobalTradeRepository.reported_id == CustomerTransactions.transaction_id,
                isouter=True,
            )
        )
        .where(CustomerTransactions.amount > 1000000)  # Move these to constants
        .where(CustomerTransactions.status == "Settled")
        .where(CustomerTransactions.trade_type.in_(["New", "Amend"]))
        .where(CustomerTransactions.region.in_(["EU", "APAC"]))
        .filter(GlobalTradeRepository.reported_id.is_(None)),
        metadata=Base.metadata,
    )


class MismatchedRecords(Base, SerializeMixin):
    """View that shows logged mismatched records with to_dict functionality mixed in"""

    __table__ = create_view(
        name="mismatched_records",
        selectable=select(
            CustomerTransactions.transaction_id,
            CustomerTransactions.amount.label("amount_ctr"),
            GlobalTradeRepository.amount.label("amount_gtr"),
            CustomerTransactions.currency.label("currency_ctr"),
            GlobalTradeRepository.currency.label("currency_gtr"),
            CustomerTransactions.instrument_type.label("instrument_type_ctr"),
            GlobalTradeRepository.instrument_type.label("instrument_type_gtr"),
            GlobalTradeRepository.reported_id,
            case(
                (GlobalTradeRepository.amount != CustomerTransactions.amount, True),
                else_=False,
            ).label("amount_difference"),
            case(
                (GlobalTradeRepository.currency != CustomerTransactions.currency, True),
                else_=False,
            ).label("currency_difference"),
            case(
                (GlobalTradeRepository.currency != CustomerTransactions.currency, True),
                else_=False,
            ).label("instrument_type_difference"),
        )
        .select_from(
            CustomerTransactions.__table__.join(
                GlobalTradeRepository,
                GlobalTradeRepository.reported_id == CustomerTransactions.transaction_id,
                isouter=True,
            )
        )
        .where(CustomerTransactions.amount > 1000000)  # Move these to constants
        .where(CustomerTransactions.status == "Settled")
        .where(CustomerTransactions.trade_type.in_(["New", "Amend"]))
        .where(CustomerTransactions.region.in_(["EU", "APAC"]))
        .filter(
            or_(
                CustomerTransactions.amount != GlobalTradeRepository.amount,
                CustomerTransactions.currency != GlobalTradeRepository.currency,
                CustomerTransactions.instrument_type != GlobalTradeRepository.instrument_type,
            )
        ),
        metadata=Base.metadata,
    )
