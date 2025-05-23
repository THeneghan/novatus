import datetime
import logging
from typing import Union
import uvicorn

from fastapi import FastAPI
from sqlalchemy import create_engine
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import List

from generate_fake_tables import POSTGRES_USER, POSTGRES_PASSWORD
from novatus_utils.models import GlobalTradeRepository, CustomerTransactions
import pandas as pd

logger = logging.getLogger(__name__)
engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@0.0.0.0/postgres", echo=False)

class CustomerTransactionModel(BaseModel):
    transaction_id: List[str]
    timestamp: List[datetime.datetime]
    amount: List[int]
    currency: List[str]
    instrument_type: List[str]
    region: List[str]
    trade_type: List[str]
    status: List[str]

app = FastAPI()


@app.post("/customer_transaction/")
def read_customer_transaction_record(transaction: CustomerTransactionModel):
    df=pd.DataFrame.from_dict(transaction.model_dump())

    df.to_sql(
            name=CustomerTransactions.__tablename__, con=engine, if_exists="append", index=False
        )
    return {"Inserted"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)