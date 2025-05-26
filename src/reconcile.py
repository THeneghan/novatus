"""Placeholder for reconciliation logic"""

from collections import namedtuple
from collections.abc import Iterable
from typing import Callable

import pandas as pd
from novatus_utils.sqlalchemy_utils import Session
from pandas import DataFrame

ColumnPairs = namedtuple("ColumnPairs", "ctr gtr")


PipeType = Callable[[DataFrame], DataFrame]

RECONCILABLE_CORE_FIELDS = {"amount", "currency", "instrument_type"}


def vals_different(df: pd.DataFrame, ctr_col_name, gtr_col_name):
    return True if df[ctr_col_name] != df[gtr_col_name] else False


def reconcile_vals(df: pd.Series, *args):
    collecting_list = []
    for val in args:
        if df[val + "to_be_reconciled"]:
            collecting_list.append(val)

    return ";".join(collecting_list)


def detail_reconciliation(comparison_cols: Iterable[str]) -> PipeType:
    def pipe(df: DataFrame) -> DataFrame:
        # TODO: Refactor this as mess

        pairs = []
        for col in comparison_cols:
            col_tuple = ColumnPairs(col, None)
            for df_col in df.columns:
                if col in df_col and df_col not in col_tuple:
                    col_tuple = ColumnPairs(col_tuple.ctr, df_col)
            pairs.append(col_tuple)
        for pair in pairs:
            df[pair.ctr + "to_be_reconciled"] = df.apply(vals_different, args=pair, axis=1)
        df["values_to_reconcile"] = df.apply(reconcile_vals, args=tuple(comparison_cols), axis=1)
        return df

    return pipe


with Session() as session:
    df = pd.read_sql("Select * from mismatched_records", con=session.connection())
    print(df.pipe(detail_reconciliation(RECONCILABLE_CORE_FIELDS)))
