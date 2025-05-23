# Placeholder for eligibility logic
import pandas as pd
from yaml import SafeLoader, load


def get_eligible_records(df: pd.DataFrame) -> pd.DataFrame:
    eligibility_rules = load(open("../config.yaml", "r"), SafeLoader)["eligibility_rules"]
    return df[
        (df["amount"] > eligibility_rules["min_amount"])
        & df["trade_type"].isin(eligibility_rules["valid_trade_types"])
        & df["region"].isin(eligibility_rules["valid_regions"])
        & (df["status"] == "Settled")
    ]
