# Placeholder for eligibility tests
import pandas as pd
import pytest
from eligibility import get_eligible_records


def test_eligibility():
    passing_data = {
        "amount": [1200000, 1200000],
        "trade_type": ["New", "Amend"],
        "region": ["EU", "APAC"],
        "status": ["Settled", "Settled"],
    }
    passing_df = pd.DataFrame(passing_data)
    pd.testing.assert_frame_equal(passing_df, passing_df.pipe(get_eligible_records))
    with pytest.raises(AssertionError):
        failing_df = passing_df.assign(amount=5)
        pd.testing.assert_frame_equal(failing_df, failing_df.pipe(get_eligible_records))
    with pytest.raises(AssertionError):
        failing_df = passing_df.assign(trade_type="old")
        pd.testing.assert_frame_equal(failing_df, failing_df.pipe(get_eligible_records))
    with pytest.raises(AssertionError):
        failing_df = passing_df.assign(region="US")
        pd.testing.assert_frame_equal(failing_df, failing_df.pipe(get_eligible_records))
    with pytest.raises(AssertionError):
        failing_df = passing_df.assign(status="Pending")
        pd.testing.assert_frame_equal(failing_df, failing_df.pipe(get_eligible_records))
