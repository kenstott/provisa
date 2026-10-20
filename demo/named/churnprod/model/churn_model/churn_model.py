# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The churn analysis's model, put to work: the scikit-learn logistic regression the analyst
trained on customers and their usage (demo/named/churn/notebook.ipynb), fitted once and shipped
here as model.joblib, scoring one customer from that customer's features."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path

import joblib
import numpy as np

_FITTED = joblib.load(Path(__file__).parent / "model.joblib")
_MODEL = _FITTED["model"]
FEATURES: list[str] = _FITTED["features"]
_MEANS = np.array(_FITTED["means"])
PLANS = ("basic", "pro", "team")
REGIONS = ("APAC", "EU", "NA")
_NUMBERS = 3  # tenure_months, logins_per_month, tickets: then the plan and region one-hot


@dataclass
class RiskRow:
    customer_id: str
    score: float
    reasons: list[str]
    computed_at: datetime


def features(plan: str, region: str, tenure_months: int, logins_per_month: float, tickets: int):
    """One customer's row as the model was trained on it: its numbers, then its plan and region
    one-hot, in the notebook's column order."""
    return np.array(
        [float(tenure_months), float(logins_per_month), float(tickets)]
        + [1.0 if plan == p else 0.0 for p in PLANS]
        + [1.0 if region == r else 0.0 for r in REGIONS]
    )


_WORDS = {
    "tenure_months": "a short time as a customer",
    "logins_per_month": "few sign-ins",
    "tickets": "many support tickets",
    "plan_basic": "on the basic plan",
    "plan_pro": "on the pro plan",
    "plan_team": "on the team plan",
    "region_APAC": "billed in APAC",
    "region_EU": "billed in the EU",
    "region_NA": "billed in North America",
}


def reasons_for(row) -> list[str]:
    """What raises this customer's risk most, in words: its numbers, plan and region whose part
    of the score is above the average customer's, largest first, at most three. A plan or region
    counts only when it is the customer's own."""
    parts = _MODEL.coef_[0] * (row - _MEANS)
    held = [i for i in np.argsort(-parts) if parts[i] > 0 and (i < _NUMBERS or row[i] == 1.0)]
    return [_WORDS[FEATURES[i]] for i in held[:3]]


def churn_risk(
    customer_id: str,
    plan: str,
    region: str,
    tenure_months: int,
    logins_per_month: float,
    tickets: int,
    as_of: date | None = None,
) -> list[RiskRow]:
    """How likely a customer is to cancel, from the features the churn analysis trained on, and
    what raises it most.

    @table
    """
    row = features(plan, region, tenure_months, logins_per_month, tickets)
    score = float(_MODEL.predict_proba(row.reshape(1, -1))[0, 1])
    stamped = datetime.combine(as_of, time(), UTC) if as_of else datetime.now(UTC)
    return [RiskRow(str(customer_id), round(score, 4), reasons_for(row), stamped)]
