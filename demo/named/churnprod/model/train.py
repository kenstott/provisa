# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fit the churn analysis's model once and ship it: churn_model/model.joblib for the Python package,
and the same fitted coefficients as churn-model-js/model.js for its Node twin.

Run in the package's own locked environment, so the pickle is read by the scikit-learn it pins:

    uv run --project churn_model python train.py <features.csv>

features.csv is the churn demo's features view (customer_id, region, plan, tenure_months,
logins_per_month, tickets, churned), read from its database. The fit is the notebook's: a
LogisticRegression on the numbers and the one-hot plan and region, trained on three quarters of
the customers (random_state 7, stratified)."""

import csv
import json
import sys
from pathlib import Path

import joblib
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

HERE = Path(__file__).parent
PLANS = ("basic", "pro", "team")
REGIONS = ("APAC", "EU", "NA")
FEATURES = (
    ["tenure_months", "logins_per_month", "tickets"]
    + [f"plan_{p}" for p in PLANS]
    + [f"region_{r}" for r in REGIONS]
)


def _row(r: dict) -> list[float]:
    return (
        [float(r["tenure_months"]), float(r["logins_per_month"]), float(r["tickets"])]
        + [1.0 if r["plan"] == p else 0.0 for p in PLANS]
        + [1.0 if r["region"] == g else 0.0 for g in REGIONS]
    )


def main(path: str) -> None:
    rows = list(csv.DictReader(open(path)))
    X = np.array([_row(r) for r in rows])
    y = np.array([int(r["churned"]) for r in rows])
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=7, stratify=y
    )
    model = LogisticRegression(max_iter=1000).fit(X_train, y_train)
    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    means = X_train.mean(axis=0).tolist()
    joblib.dump(
        {"model": model, "features": FEATURES, "means": means},
        HERE / "churn_model" / "model.joblib",
    )
    fitted = {
        "features": FEATURES,
        "coefficients": model.coef_[0].tolist(),
        "intercept": float(model.intercept_[0]),
        "means": means,
    }
    (HERE / "churn-model-js" / "model.js").write_text(
        "// The churn model's fitted coefficients, written by ../train.py from the same fit as\n"
        "// churn_model/model.joblib. Do not edit.\n"
        f"export const MODEL = {json.dumps(fitted, indent=2)};\n"
    )
    print(f"{len(rows)} customers; AUC on held-out customers {auc:.3f}")


if __name__ == "__main__":
    main(sys.argv[1])
