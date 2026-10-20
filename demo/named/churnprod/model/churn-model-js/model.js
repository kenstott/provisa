// Copyright (c) 2026 Kenneth Stott
// Canary: 5ae8c327-5ade-481e-9078-253a7f6ee59d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The churn model's fitted coefficients, written by ../train.py from the same fit as
// churn_model/model.joblib. Do not edit.
export const MODEL = {
  "features": [
    "tenure_months",
    "logins_per_month",
    "tickets",
    "plan_basic",
    "plan_pro",
    "plan_team",
    "region_APAC",
    "region_EU",
    "region_NA"
  ],
  "coefficients": [
    -0.02776466289925552,
    -0.15047772155900058,
    0.21740051806774288,
    0.4716565448452646,
    -0.1348260277614974,
    -0.5239613666023688,
    -0.08051469340203245,
    -0.10345403453621822,
    -0.0031621215803518243
  ],
  "intercept": -0.1955267817134429,
  "means": [
    23.899555555555555,
    14.2385925925926,
    5.871111111111111,
    0.5146666666666667,
    0.248,
    0.23733333333333334,
    0.32,
    0.33244444444444443,
    0.34755555555555556
  ]
};
