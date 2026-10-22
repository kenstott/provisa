// Copyright (c) 2026 Kenneth Stott
// Canary: e3a9dd79-3e62-4872-a9a5-61f01c2a39c5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export interface RiskRow {
  customer_id: string;
  score: number;
  reasons: string[];
  computed_at: Date;
}

/**
 * How likely a customer is to cancel, from the features the churn analysis trained on, and what
 * raises it most: the same model as the Python package, its coefficients in JavaScript.
 *
 * @table
 */
export declare function churn_risk_js(
  customer_id: string,
  plan: string,
  region: string,
  tenure_months: number,
  logins_per_month: number,
  tickets: number,
  as_of?: Date,
): RiskRow[];
