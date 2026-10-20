// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The churn analysis's model in JavaScript: the logistic regression fitted in Python
// (../churn_model/model.joblib), its coefficients in model.js, scoring one customer as the Python
// package does.

import { MODEL } from "./model.js";

const PLANS = ["basic", "pro", "team"];
const REGIONS = ["APAC", "EU", "NA"];
const NUMBERS = 3; // tenure_months, logins_per_month, tickets: then the plan and region one-hot
const WORDS = {
  tenure_months: "a short time as a customer",
  logins_per_month: "few sign-ins",
  tickets: "many support tickets",
  plan_basic: "on the basic plan",
  plan_pro: "on the pro plan",
  plan_team: "on the team plan",
  region_APAC: "billed in APAC",
  region_EU: "billed in the EU",
  region_NA: "billed in North America",
};

function features(plan, region, tenureMonths, loginsPerMonth, tickets) {
  return [
    Number(tenureMonths),
    Number(loginsPerMonth),
    Number(tickets),
    ...PLANS.map((p) => (plan === p ? 1 : 0)),
    ...REGIONS.map((r) => (region === r ? 1 : 0)),
  ];
}

export function churn_risk_js(
  customer_id,
  plan,
  region,
  tenure_months,
  logins_per_month,
  tickets,
  as_of,
) {
  const row = features(plan, region, tenure_months, logins_per_month, tickets);
  const z = row.reduce((sum, x, i) => sum + x * MODEL.coefficients[i], MODEL.intercept);
  const score = 1 / (1 + Math.exp(-z));
  const parts = row.map((x, i) => MODEL.coefficients[i] * (x - MODEL.means[i]));
  // A plan or region counts only when it is the customer's own.
  const reasons = parts
    .map((part, i) => [part, i])
    .filter(([part, i]) => part > 0 && (i < NUMBERS || row[i] === 1))
    .sort((a, b) => b[0] - a[0])
    .slice(0, 3)
    .map(([, i]) => WORDS[MODEL.features[i]]);
  const computed_at = as_of ? new Date(`${new Date(as_of).toISOString().slice(0, 10)}T00:00:00Z`) : new Date();
  return [
    { customer_id: String(customer_id), score: Math.round(score * 10000) / 10000, reasons, computed_at },
  ];
}
