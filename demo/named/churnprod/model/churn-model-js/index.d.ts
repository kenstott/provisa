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
