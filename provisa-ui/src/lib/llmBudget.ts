// Copyright (c) 2026 Kenneth Stott
// Canary: 2f67618a-6655-4598-9647-d6f61e6f42ec
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1174: a role's LLM budget as the role's form holds it, and as its rate_limit input carries it.

import { LLM_WINDOWS, type LlmLimit, type LlmWindow, type RoleRateLimit } from "../types/auth";

/** One limit as the form holds it: "" = not set. */
export interface LlmLimitForm {
  amount: number | "";
  unit: LlmLimit["unit"];
}

export interface LlmBudgetForm {
  cap: LlmLimitForm;
  windows: Record<LlmWindow, LlmLimitForm>;
}

const limitForm = (given: LlmLimit | null | undefined): LlmLimitForm =>
  given ? { amount: given.amount, unit: given.unit } : { amount: "", unit: "tokens" };

/** The form's values for a role's budget; a new role sets none. */
export function llmBudgetForm(rateLimit: RoleRateLimit | null | undefined): LlmBudgetForm {
  return {
    cap: limitForm(rateLimit?.llmCallCap),
    windows: Object.fromEntries(
      LLM_WINDOWS.map((w) => [w, limitForm(rateLimit?.llmWindows?.[w])]),
    ) as Record<LlmWindow, LlmLimitForm>,
  };
}

const limitInput = (form: LlmLimitForm): LlmLimit | null =>
  form.amount === "" ? null : { amount: Number(form.amount), unit: form.unit };

/** The budget as the role's rate_limit input carries it: an unset limit is null. */
export function llmBudgetInput(form: LlmBudgetForm) {
  return {
    llmCallCap: limitInput(form.cap),
    llmWindows: Object.fromEntries(
      LLM_WINDOWS.map((w) => [w, limitInput(form.windows[w])]),
    ) as Record<LlmWindow, LlmLimit | null>,
  };
}
