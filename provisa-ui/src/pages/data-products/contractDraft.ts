// Copyright (c) 2026 Kenneth Stott
// Canary: 99c5534a-cae4-46b9-b973-c034f7c539b7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a member's contract as the product's form edits it, and whether an edit changed it.

import type { DataContract } from "../../types/admin";
import type { DataContractInput } from "../../hooks/useDataProductMutations";

export const CONTRACT_STATUSES = ["proposed", "draft", "active", "deprecated", "retired"] as const;

/** The editable form of a stored contract. */
export function contractDraft(contract: DataContract): DataContractInput {
  return {
    version: contract.version,
    status: contract.status,
    description: contract.description ?? null,
    terms: {
      usage: contract.terms.usage ?? null,
      limitations: contract.terms.limitations ?? null,
      noticePeriod: contract.terms.noticePeriod ?? null,
    },
    sla: contract.sla.map((s) => ({
      property: s.property,
      value: s.value ?? null,
      unit: s.unit ?? null,
      description: s.description ?? null,
    })),
    columns: contract.columns.map((c) => ({
      name: c.name,
      type: c.type,
      description: c.description ?? null,
      required: c.required,
    })),
  };
}

/** Whether ``draft`` changes ``contract``: only a changed contract is saved. */
export function contractChanged(contract: DataContract, draft: DataContractInput): boolean {
  return JSON.stringify(contractDraft(contract)) !== JSON.stringify(draft);
}
