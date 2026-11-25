// Copyright (c) 2026 Kenneth Stott
// Canary: 88bb7bff-1c09-4c01-87cc-4685a72f084d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Microsoft Fabric Warehouse or Azure Synapse SQL source may sign in as an Azure AD service
// principal pinned to the source (provisa/executor/drivers/mssql_warehouse.py's
// MssqlWarehouseDriver._token reads tenant_id, client_id and client_secret from the hints); left
// out, the driver signs in with the ambient credential.

import { fieldsWhen, hintKey, hintKeys } from "./descriptor";
import { givenOf, heldOf, type AuthSeed } from "./descriptorPick";

/** The source types read by the warehouse driver. */
export const MSSQL_WAREHOUSES: ReadonlySet<string> = new Set(["fabric", "synapse"]);
/** The form's choice that signs in as a service principal. */
export const SERVICE_PRINCIPAL = "service_principal";

// REQ-2114: the keys are the descriptor's (provisa/api/admin/source_types.py).
const principalKeys = (type: string): string[] =>
  fieldsWhen(type, "federationHintsJson", "auth", SERVICE_PRINCIPAL).map((f) => f.name);

/** The hints a form saves: the service principal's, each one given, when it is chosen. */
export function mssqlWarehouseHints(
  type: string,
  authType: string,
  fields: Record<string, string>,
): Record<string, string> {
  return authType === SERVICE_PRINCIPAL ? givenOf(principalKeys(type), fields) : {};
}

/** A saved source's hints into the form: a tenant is a service-principal sign-in. */
export function mssqlWarehouseSeed(type: string, hints: Record<string, unknown>): AuthSeed {
  return {
    authType: hints[hintKey(type, "tenant_id")] ? SERVICE_PRINCIPAL : "none",
    authFields: heldOf(hintKeys(type), hints),
  };
}
