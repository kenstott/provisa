// Copyright (c) 2026 Kenneth Stott
// Canary: 68a08375-722b-414b-9771-9998262986ca
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What the entity/fact form and the modeling wizards offer to pick: the user's own registered
// tables, and of a table's columns those that can be a key and those that can be a measure.

import type { ListedColumn, ListedTable } from "../../../types/admin";
import { DERIVED_SOURCE_ID } from "../../../types/admin";
import { isAssignableDomain } from "../../../lib/internalDomains";

// A key must support exact equality — the delta anti-join and snapshot dedup partition on it. Approximate
// numerics, JSON, and binary columns are excluded from the key picker (equality is unreliable/meaningless).
const _t = (c: ListedColumn) => (c.dataType ?? "").toLowerCase();
const _KEY_INELIGIBLE = /(float|double|real|json|jsonb|blob|bytea|array|geometry|geography)/;
const _NUMERIC = /(int|numeric|decimal|double|real|float|number|money)/;
export const isKeyable = (c: ListedColumn) => !_KEY_INELIGIBLE.test(_t(c));
export const isNumeric = (c: ListedColumn) => _NUMERIC.test(_t(c));

export const AGGS = ["sum", "avg", "min", "max", "count"] as const;

// User-defined tables only — exclude Provisa's own system sources/domains from the source picker.
// Mirrors provisa/core/config_loader.py::_SYSTEM_SOURCE_IDS; the domains are lib/internalDomains'.
const _SYSTEM_SOURCE_IDS = new Set(["provisa-admin", "provisa-otel", DERIVED_SOURCE_ID]);
export const isUserTable = (tbl: ListedTable) =>
  !_SYSTEM_SOURCE_IDS.has(tbl.sourceId) && isAssignableDomain(tbl.domainId);

export const colLabel = (c: ListedColumn) =>
  c.dataType ? `${c.columnName} (${c.dataType})` : c.columnName;
