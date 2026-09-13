// Copyright (c) 2026 Kenneth Stott
// Canary: 3a7f9c21-8b4e-4d12-a6f1-9e2c5b8d0f34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export interface ColumnForm {
  name: string;
  visibleTo: string[];
  writableBy: string[];
  unmaskedTo: string;
  maskType: string;
  maskPattern: string;
  maskReplace: string;
  maskValue: string;
  maskPrecision: string;
  alias: string;
  description: string;
  selected: boolean;
  nativeFilterType: string | null;
  dataType: string;
  isPrimaryKey: boolean;
  scope: string;
  // REQ-1739: dot-notation JSON extraction path (e.g. "payload.order_id") for an `ingest` push
  // source's per-column mapping — provisa/ingest/router.py's `_extract_row`. Only meaningful when
  // the owning table's source type is `ingest`.
  path?: string | null;
}
