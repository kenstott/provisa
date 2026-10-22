// Copyright (c) 2026 Kenneth Stott
// Canary: 22aa757b-96c5-41b1-a9c5-6eb9db73b788
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { MaskEntry } from "../../lib/masks";

/** A table's Cache TTL as typed in its form, before it is saved. */
export interface CacheTtlEdit {
  value: string;
  dirty: boolean;
  saving: boolean;
}

/** Stages one attribute of the form's `i`th column. */
export type UpdateEditCol = (
  i: number,
  key: string,
  value: string | string[] | boolean | MaskEntry[],
) => void;
