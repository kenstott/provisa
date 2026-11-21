// Copyright (c) 2026 Kenneth Stott
// Canary: 05896c51-f2ed-462b-9aef-5fd4315a127b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { health } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export const status = health(function status(): number {
  return 1;
});
