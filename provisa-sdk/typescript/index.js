// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984: "@provisa/sdk" at run time. Every wrapper returns the function it is given: what it
// means is in its type (index.d.ts), which Provisa reads from the bundle's declarations.

export const SPELLING = 1;
export const provisa = Symbol("provisa");

const marker = (fn) => fn;

export const table = marker;
export const stream = marker;
export const command = marker;
export const query = marker;
export const columns = marker;
export const onStart = marker;
export const onStop = marker;
export const health = marker;
