// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** True for an error thrown by an api function whose response was 403. */
export const isForbidden = (e: unknown): boolean => (e as { status?: number })?.status === 403;
