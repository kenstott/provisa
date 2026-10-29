// Copyright (c) 2026 Kenneth Stott
// Canary: a41f8c07-3e5b-4d92-b6a0-5c18d7e2f390
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: the strokes of the ground symbol, one path each. graph3d.css carries the same set for a
// graph label (GroundIcon.test.tsx holds the two together).

export const GROUND_PATHS = ["M12 3v8", "M5 11h14", "M8 15h8", "M11 19h2"] as const;

export type GroundState = "grounded" | "grounded_pending";
