// Copyright (c) 2026 Kenneth Stott
// Canary: 13bd368c-dffc-4880-9ff2-8c3983c8b33c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Attraction slider's range (Attraction.tsx): how close a force-laid-out 3D graph draws its
// nodes, loose to tight; 1 is the usual spacing. The value is one of the graph's remembered view
// settings (graphSettings.ts); a graph with a fixed layout (the terrain, the strata) has none.

export const ATTRACTION_MIN = 0.5;
export const ATTRACTION_MAX = 2;
