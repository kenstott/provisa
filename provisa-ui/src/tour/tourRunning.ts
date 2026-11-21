// Copyright (c) 2026 Kenneth Stott
// Canary: f3a91c47-2e6b-4d08-b5c3-7c1e8a4d9f26
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createContext } from "react";

/** Whether a tour is running: a page drawn under one can show what the tour explains. Apart from
 * useTour.tsx, so a page need not import the tour's runner, which imports the pages. */
export const TourRunningContext = createContext(false);
