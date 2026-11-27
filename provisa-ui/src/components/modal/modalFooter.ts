// Copyright (c) 2026 Kenneth Stott
// Canary: 6b1f2d84-3e7a-4c95-a0d8-52c9e1f7b346
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 10: where a dialog's buttons go, the fixed bar at the bottom of the ModalLayout
// they are in. `undefined` means "not in a dialog" (render the buttons in place); `null` means the
// dialog's bar is not mounted yet.

import { createContext } from "react";

export const ModalFooterContext = createContext<HTMLElement | null | undefined>(undefined);
