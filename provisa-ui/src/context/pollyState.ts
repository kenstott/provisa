// Copyright (c) 2026 Kenneth Stott
// Canary: a809aa9a-aac1-4b82-9425-86273c6fbcb1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createContext, useContext } from "react";

// REQ-1945: Polly's open/closed state and the one handler that opens it, shared by its launcher
// button and the product tour so the tour opens it through the same path a user does.
export interface PollyState {
  open: boolean;
  /** True while the LLM-configured check (REQ-1804) behind the launcher is in flight. */
  checkingConfig: boolean;
  /** Why Polly cannot open (REQ-1804), or null. Shown by the launcher as an explanatory modal. */
  unconfiguredReason: string | null;
  setUnconfiguredReason: (reason: string | null) => void;
  /** The launcher's handler: checks configuration, then opens the panel. */
  openPolly: () => Promise<void>;
  closePolly: () => void;
}

export const PollyContext = createContext<PollyState | null>(null);

export function usePolly(): PollyState {
  const ctx = useContext(PollyContext);
  if (!ctx) throw new Error("usePolly must be used within PollyProvider");
  return ctx;
}
