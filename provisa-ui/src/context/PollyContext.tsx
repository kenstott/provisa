// Copyright (c) 2026 Kenneth Stott
// Canary: 7d1c4a92-5e3b-4f08-a6d7-2b9e0c13f845
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useMemo, useState, type ReactNode } from "react";
import { fetchMcpChatStatus } from "../api/mcpChat";
import { PollyContext } from "./pollyState";

export function PollyProvider({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);
  // REQ-1804: checked before opening the panel — an unconfigured LLM shows an explanatory modal at
  // the button instead of an open panel that only reveals the problem after the user has already
  // typed and sent a message.
  const [checkingConfig, setCheckingConfig] = useState(false);
  const [unconfiguredReason, setUnconfiguredReason] = useState<string | null>(null);
  const openPolly = useCallback(async () => {
    setCheckingConfig(true);
    try {
      const status = await fetchMcpChatStatus();
      if (status.configured) {
        setOpen(true);
      } else {
        setUnconfiguredReason(status.reason || "no vendor or credential is configured");
      }
    } catch {
      // The status check itself failed (network/server error) — open anyway and let the chat's
      // own error handling surface it, rather than blocking the toggle on a second failure mode.
      setOpen(true);
    } finally {
      setCheckingConfig(false);
    }
  }, []);
  const closePolly = useCallback(() => setOpen(false), []);
  const value = useMemo(
    () => ({ open, checkingConfig, unconfiguredReason, setUnconfiguredReason, openPolly, closePolly }),
    [open, checkingConfig, unconfiguredReason, openPolly, closePolly],
  );
  return <PollyContext.Provider value={value}>{children}</PollyContext.Provider>;
}
