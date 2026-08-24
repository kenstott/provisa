// Copyright (c) 2026 Kenneth Stott
// Canary: 2792d126-821f-48a0-8952-5d444a2bd2d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Group, Loader, Paper, Text } from "@mantine/core";
import { engineWaking, engineWakingSeconds, subscribeEngineWake } from "../lib/engineWake";

/**
 * REQ-1516: the app-wide "your query engine is starting" notice.
 *
 * A banner, not a modal, and the distinction is the point. {@link ./ServerUnavailableModal} blocks
 * because nothing can succeed while the server is down; here the server is answering, the query is
 * running, and every surface that does not touch the engine — settings, users, catalog metadata —
 * still works. Taking the screen away would stop work that a wait does not.
 *
 * It says how long, because ~2-4min of Autopilot node provision reads as a hang without a number,
 * and that is the whole reason this exists.
 */
export function EngineWakingBanner() {
  const [waking, setWaking] = useState(engineWaking);
  const [seconds, setSeconds] = useState(engineWakingSeconds);

  useEffect(() => subscribeEngineWake(setWaking), []);

  useEffect(() => {
    if (!waking) {
      setSeconds(null);
      return;
    }
    setSeconds(engineWakingSeconds());
    const timer = window.setInterval(() => setSeconds(engineWakingSeconds()), 1_000);
    return () => window.clearInterval(timer);
  }, [waking]);

  if (!waking) return null;

  return (
    <Paper
      withBorder
      shadow="md"
      p="sm"
      radius="md"
      pos="fixed"
      bottom={16}
      right={16}
      style={{ zIndex: 400, maxWidth: 380 }}
      data-testid="engine-waking-banner"
    >
      <Group gap="sm" wrap="nowrap" align="flex-start">
        <Loader size="sm" mt={2} />
        <div>
          <Text size="sm" fw={600}>
            Starting the query engine
          </Text>
          <Text size="xs" c="dimmed">
            It stopped while idle and takes 2–4 minutes to come back. Your query is still running
            and will return on its own
            {seconds === null ? "" : ` — ${formatElapsed(seconds)} so far`}.
          </Text>
        </div>
      </Group>
    </Paper>
  );
}

function formatElapsed(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  return `${Math.floor(seconds / 60)}m ${String(seconds % 60).padStart(2, "0")}s`;
}
