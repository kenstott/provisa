// Copyright (c) 2026 Kenneth Stott
// Canary: 25cc2bcf-be82-4587-a71b-a08ec466366b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2006: /demos/<name>, the live demos page's "Open live demo". It opens a visitor's sandbox of
// the demo -- no sign-in -- and goes to it. A demo whose stores are still starting (the first open
// asks for them) is waited for: the page asks the demo's status at a steady interval and opens it
// the moment it is ready. Any other refusal (an unknown demo, every sandbox in use) is shown as the
// server words it, with a way to try again where it passes with time.

import { Alert, Button, Center, Stack, Text, Title } from "@mantine/core";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";
import {
  fetchDemoStatus,
  openDemoSandbox,
  SandboxRefused,
  type OpenedSandbox,
} from "../../api/demoSandbox";
import { AssemblingCube } from "../../components/AssemblingCube";
import { orgOrigin } from "../../lib/authHost";
import { sandboxDestination } from "../../lib/sandboxSession";

/** A demo whose stores or template are still being prepared: waited for, not refused. */
const NOT_READY = "sandbox.demo_not_ready";
/** Refusals that pass with time: the page offers to try again. Trying again after the demo's
 * stores failed to start asks the node for them again. */
const TRANSIENT = new Set(["sandbox.at_capacity", "sandbox.demo_stores_failed"]);
/** How often a starting demo's status is asked. */
const STARTING_POLL_MS = 5000;

function go(opened: OpenedSandbox): void {
  window.location.assign(sandboxDestination(opened.org_id, opened.token, orgOrigin(opened.org_id)));
}

function asRefusal(e: unknown): SandboxRefused {
  return e instanceof SandboxRefused ? e : new SandboxRefused(String(e), null);
}

const pause = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));

export function DemoOpenPage() {
  const { t } = useTranslation();
  const { name = "" } = useParams<{ name: string }>();
  const [refusal, setRefusal] = useState<SandboxRefused | null>(null);
  // What the page is waiting on: the open itself, or the demo's stores starting first.
  const [phase, setPhase] = useState<"opening" | "starting">("opening");
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let current = true;
    (async () => {
      for (;;) {
        try {
          const opened = await openDemoSandbox(name);
          if (current) go(opened);
          return;
        } catch (e: unknown) {
          const refused = asRefusal(e);
          if (!current) return;
          if (refused.code !== NOT_READY) {
            setRefusal(refused);
            return;
          }
          setPhase("starting");
        }
        try {
          do {
            await pause(STARTING_POLL_MS);
            if (!current) return;
          } while (!(await fetchDemoStatus(name)).ready);
        } catch (e: unknown) {
          if (current) setRefusal(asRefusal(e));
          return;
        }
        if (!current) return;
      }
    })();
    return () => {
      current = false;
    };
  }, [name, attempt]);

  const waiting = phase === "starting" ? t("demoOpenPage.starting") : t("demoOpenPage.opening");

  const retry = () => {
    setRefusal(null);
    setPhase("opening");
    setAttempt((n) => n + 1);
  };

  return (
    <Center mih="60vh" p="md">
      <Stack align="center" gap="md" maw={520}>
        <Title order={2}>{t("demoOpenPage.title")}</Title>
        {refusal === null ? (
          <>
            <AssemblingCube size={88} label={waiting} testId="demo-open-loading" />
            <Text c="dimmed" ta="center" data-testid={`demo-open-${phase}`}>
              {waiting}
            </Text>
          </>
        ) : (
          <>
            <Alert
              color={TRANSIENT.has(refusal.code ?? "") ? "yellow" : "red"}
              data-testid="demo-open-refused"
            >
              {refusal.message}
            </Alert>
            {TRANSIENT.has(refusal.code ?? "") && (
              <Button onClick={retry} data-testid="demo-open-retry">
                {t("demoOpenPage.retry")}
              </Button>
            )}
          </>
        )}
      </Stack>
    </Center>
  );
}

export default DemoOpenPage;
