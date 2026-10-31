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
// the demo -- no sign-in -- and goes to it. From cold both of the deployment's machines are off, so
// the page waits through each stage of a start (the server waking or being updated, the demo server
// starting, the demo's stores and template), saying which, for as long as a start takes: it asks the
// demo's status at a steady interval and opens it the moment it is ready. A failure is named as the
// server words it, with a way to try again where it passes with time.

import { Alert, Button, Center, Progress, Stack, Text, Title } from "@mantine/core";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";
import {
  fetchDemoStatus,
  openDemoSandbox,
  SandboxRefused,
  type OpenedSandbox,
} from "../../api/demoSandbox";
import { SandboxLoader } from "../../components/SandboxLoader";
import { orgOrigin } from "../../lib/authHost";
import { sandboxDestination } from "../../lib/sandboxSession";

/** What a refusal says the page is waiting on, and how the page words it: each passes by itself. */
const STAGES: Record<string, string> = {
  "front_door.coordinator_waking": "demoOpenPage.coordinatorWaking",
  "front_door.coordinator_updating": "demoOpenPage.coordinatorUpdating",
  "sandbox.starting": "demoOpenPage.serverStarting",
  "sandbox.demo_not_ready": "demoOpenPage.starting",
};
const NOT_READY = "demoOpenPage.starting";
/** The connection lost, or a gateway's answer: a server restarting, so waited through. */
const CONNECTING = "demoOpenPage.connecting";
const TIMED_OUT = "demo_open.timed_out";
/** Refusals that pass with time: the page offers to try again. Trying again after the demo's
 * stores or template failed asks for them again. */
const TRANSIENT = new Set([
  "sandbox.at_capacity",
  "sandbox.demo_stores_failed",
  "sandbox.demo_failed",
  "sandbox.failed_to_start",
  TIMED_OUT,
]);
/** How often a starting demo's status is asked. */
const STARTING_POLL_MS = 5000;
/** How many status answers pass before the demo is opened anyway: an open is what asks the node
 * for its stores, and its status does not say when that is needed. */
const OPEN_AGAIN_POLLS = 12;
/** What a cold start takes, as the front door states it (terraform/gcp-saas/front-door/proxy.py). */
const COLD_START_S = 300;
/** Past this an open still under way is called a failure. It is far beyond a cold start: the
 * server names one it knows of (sandbox.failed_to_start, after ten minutes), and a template built
 * for the first time takes longer than a start. */
const GIVE_UP_MIN = 30;
/** An open longer than this is no longer "a few seconds": the page says it is starting the demo. */
const SLOW_OPEN_S = 10;

function go(opened: OpenedSandbox): void {
  window.location.assign(sandboxDestination(opened.org_id, opened.token, orgOrigin(opened.org_id)));
}

function asRefusal(e: unknown): SandboxRefused {
  return e instanceof SandboxRefused ? e : new SandboxRefused(String(e), null);
}

/** The stage a failed call says the page is waiting on; null for a refusal that does not pass. */
function stageOf(e: unknown): string | null {
  if (e instanceof TypeError) return CONNECTING;
  const { code, status } = asRefusal(e);
  if (code !== null) return STAGES[code] ?? null;
  return status !== null && status >= 502 ? CONNECTING : null;
}

function clock(seconds: number): string {
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")}`;
}

const pause = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));

export function DemoOpenPage() {
  const { t } = useTranslation();
  const { name = "" } = useParams<{ name: string }>();
  const [refusal, setRefusal] = useState<SandboxRefused | null>(null);
  // What the page is waiting on, once a refusal or a slow open has said; null while it opens.
  const [stage, setStage] = useState<string | null>(null);
  const [seconds, setSeconds] = useState(0);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let current = true;
    const started = Date.now();
    const tick = window.setInterval(
      () => setSeconds(Math.floor((Date.now() - started) / 1000)),
      1000,
    );
    const fail = (refused: SandboxRefused) => {
      window.clearInterval(tick);
      setRefusal(refused);
    };
    (async () => {
      for (;;) {
        try {
          const opened = await openDemoSandbox(name);
          if (current) go(opened);
          return;
        } catch (e: unknown) {
          if (!current) return;
          const waitingOn = stageOf(e);
          if (waitingOn === null) {
            fail(asRefusal(e));
            return;
          }
          setStage(waitingOn);
        }
        for (let polls = 0; polls < OPEN_AGAIN_POLLS; polls += 1) {
          await pause(STARTING_POLL_MS);
          if (!current) return;
          if (Date.now() - started >= GIVE_UP_MIN * 60 * 1000) {
            fail(new SandboxRefused(t("demoOpenPage.gaveUp", { minutes: GIVE_UP_MIN }), TIMED_OUT));
            return;
          }
          try {
            const status = await fetchDemoStatus(name);
            if (!current) return;
            if (status.ready) break;
            setStage(NOT_READY);
          } catch (e: unknown) {
            if (!current) return;
            const waitingOn = stageOf(e);
            if (waitingOn === null) {
              fail(asRefusal(e));
              return;
            }
            setStage(waitingOn);
          }
        }
      }
    })();
    return () => {
      current = false;
      window.clearInterval(tick);
    };
  }, [name, attempt, t]);

  const slow = stage !== null || seconds >= SLOW_OPEN_S;
  const waiting = t(stage ?? (slow ? NOT_READY : "demoOpenPage.opening"));

  const retry = () => {
    setRefusal(null);
    setStage(null);
    setSeconds(0);
    setAttempt((n) => n + 1);
  };

  return (
    <Center mih="60vh" p="md">
      <Stack align="center" gap="md" w="100%" maw={520}>
        <Title order={2}>{t("demoOpenPage.title")}</Title>
        {refusal === null ? (
          <>
            <SandboxLoader size={88} label={waiting} testId="demo-open-loading" />
            <Text c="dimmed" ta="center" data-testid={`demo-open-${slow ? "starting" : "opening"}`}>
              {waiting}
            </Text>
            {slow && (
              <>
                {/* Held short of full: past the estimate the page is still only waiting. */}
                <Progress
                  w="100%"
                  value={Math.min((seconds * 100) / COLD_START_S, 95)}
                  aria-label={waiting}
                  data-testid="demo-open-progress"
                />
                <Text c="dimmed" size="sm" ta="center" data-testid="demo-open-elapsed">
                  {t("demoOpenPage.elapsed", { time: clock(seconds) })}
                </Text>
                <Text c="dimmed" size="sm" ta="center" data-testid="demo-open-cold-start">
                  {t("demoOpenPage.coldStart")}
                </Text>
              </>
            )}
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
