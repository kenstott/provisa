// Copyright (c) 2026 Kenneth Stott
// Canary: 4a9d2e63-1c8b-4f75-b0e6-d27f5a3c9b14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: a discovery run in the dialog. While it runs: how far it has got, the suggestions so
// far and a Stop that keeps what was found. Once it has ended: how it ended, what each kind found,
// the model that ran, the run's suggestions with Accept / Reject, Accept all and Reject all, and
// under them what the run read and left out, with why.

import { useTranslation } from "react-i18next";
import { Alert, Button, Progress, Stack, Text } from "@mantine/core";
import { isReading, type DiscoveryRun } from "../../api/admin";
import { FormFooter, FormLayout } from "../form/FormLayout";
import { CandidatesBulkFailures } from "./CandidatesBulkFailures";
import { CandidatesTable } from "./CandidatesTable";
import { discoveryResultLines } from "./discoveryResult";
import { LeftOutTable } from "./LeftOutTable";
import type { Candidate } from "./relationship-types";
import type { BulkFailures } from "./useCandidatesAtOnce";
import type { LeftOutState } from "./useLeftOut";

export interface SuggestionActions {
  candidates: Candidate[];
  tableDomainById: Record<string, string>;
  tableNameById: Record<string, string>;
  onAccept: (id: number, name: string) => void;
  onReject: (id: number) => void;
  onAcceptAll: (candidates: { id: number; name: string }[]) => Promise<void>;
  onRejectAll: (ids: number[]) => Promise<void>;
  /** Accept a proposal the run left out; a refusal is thrown with the server's words. */
  onAcceptLeftOut: (id: number, name: string) => Promise<void>;
  failures: BulkFailures | null;
  message: string;
  error: string;
}

export function DiscoverRunView({
  run,
  readError,
  onStop,
  onNewRun,
  suggestions,
  leftOut,
}: {
  run: DiscoveryRun;
  readError: string;
  onStop: () => void;
  onNewRun: () => void;
  suggestions: SuggestionActions;
  /** What the ended run left out. */
  leftOut: LeftOutState;
}) {
  const { t } = useTranslation();
  const running = run.status === "running";
  const reading = isReading(run.report) ? run.report : null;
  const report = run.report !== null && !isReading(run.report) ? run.report : null;

  const status = (() => {
    if (run.status === "done") return t("discoverDialog.done");
    if (run.status === "stopped") return t("discoverDialog.stopped");
    if (run.status === "failed") return t("discoverDialog.failed", { error: run.error ?? "" });
    if (run.stop_requested) return t("discoverDialog.stopping");
    if (reading) {
      return t("discoverDialog.reading", {
        done: reading.tables_read,
        total: reading.tables_total,
      });
    }
    if (run.requests_total > 0) {
      return t("discoverDialog.requests", { done: run.requests_done, total: run.requests_total });
    }
    return t("discoverDialog.starting");
  })();
  const fraction = reading
    ? reading.tables_total > 0
      ? reading.tables_read / reading.tables_total
      : 0
    : run.requests_total > 0
      ? run.requests_done / run.requests_total
      : 0;

  return (
    <>
      <Stack gap="xs" mb="md" data-testid="discover-progress" aria-live="polite">
        <Text fw={500} c={run.status === "failed" ? "red" : undefined}>
          {status}
        </Text>
        {running && <Progress value={Math.round(fraction * 100)} animated aria-hidden />}
        {running && (
          <Text fz="sm" c="dimmed">
            {t("discoverDialog.suggestionsSoFar", { count: run.suggestions })}
            {run.model ? ` · ${t("discoverDialog.modelRan", { model: run.model })}` : ""}
          </Text>
        )}
      </Stack>
      {readError && (
        <Alert color="red" mb="md" data-testid="discover-read-error">
          {readError}
        </Alert>
      )}
      {!running && (
        <Alert color="blue" mb="md" data-testid="discover-result">
          {run.model && <Text fz="sm">{t("discoverDialog.modelRan", { model: run.model })}</Text>}
          {report &&
            discoveryResultLines(report, t).map((line) => (
              <Text key={line} fz="sm">
                {line}
              </Text>
            ))}
        </Alert>
      )}
      {suggestions.error && (
        <Alert color="red" mb="md" data-testid="discover-suggestions-error">
          {suggestions.error}
        </Alert>
      )}
      <CandidatesBulkFailures failures={suggestions.failures} />
      {suggestions.message && (
        <Text c="var(--approve)" mb="md" fz="0.875rem" data-testid="discover-suggestions-msg">
          {suggestions.message}
        </Text>
      )}
      {suggestions.candidates.length > 0 ? (
        <CandidatesTable
          candidates={suggestions.candidates}
          tableDomainById={suggestions.tableDomainById}
          tableNameById={suggestions.tableNameById}
          onAccept={suggestions.onAccept}
          onReject={suggestions.onReject}
          onAcceptAll={suggestions.onAcceptAll}
          onRejectAll={suggestions.onRejectAll}
        />
      ) : (
        !running && (
          <Text fz="sm" c="dimmed" data-testid="discover-no-suggestions">
            {t("discoverDialog.noSuggestions")}
          </Text>
        )
      )}
      {leftOut.leftOut && (
        <LeftOutTable
          proposals={leftOut.leftOut.left_out}
          threshold={leftOut.leftOut.threshold}
          tableDomainById={suggestions.tableDomainById}
          tableNameById={suggestions.tableNameById}
          onAccept={(id, name) => void leftOut.accept(id, name)}
          error={leftOut.error}
        />
      )}
      <FormLayout style={{ marginTop: "var(--mantine-spacing-md)" }}>
        <FormFooter
          action={
            running ? (
              <Button
                variant="default"
                onClick={onStop}
                disabled={run.stop_requested}
                data-testid="discover-stop"
              >
                {t("discoverDialog.stop")}
              </Button>
            ) : (
              <Button onClick={onNewRun} data-testid="discover-new-run">
                {t("discoverDialog.newRun")}
              </Button>
            )
          }
        />
      </FormLayout>
    </>
  );
}
