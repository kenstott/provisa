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
// far and a Stop that keeps what was found. Once it has ended: how it ended, the scope it ran with
// and what each kind found, then one table of every proposal it read, each ticked to accept, ticked
// not to be recommended again, or left undecided, and one Apply. New run sits beside Apply above
// the table, whose rows scroll beneath its pinned header while the result stays in view.

import { useTranslation } from "react-i18next";
import { Alert, Button, Progress, Stack, Text } from "@mantine/core";
import { isReading, type DiscoveryRun } from "../../api/admin";
import { FormFooter, FormLayout } from "../form/FormLayout";
import { discoveryResultLines } from "./discoveryResult";
import { ProposalsTable } from "./ProposalsTable";
import type { RunProposalsState } from "./useRunProposals";

/** What the dialog needs of the page to show and decide a run's proposals. */
export interface SuggestionActions {
  tableDomainById: Record<string, string>;
  tableNameById: Record<string, string>;
  /** Told after an Apply decided something: relationships and suggestions have changed. */
  onApplied: () => void;
}

export function DiscoverRunView({
  run,
  readError,
  onStop,
  onNewRun,
  suggestions,
  proposals,
}: {
  run: DiscoveryRun;
  readError: string;
  onStop: () => void;
  onNewRun: () => void;
  suggestions: SuggestionActions;
  /** What the ended run read, and what is ticked of it. */
  proposals: RunProposalsState;
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
        <Alert color="red" mb="md" style={{ flexShrink: 0 }} data-testid="discover-read-error">
          {readError}
        </Alert>
      )}
      {/* An Alert clips what overflows it, so in the dialog's column it would give up height to the
          table: the alerts keep theirs and only the table shrinks. */}
      {!running && (
        <Alert color="blue" mb="md" style={{ flexShrink: 0 }} data-testid="discover-result">
          {report &&
            discoveryResultLines(report, t).map((line) => (
              <Text key={line} fz="sm">
                {line}
              </Text>
            ))}
        </Alert>
      )}
      {!running && (
        <ProposalsTable
          state={proposals}
          tableDomainById={suggestions.tableDomainById}
          tableNameById={suggestions.tableNameById}
          newRun={
            <Button variant="default" onClick={onNewRun} data-testid="discover-new-run">
              {t("discoverDialog.newRun")}
            </Button>
          }
        />
      )}
      {running && (
        <FormLayout style={{ marginTop: "var(--mantine-spacing-md)" }}>
          <FormFooter
            action={
              <Button
                variant="default"
                onClick={onStop}
                disabled={run.stop_requested}
                data-testid="discover-stop"
              >
                {t("discoverDialog.stop")}
              </Button>
            }
          />
        </FormLayout>
      )}
    </>
  );
}
