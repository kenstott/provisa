// Copyright (c) 2026 Kenneth Stott
// Canary: 4a9d2e63-1c8b-4f75-b0e6-d27f5a3c9b14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: a discovery run in the dialog, as one collapsible block for its whole life (amended
// 2026-11-16). While it runs the block is held open on its progress: requests done of the total,
// the suggestions so far and the model, with Stop, which keeps what was found; Run in background
// closes the dialog and the run goes on on the server. Runs going at once are each a line in the
// block. Once it has ended the same block holds its summary (how it ended, the scope it ran with,
// what each kind found and what was left out and why), collapsed to its first line, which is the
// line the Relationships page shows. Then one table of every proposal it read, each ticked to
// accept, ticked not to be recommended again, or left undecided, and one Apply. New run sits beside
// Apply above the table, whose rows scroll beneath its pinned header while the block stays in view.

import { useTranslation } from "react-i18next";
import { Alert, Button, Group, Progress, Stack, Text } from "@mantine/core";
import { isReading, type DiscoveryRun } from "../../api/admin";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { FormFooter, FormLayout } from "../form/FormLayout";
import { discoveryResultLines } from "./discoveryResult";
import { ProposalsTable } from "./ProposalsTable";
import { runFraction, runLine, runStatus } from "./runLine";
import { waitingCount } from "./useDiscoveryRun";
import type { RunProposalsState } from "./useRunProposals";

/** What the dialog needs of the page to show and decide a run's proposals. */
export interface SuggestionActions {
  tableDomainById: Record<string, string>;
  tableNameById: Record<string, string>;
  /** Told after an Apply decided something, with how many proposals it accepted: relationships
   *  and suggestions have changed. */
  onApplied: (accepted: number) => void;
}

/** One run going, as a line of the block: how far it has got, its suggestions and model, Stop.
 *  ``titled``: the block's first line already says how far it has got and its suggestions. */
function GoingLine({
  run,
  titled,
  onStop,
}: {
  run: DiscoveryRun;
  titled: boolean;
  onStop: (runId: number) => void;
}) {
  const { t } = useTranslation();
  const said = [
    ...(titled ? [] : [t("discoverDialog.suggestionsSoFar", { count: run.suggestions })]),
    ...(run.model ? [t("discoverDialog.modelRan", { model: run.model })] : []),
  ];
  return (
    <Stack gap={4} data-testid={`discover-going-${run.id}`}>
      {!titled && (
        <Text fz="sm" fw={500}>
          {runStatus(run, t)}
        </Text>
      )}
      <Group gap="sm" wrap="nowrap">
        <Progress
          value={Math.round(runFraction(run) * 100)}
          animated
          aria-hidden
          style={{ flex: 1 }}
        />
        <Button
          variant="default"
          size="compact-sm"
          onClick={() => onStop(run.id)}
          disabled={run.stop_requested}
          data-testid={`discover-stop-${run.id}`}
        >
          {t("discoverDialog.stop")}
        </Button>
      </Group>
      {said.length > 0 && (
        <Text fz="sm" c="dimmed">
          {said.join(" · ")}
        </Text>
      )}
    </Stack>
  );
}

export function DiscoverRunView({
  run,
  going,
  readError,
  onStop,
  onBackground,
  onNewRun,
  suggestions,
  proposals,
  onDone,
}: {
  run: DiscoveryRun;
  /** Every run going, the newest first: each is a line of the block. */
  going: DiscoveryRun[];
  readError: string;
  onStop: (runId: number) => void;
  /** Close the dialog; the run goes on on the server. */
  onBackground: () => void;
  onNewRun: () => void;
  suggestions: SuggestionActions;
  /** What the ended run read, and what is ticked of it. */
  proposals: RunProposalsState;
  /** REQ-2262: the review after registration's Done, beside New run. */
  onDone?: () => void;
}) {
  const { t } = useTranslation();
  const running = run.status === "running";
  const report = run.report !== null && !isReading(run.report) ? run.report : null;
  // The shown run first, then the others going.
  const lines = [...(running ? [run] : []), ...going.filter((r) => r.id !== run.id)];
  const waiting = proposals.proposals === null ? null : waitingCount(proposals.proposals);

  return (
    <>
      {/* The block keeps its height in the dialog's column: only the proposals table shrinks. */}
      <div
        style={{ flexShrink: 0, marginBottom: "var(--mantine-spacing-md)" }}
        data-testid="discover-run-block"
      >
        <Collapsibles id="relationships.discover-run" testId="discover-progress">
          <CollapsibleItem
            value="run"
            // Collapsed to its first line once the run has ended; held open while it runs.
            defaultOpen={false}
            forceOpen={running}
            controlTestId="discover-run-toggle"
            title={
              <Text
                fw={500}
                c={run.status === "failed" ? "red" : undefined}
                data-testid="discover-run-line"
                aria-live="polite"
              >
                {runLine(run, running ? null : waiting, t)}
              </Text>
            }
          >
            <Stack gap="sm">
              {report && !running && (
                <Stack gap={2} data-testid="discover-result">
                  {discoveryResultLines(report, t).map((line) => (
                    <Text key={line} fz="sm">
                      {line}
                    </Text>
                  ))}
                </Stack>
              )}
              {lines.map((r) => (
                <GoingLine key={r.id} run={r} titled={r.id === run.id} onStop={onStop} />
              ))}
            </Stack>
          </CollapsibleItem>
        </Collapsibles>
      </div>
      {readError && (
        <Alert color="red" mb="md" style={{ flexShrink: 0 }} data-testid="discover-read-error">
          {readError}
        </Alert>
      )}
      {!running && (
        <ProposalsTable
          state={proposals}
          tableDomainById={suggestions.tableDomainById}
          tableNameById={suggestions.tableNameById}
          newRun={
            <>
              <Button variant="default" onClick={onNewRun} data-testid="discover-new-run">
                {t("discoverDialog.newRun")}
              </Button>
              {onDone && (
                <Button variant="default" onClick={onDone} data-testid="discover-done">
                  {t("discoverDialog.reviewDone")}
                </Button>
              )}
            </>
          }
        />
      )}
      {running && (
        <FormLayout>
          <FormFooter
            action={
              <Button variant="default" onClick={onBackground} data-testid="discover-background">
                {t("discoverDialog.runInBackground")}
              </Button>
            }
          />
        </FormLayout>
      )}
    </>
  );
}
