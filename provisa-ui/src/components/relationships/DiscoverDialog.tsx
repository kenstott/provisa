// Copyright (c) 2026 Kenneth Stott
// Canary: 3e9a7c51-0b2d-4f86-a1c4-7d5e2b9f6a03
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: discovery asks before it runs. With no run to show, the dialog sets up a run's scope;
// with one going, the one ended while it was watched, or the one the page's line opened, it shows
// that run. Closing the dialog, or Run in background, leaves a run going: the run is the
// server's, and opening the dialog again shows it.
// The form starts from the defaults when the dialog opens; only "New run" at the end of a run
// opens it with that run's fields.

import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Modal } from "@mantine/core";
import type { DiscoveryScope } from "../../api/admin";
import { DiscoverRunView, type SuggestionActions } from "./DiscoverRunView";
import { DiscoverScopeForm, type ScopeTable } from "./DiscoverScopeForm";
import type { LastRun } from "./restoreScope";
import type { DiscoveryRunState } from "./useDiscoveryRun";
import { useRunProposals } from "./useRunProposals";

interface DiscoverDialogProps {
  opened: boolean;
  onClose: () => void;
  discovery: DiscoveryRunState;
  tables: ScopeTable[];
  /** The domains ticked in the Domains picker: the source domains of the defaults. */
  defaultDomains: string[];
  suggestions: SuggestionActions;
  /** REQ-2262: the run the form starts from when the dialog opens; absent: the defaults. */
  start?: LastRun;
  /** REQ-2262: the review after registration. Closing the dialog, or its Done, ends the review
   *  with how many relationships its Applies created. */
  onDone?: (created: number) => void;
  /** REQ-2277: the run the page's link names could not be read; absent where the dialog has no
   *  link (the review after registration). */
  linkError?: string;
}

export function DiscoverDialog({
  opened,
  onClose,
  discovery,
  tables,
  defaultDomains,
  suggestions,
  start,
  onDone,
  linkError,
}: DiscoverDialogProps) {
  const { t } = useTranslation();
  const run = discovery.run;
  // REQ-2262: the relationships the dialog's Applies created, which the review ends with.
  const [created, setCreated] = useState(0);
  const { onApplied } = suggestions;
  const applied = useCallback(
    (accepted: number) => {
      setCreated((n) => n + accepted);
      onApplied(accepted);
    },
    [onApplied],
  );
  const proposals = useRunProposals(run, applied);
  // The run "New run" left, whose fields the form starts from; null: the defaults. "Reset to
  // defaults" and closing the dialog clear it.
  const [initial, setInitial] = useState<LastRun | null>(start ?? null);
  // "Reset to defaults" was pressed: Key value overlap takes its defaults, not the person's own.
  const [reset, setReset] = useState(false);
  const close = () => {
    setInitial(null);
    setReset(false);
    // An ended run the dialog is closed on is left: it opens again on the form, while the page's
    // line keeps the run, and its Open shows it again.
    if (run !== null && run.status !== "running") discovery.dismiss();
    onClose();
    onDone?.(created);
  };
  // Only the list of an ended run's proposals takes the window's width: setting up, a run going,
  // and an ended run with no proposal to list take the large dialog size multi-field dialogs use.
  const results =
    run !== null &&
    run.status !== "running" &&
    proposals.proposals !== null &&
    proposals.proposals.length > 0;
  return (
    <Modal
      opened={opened}
      onClose={close}
      title={t("discoverDialog.title")}
      // A run's proposals are listed in one table, whose columns need the room: the window's
      // width less the page gutter the lists keep.
      size={results ? "100%" : "xl"}
      xOffset={results ? "var(--page-gutter)" : undefined}
      // A run's proposals never scroll as a whole: the dialog is at most the window's height, its
      // title and the run's result and actions stay in view, and only the proposals table, which
      // takes the height left, scrolls beneath its pinned header row.
      styles={
        results
          ? {
              content: { display: "flex", flexDirection: "column" },
              body: {
                flex: "1 1 auto",
                minHeight: 0,
                display: "flex",
                flexDirection: "column",
              },
            }
          : undefined
      }
      centered
      data-testid="discover-dialog"
    >
      {/* The scope mounts each time it is shown, and again when the run it starts from is known:
          which models can run is read again, so a key set meanwhile counts. */}
      {opened &&
        (run ? (
          <DiscoverRunView
            run={run}
            going={discovery.going}
            readError={discovery.error}
            onStop={(runId) => void discovery.stop(runId)}
            onBackground={close}
            onNewRun={() => {
              setReset(false);
              setInitial({ scope: run.scope, startedAt: run.started_at });
              discovery.dismiss();
            }}
            suggestions={suggestions}
            proposals={proposals}
            onDone={onDone ? close : undefined}
          />
        ) : (
          <>
            {/* REQ-2277: the run the link names could not be read. */}
            {linkError && (
              <Alert color="red" mb="md" data-testid="discover-link-error">
                {linkError}
              </Alert>
            )}
            <DiscoverScopeForm
              key={initial === null ? (reset ? "reset" : "defaults") : JSON.stringify(initial)}
              tables={tables}
              defaultDomains={defaultDomains}
              initial={initial}
              fromDefaults={reset}
              onReset={() => {
                setInitial(null);
                setReset(true);
              }}
              onCancel={close}
              onRun={async (scope: DiscoveryScope, domains: ReadonlySet<string> | null) => {
                await discovery.start(scope, domains);
              }}
            />
          </>
        ))}
    </Modal>
  );
}
