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
// with one going, or the one ended while it was watched, it shows that run. Closing the dialog
// leaves a run going: the run is the server's, and opening the dialog again shows it.
// The form starts from the defaults when the dialog opens; only "New run" at the end of a run
// opens it with that run's fields.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Modal } from "@mantine/core";
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
}

export function DiscoverDialog({
  opened,
  onClose,
  discovery,
  tables,
  defaultDomains,
  suggestions,
}: DiscoverDialogProps) {
  const { t } = useTranslation();
  // An ended run the dialog was closed on is left: it opens again on the form, while the page's
  // banner keeps the run's result. A run still going, or one that ended while the dialog was
  // closed, is shown.
  const [left, setLeft] = useState<number | null>(null);
  const run = discovery.run !== null && discovery.run.id !== left ? discovery.run : null;
  const proposals = useRunProposals(run, suggestions.onApplied);
  // The run "New run" left, whose fields the form starts from; null: the defaults. "Reset to
  // defaults" and closing the dialog clear it.
  const [initial, setInitial] = useState<LastRun | null>(null);
  const close = () => {
    setInitial(null);
    if (run !== null && run.status !== "running") setLeft(run.id);
    onClose();
  };
  return (
    <Modal
      opened={opened}
      onClose={close}
      title={t("discoverDialog.title")}
      // A run lists its proposals in one table, whose columns need the room: the window's width
      // less the page gutter the lists keep.
      size={run ? "100%" : "lg"}
      xOffset={run ? "var(--page-gutter)" : undefined}
      centered
      data-testid="discover-dialog"
    >
      {/* The scope mounts each time it is shown, and again when the run it starts from is known:
          which models can run is read again, so a key set meanwhile counts. */}
      {opened &&
        (run ? (
          <DiscoverRunView
            run={run}
            readError={discovery.error}
            onStop={() => void discovery.stop()}
            onNewRun={() => {
              setInitial({ scope: run.scope, startedAt: run.started_at });
              discovery.dismiss();
            }}
            suggestions={suggestions}
            proposals={proposals}
          />
        ) : (
          <DiscoverScopeForm
            key={initial === null ? "defaults" : JSON.stringify(initial)}
            tables={tables}
            defaultDomains={defaultDomains}
            initial={initial}
            onReset={() => setInitial(null)}
            onCancel={close}
            onRun={async (scope: DiscoveryScope) => {
              await discovery.start(scope);
            }}
          />
        ))}
    </Modal>
  );
}
