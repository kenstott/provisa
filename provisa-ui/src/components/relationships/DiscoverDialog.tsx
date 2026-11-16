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
// with one going, or the one last watched, it shows that run. Closing the dialog leaves a run
// going: the run is the server's, and opening the dialog again shows it.

import { useTranslation } from "react-i18next";
import { Modal } from "@mantine/core";
import type { DiscoveryScope } from "../../api/admin";
import { DiscoverRunView, type SuggestionActions } from "./DiscoverRunView";
import { DiscoverScopeForm, type ScopeTable } from "./DiscoverScopeForm";
import type { DiscoveryRunState } from "./useDiscoveryRun";

interface DiscoverDialogProps {
  opened: boolean;
  onClose: () => void;
  discovery: DiscoveryRunState;
  tables: ScopeTable[];
  /** The domains ticked in the Domains picker: the source domains a new run starts with. */
  defaultDomains: string[];
  /** Every waiting suggestion, with their actions: the dialog lists the shown run's own. */
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
  const { run } = discovery;
  const runScope = run ? `run:${run.id}` : null;
  const runSuggestions = {
    ...suggestions,
    candidates: suggestions.candidates.filter((c) => c.scope === runScope),
  };
  return (
    <Modal
      opened={opened}
      onClose={onClose}
      title={t("discoverDialog.title")}
      // A run lists its suggestions in the shared table, whose six columns need the room.
      size={run ? "min(75rem, calc(100vw - 2rem))" : "lg"}
      centered
      data-testid="discover-dialog"
    >
      {/* The scope mounts each time it is shown: the defaults again, and which models can run read
          again, so a key set meanwhile counts. */}
      {opened &&
        (run ? (
          <DiscoverRunView
            run={run}
            readError={discovery.error}
            onStop={() => void discovery.stop()}
            onNewRun={discovery.dismiss}
            suggestions={runSuggestions}
          />
        ) : (
          <DiscoverScopeForm
            tables={tables}
            defaultDomains={defaultDomains}
            onCancel={onClose}
            onRun={(scope: DiscoveryScope) => discovery.start(scope)}
          />
        ))}
    </Modal>
  );
}
