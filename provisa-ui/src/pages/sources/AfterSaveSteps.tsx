// Copyright (c) 2026 Kenneth Stott
// Canary: 4e9a7c13-6b25-4f80-8d3e-c1f5a2b7e964
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: from connecting a source to querying it, in the source's own detail panel. Once a
// source is saved, the panel opens on Register tables: Register Table with the source already
// chosen, its tables listed and those that need no input ticked, so one action registers them
// all. Once they are registered, the panel shows Your data. Skipping a step closes it.

import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useAuth } from "../../context/AuthContext";
import { useDomainFilter } from "../../context/DomainFilterContext";
import {
  useAvailableColumnsMetadataLazy,
  useDomains,
  useRoles,
  useSources,
  useSuggestTableAlias,
  useTableDetails,
} from "../../hooks/useAdminQueries";
import { RegisterTableForm } from "../tables/RegisterTableForm";
import { YourDataStep } from "../tables/YourDataStep";
import { registeredOf, type Registered } from "../tables/useManyTables";
import { PanelWorking } from "./PanelWorking";

export function AfterSaveSteps({ sourceId, onClose }: { sourceId: string; onClose: () => void }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { domainAccess, actingRoles } = useAuth();
  const { checkedDomains, domainsEnabled } = useDomainFilter();
  const { sources } = useSources();
  const { domains } = useDomains();
  const { roles } = useRoles();
  const { tables } = useTableDetails();
  const columns = useAvailableColumnsMetadataLazy();
  const { suggestTableAlias } = useSuggestTableAlias();
  const [error, setError] = useState<string | null>(null);
  const [registered, setRegistered] = useState<Registered | null>(null);

  if (registered !== null) {
    return (
      <PanelWorking back={t("yourData.close")} onBack={onClose} testId="your-data-back">
        <YourDataStep registered={registered} role={actingRoles} />
      </PanelWorking>
    );
  }
  return (
    <PanelWorking
      back={t("registerManyTables.skip")}
      onBack={onClose}
      testId="register-tables-skip"
    >
      <RegisterTableForm
        sources={sources}
        domainHints={domains.map((d) => d.id)}
        domainAccess={domainAccess}
        checkedDomains={checkedDomains}
        domainsEnabled={domainsEnabled}
        tables={tables}
        roles={roles}
        getAvailableColumnsMetadata={columns.fetchColumns}
        lateColumns={columns.late}
        columnsStarting={columns.starting}
        columnsStartingTimedOut={columns.startingTimedOut}
        suggestTableAlias={suggestTableAlias}
        initialSourceId={sourceId}
        severalFirst
        onRegisteredMany={(outcomes) => setRegistered(registeredOf(outcomes))}
        // One table picked: its settings follow in the table editor, as on the Tables page.
        onContinue={(registration) => navigate("/tables", { state: { createTable: registration } })}
        onCancel={onClose}
        onProductsRegistered={() => navigate("/data-products")}
        setError={setError}
        problem={error}
      />
    </PanelWorking>
  );
}
