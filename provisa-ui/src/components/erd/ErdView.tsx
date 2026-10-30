// Copyright (c) 2026 Kenneth Stott
// Canary: 8f14c3a6-2d75-4e09-a6b3-1c9e7d02f4b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2187: the one ERD view, hosted in place of the list on the Tables and the Relationships page.
// The pages differ only in the panel: a click on the page's own kind of item opens its panel there,
// and a click on the other kind goes to the other page's ERD with that item's panel open.

import { useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Button, Group, Loader, Text } from "@mantine/core";
import { useLeaveGuard } from "../list/panelLeave";
import { useDomainFilter } from "../../context/DomainFilterContext";
import { useAllRelationships, useDomains, useTables } from "../../hooks/useAdminQueries";
import { erdViewHref } from "../../lib/entityHref";
import { ErdPanel } from "./ErdPanel";
import { focusIsOn, resolveErdFocus, type ResolvedErdFocus } from "./erd-focus";
import { useErdView } from "./useErdView";

/** Names the focus, says what of it does not exist, and clears it. */
function ErdFocusBar({ resolved, onClear }: { resolved: ResolvedErdFocus; onClear: () => void }) {
  const { t } = useTranslation();
  return (
    <Group gap="md" className="erd-view__bar" wrap="wrap" data-testid="erd-focus-bar">
      {resolved.domains.length > 0 && (
        <Text size="sm">{t("erdModal.focusDomains", { names: resolved.domains.join(", ") })}</Text>
      )}
      {resolved.tables.length > 0 && (
        <Text size="sm">{t("erdModal.focusTables", { names: resolved.tables.join(", ") })}</Text>
      )}
      {resolved.missing.length > 0 && (
        <Text size="sm" c="red" data-testid="erd-focus-missing">
          {t("erdModal.focusMissing", { names: resolved.missing.join(", ") })}
        </Text>
      )}
      <Button size="compact-sm" variant="default" onClick={onClear} data-testid="erd-show-all">
        {t("erdModal.showAll")}
      </Button>
    </Group>
  );
}

export function ErdView({
  page,
  onOpen,
}: {
  page: "tables" | "relationships";
  /** Opens the panel of the page's own kind of item: a table's id on Tables, a relationship's on
   * Relationships. */
  onOpen: (id: string) => void;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  // A click leaves the form open in the panel, as a list row's does.
  const guard = useLeaveGuard();
  const { focus, setFocus } = useErdView();
  const { checkedDomains, domains: filterDomains } = useDomainFilter();
  const { tables, loading: tablesLoading } = useTables();
  const { relationships, loading: relationshipsLoading } = useAllRelationships();
  const { domains, loading: domainsLoading } = useDomains();
  const resolved = useMemo(
    () =>
      resolveErdFocus(
        focus,
        tables,
        domains.map((d) => d.id),
      ),
    [focus, tables, domains],
  );
  if (tablesLoading || relationshipsLoading || domainsLoading) {
    return <Loader size="sm" aria-label={t("erdModal.title")} />;
  }
  // A focus is what is shown; the page's filter stands underneath it, unchanged.
  const narrowed =
    checkedDomains.size > 0 && checkedDomains.size < filterDomains.length ? checkedDomains : null;
  return (
    <div className="erd-view" data-testid="erd-view">
      {focusIsOn(focus) && (
        <ErdFocusBar resolved={resolved} onClear={() => setFocus({ domains: [], objects: [] })} />
      )}
      <div className="erd-view__diagram">
        <ErdPanel
          tables={tables}
          relationships={relationships}
          domains={domains}
          checkedDomains={resolved.shownDomains ?? narrowed}
          highlightTableIds={resolved.highlighted}
          onTableClick={(id) =>
            guard(() =>
              page === "tables"
                ? onOpen(String(id))
                : navigate(erdViewHref("tables", { ...focus, table: id })),
            )
          }
          onRelationshipClick={(id) =>
            guard(() =>
              page === "relationships"
                ? onOpen(id)
                : navigate(erdViewHref("relationships", { ...focus, relationship: id })),
            )
          }
        />
      </div>
    </div>
  );
}
