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

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useScreenInputs } from "../../mcp/screenState";
import { useTranslation } from "react-i18next";
import { Button, Group, Loader, Text } from "@mantine/core";
import { useLeaveGuard } from "../list/panelLeave";
import { useDomainFilter } from "../../context/DomainFilterContext";
import { useAllRelationships, useDomains, useTables } from "../../hooks/useAdminQueries";
import { erdViewHref } from "../../lib/entityHref";
import {
  deleteErdLayout,
  fetchErdLayout,
  fetchErdLayouts,
  saveErdLayout,
  type SavedLayout,
  type SavedLayoutName,
} from "../../api/erdLayouts";
import { ErdPanel } from "./ErdPanel";
import { ErdLayoutMenu } from "./ErdLayoutMenu";
import { ErdNewTray } from "./ErdNewTray";
import { focusIsOn, resolveErdFocus, type ResolvedErdFocus } from "./erd-focus";
import {
  arrangementOf,
  layoutFocus,
  newSinceSaved,
  sameFocus,
  type ErdPlaces,
} from "./erd-saved-layout";
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

const NONE: string[] = [];

export function ErdView({
  page,
  search = "",
  selected = null,
  onOpen,
}: {
  page: "tables" | "relationships";
  /** The page's own item whose detail panel is open -- a table's id on Tables, a relationship's
   * on Relationships -- highlighted as a focus's tables are, however the ERD was opened. */
  selected?: string | null;
  /** The page's filter box: highlights what matches and dims the rest; not part of the link. */
  search?: string;
  /** Opens the panel of the page's own kind of item: a table's id on Tables, a relationship's on
   * Relationships. */
  onOpen: (id: string) => void;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  // A click leaves the form open in the panel, as a list row's does.
  const guard = useLeaveGuard();
  const { focus, setFocus, collapsed, layout, setLayout, takeFocus } = useErdView();
  // Polly's read_screen reads the collapsed domains from here: they are the panel's own state.
  const [collapsedDomains, setCollapsedDomains] = useState<string[] | "all">([]);
  useScreenInputs(`/${page}`, { collapsed: collapsedDomains });
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
  // REQ-2189: the saved layouts, the one shown (null for Automatic), and what is edited since it
  // was loaded or saved.
  const [layouts, setLayouts] = useState<SavedLayoutName[]>([]);
  const [loaded, setLoaded] = useState<SavedLayout | null>(null);
  const [edited, setEdited] = useState(false);
  const [placed, setPlaced] = useState<Set<number>>(new Set());
  const [layoutError, setLayoutError] = useState<string | null>(null);
  const placesRef = useRef<(() => ErdPlaces | null) | null>(null);
  const shown = layout !== null && loaded?.name === layout ? loaded : null;
  useEffect(() => {
    fetchErdLayouts().then(
      (answer) => setLayouts(answer.layouts),
      (error: Error) => setLayoutError(error.message),
    );
  }, []);
  const [asked, setAsked] = useState<string | null>(null);
  if (layout !== asked) {
    setAsked(layout);
    setEdited(false);
    setPlaced(new Set());
    setLayoutError(null);
  }
  useEffect(() => {
    if (layout === null || layout === loaded?.name) return;
    fetchErdLayout(layout).then(
      (answer) => setLoaded(answer),
      (error: Error) => setLayoutError(error.message),
    );
  }, [layout, loaded]);
  // A layout's selection is its focus: it takes the link's place once the layout and the tables
  // it names are in hand.
  const [focused, setFocused] = useState<SavedLayout | null>(null);
  if (shown !== null && shown !== focused && !tablesLoading) setFocused(shown);
  const latest = useRef({ tables, takeFocus });
  useEffect(() => {
    latest.current = { tables, takeFocus };
  });
  useEffect(() => {
    if (focused !== null) latest.current.takeFocus(layoutFocus(focused, latest.current.tables));
  }, [focused]);
  const unsaved =
    edited ||
    (shown !== null && shown === focused && !sameFocus(focus, layoutFocus(shown, tables)));
  const narrowed =
    checkedDomains.size > 0 && checkedDomains.size < filterDomains.length ? checkedDomains : null;
  const shownDomains = resolved.shownDomains ?? narrowed;
  const waiting = useMemo(
    () =>
      shown === null
        ? []
        : newSinceSaved(shown, tables, shownDomains).filter((t) => !placed.has(t.id)),
    [shown, tables, shownDomains, placed],
  );
  const drawn = useMemo(() => {
    if (waiting.length === 0) return tables;
    const held = new Set(waiting.map((t) => t.id));
    return tables.filter((t) => !held.has(t.id));
  }, [tables, waiting]);
  const arrange = useCallback(() => setEdited(true), []);

  const save = async (name: string): Promise<boolean> => {
    const places = placesRef.current?.() ?? null;
    if (places === null) {
      setLayoutError(t("erdLayouts.notDrawn"));
      return false;
    }
    const members = drawn.filter((d) => shownDomains === null || shownDomains.has(d.domainId));
    try {
      const saved = await saveErdLayout(name, arrangementOf(places, focus, members, shown));
      setLayouts((await fetchErdLayouts()).layouts);
      setLoaded(saved);
      setFocused(saved);
      setEdited(false);
      setPlaced(new Set());
      setLayoutError(null);
      if (name !== layout) setLayout(name);
      return true;
    } catch (error) {
      setLayoutError((error as Error).message);
      return false;
    }
  };
  const remove = async (name: string): Promise<boolean> => {
    try {
      await deleteErdLayout(name);
      setLayouts((await fetchErdLayouts()).layouts);
      setLoaded(null);
      setLayout(null);
      return true;
    } catch (error) {
      setLayoutError((error as Error).message);
      return false;
    }
  };

  const highlightTableIds = useMemo(
    () =>
      page === "tables" && selected !== null
        ? [...new Set([...resolved.highlighted, Number(selected)])]
        : resolved.highlighted,
    [page, selected, resolved.highlighted],
  );
  const highlightRelationshipIds = useMemo(
    () => (page === "relationships" && selected !== null ? [selected] : NONE),
    [page, selected],
  );
  if (tablesLoading || relationshipsLoading || domainsLoading) {
    return <Loader size="sm" aria-label={t("erdModal.title")} />;
  }
  // A focus is what is shown; the page's filter stands underneath it, unchanged.
  return (
    <div className="erd-view" data-testid="erd-view">
      {focusIsOn(focus) && (
        <ErdFocusBar resolved={resolved} onClear={() => setFocus({ domains: [], objects: [] })} />
      )}
      <div className="erd-view__diagram">
        <ErdPanel
          tables={drawn}
          relationships={relationships}
          domains={domains}
          checkedDomains={shownDomains}
          savedLayout={shown}
          onArrange={arrange}
          placesRef={placesRef}
          layoutMenu={
            <>
              <ErdLayoutMenu
                layouts={layouts}
                current={layout}
                unsaved={unsaved}
                onChoose={setLayout}
                onSave={save}
                onDelete={remove}
              />
              {layoutError !== null && (
                <Text size="sm" c="red" data-testid="erd-layout-error">
                  {layoutError}
                </Text>
              )}
            </>
          }
          overlay={
            waiting.length > 0 && (
              <ErdNewTray
                tables={waiting}
                onPlace={(id) => {
                  setPlaced((before) => new Set([...before, id]));
                  setEdited(true);
                }}
              />
            )
          }
          highlightTableIds={highlightTableIds}
          highlightRelationshipIds={highlightRelationshipIds}
          search={search}
          page={page}
          collapsed={collapsed}
          onCollapsed={setCollapsedDomains}
          onTableClick={(id) =>
            guard(() =>
              page === "tables"
                ? onOpen(String(id))
                : navigate(erdViewHref("tables", { ...focus, table: id, layout })),
            )
          }
          onRelationshipClick={(id) =>
            guard(() =>
              page === "relationships"
                ? onOpen(id)
                : navigate(erdViewHref("relationships", { ...focus, relationship: id, layout })),
            )
          }
        />
      </div>
    </div>
  );
}
