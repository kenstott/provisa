// Copyright (c) 2026 Kenneth Stott
// Canary: 3f7b2c90-8e14-4d6a-b5c3-9a1e6d0f4b27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 / REQ-2262: a source's setup, one flow from connecting it to querying it, shown with
// the same step strip wherever it is reached: right after the source is saved, or later from the
// source's read view. Connect source, Register tables, Your data, Discover relationships. A step is
// done (a check, pressed to go back to it), current, or not yet reached. A step is done once it has
// an outcome -- recorded per source on the server, shared by every user and environment -- or once
// it has produced its object (tables registered, a relationship touching them).
//
// On the read view the strip carries one line for the current step; opened (the panel's working
// surface, as it is after a save) the step's own content follows the strip: Register Table with the
// source chosen and its tables that need no input already picked; Your data, the registered tables'
// rows with a query ready to open; the relationship review. Each step offers one way past it:
// Register tables beside Skip; Your data finishes on opening the query explorer (recorded before
// it opens, so returning resumes at the next step) or Done; Discover relationships finishes either
// way after a run (found or none found) and offers Skip before one. Finish later leaves the flow
// with nothing recorded; Finish setup, at the strip's end, closes every remaining step as skipped.
// With every step decided the strip collapses to one "Setup complete" line with Run setup again; it
// never reopens on its own, but tables registered since, related to nothing, show as a one-line
// hint on that line.

import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, Stack, Text } from "@mantine/core";
import { Check } from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { useDomainFilter } from "../../context/DomainFilterContext";
import {
  useDomains,
  useRelationships,
  useRoles,
  useSources,
  useSuggestTableAlias,
  useTableAccess,
  useTableDetails,
  useTables,
} from "../../hooks/useAdminQueries";
import { useCapability } from "../../hooks/useCapability";
import {
  useSetSourceSetupOutcomes,
  type SetupOutcomesWrite,
} from "../../hooks/useSourceSetupOutcomes";
import { StepStrip } from "../../components/StepStrip";
import { RelationshipReview } from "../../components/relationships/RelationshipReview";
import { RegisterTableForm } from "../tables/RegisterTableForm";
import { YourDataStep } from "../tables/YourDataStep";
import { registeredOf, type Registered } from "../tables/useManyTables";
import type { TableRegistrationOutcome } from "../../hooks/useTableQueries";
import type { Relationship, SourceSetupOutcomes, TableRow } from "../../types/admin";

const CONNECT = 0;
const REGISTER = 1;
const YOUR_DATA = 2;
const RELATE = 3;
const COLLAPSED = 4;

function touches(r: Relationship, ids: Set<number>): boolean {
  return ids.has(r.sourceTableId) || (r.targetTableId !== null && ids.has(r.targetTableId));
}

function QueryHandOff({
  table,
  role,
  onOpen,
}: {
  table: TableRow;
  role: string | null;
  onOpen: (graphql: string) => void;
}) {
  const { t } = useTranslation();
  const { access } = useTableAccess(table.id, role);
  return (
    <>
      <Text size="sm" c="dimmed">
        {t("sourceSetup.queryHint", { table: table.alias || table.tableName })}
      </Text>
      <Button
        size="compact-sm"
        disabled={access === null}
        onClick={() => access && onOpen(access.graphql)}
        data-testid="source-setup-query"
      >
        {t("sourceSetup.queryAction")}
      </Button>
    </>
  );
}

/** Register tables, opened: Register Table with the source chosen, as the Tables page's form. */
function RegisterStep({
  sourceId,
  onRegistered,
  onCancel,
}: {
  sourceId: string;
  onRegistered: (outcomes: TableRegistrationOutcome[]) => void;
  onCancel: () => void;
}) {
  const navigate = useNavigate();
  const { domainAccess } = useAuth();
  const { checkedDomains, domainsEnabled } = useDomainFilter();
  const { sources } = useSources();
  const { domains } = useDomains();
  const { roles } = useRoles();
  const { tables } = useTableDetails();
  const { suggestTableAlias } = useSuggestTableAlias();
  const [error, setError] = useState<string | null>(null);
  return (
    <RegisterTableForm
      sources={sources}
      domainHints={domains.map((d) => d.id)}
      domainAccess={domainAccess}
      checkedDomains={checkedDomains}
      domainsEnabled={domainsEnabled}
      tables={tables}
      roles={roles}
      suggestTableAlias={suggestTableAlias}
      initialSourceId={sourceId}
      pickReady
      onRegisteredMany={onRegistered}
      // A table registered on its own: its settings follow in the table editor, as on the Tables
      // page.
      onContinue={(registration) => navigate("/tables", { state: { createTable: registration } })}
      onCancel={onCancel}
      onProductsRegistered={() => navigate("/data-products")}
      setError={setError}
      problem={error}
    />
  );
}

export function SourceSetupStepper({
  sourceId,
  outcomes,
  open,
  step,
  onConnect,
  onOpen,
  onClose,
  onRelated,
}: {
  sourceId: string;
  /** The source's setup record, as the server holds it. */
  outcomes: SourceSetupOutcomes;
  /** Shown as the panel's working surface, each step with its content; else the read view's strip. */
  open: boolean;
  /** Opened on this done step (gone back to); null opens on the current step. */
  step: number | null;
  /** Back to the source's connection: its edit form. */
  onConnect: () => void;
  /** Open the flow, on a done step gone back to or (null) on the current one. */
  onOpen: (step: number | null) => void;
  /** Leave the opened flow: Finish later, or every step decided. */
  onClose: () => void;
  /** Discover relationships is decided by a review, with how many relationships it created. */
  onRelated: (count: number) => void;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { actingRoles } = useAuth();
  const mayRelate = useCapability("create_relationship");
  const {
    tables,
    loading: tablesLoading,
    refetch: refetchTables,
  } = useTables({ columns: false, includeWithdrawn: true });
  const {
    relationships,
    loading: relationshipsLoading,
    refetch: refetchRelationships,
  } = useRelationships();
  const setOutcomes = useSetSourceSetupOutcomes();
  // The done step the opened flow shows, gone back to; null shows the current step.
  const [viewing, setViewing] = useState<number | null>(step);
  // What the registration just made answered: the tables Your data shows and the ones refused.
  const [registered, setRegistered] = useState<Registered | null>(null);
  // The tables a review opened from here is scoped to: the source's, or (from the collapsed
  // line's hint) those registered since setup was decided. Null while no review is open.
  const [reviewing, setReviewing] = useState<number[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loading = tablesLoading || relationshipsLoading;
  const own = tables.filter((tbl) => tbl.sourceId === sourceId);
  const ownIds = new Set(own.map((tbl) => tbl.id));
  const related = relationships.some((r) => touches(r, ownIds));

  // Each step's outcome: recorded, or produced by its object.
  const tablesOutcome = outcomes.tables ?? (own.length > 0 ? "registered" : null);
  // With no table of its own, or no other table in the model, there is nothing to relate; without
  // the create relationship right this person cannot. Either way the step is passed, not decided.
  const nothingToRelate = own.length === 0 || tables.length < 2;
  const relationshipsOutcome = outcomes.relationships ?? (related ? "found" : null);
  const relatePassed = relationshipsOutcome !== null || nothingToRelate || !mayRelate;
  // What Discover relationships is recorded as when a later decision closes it undecided.
  const relateClosed = relationshipsOutcome ?? (nothingToRelate ? "none_found" : "skipped");

  const active =
    tablesOutcome === null
      ? REGISTER
      : outcomes.query === null
        ? YOUR_DATA
        : !relatePassed
          ? RELATE
          : COLLAPSED;

  // Opened, the flow ends once every step is decided: the source's read shows Setup complete.
  useEffect(() => {
    if (open && !loading && active === COLLAPSED) onClose();
  }, [open, loading, active, onClose]);

  if (loading) return null;

  const record = (next: SetupOutcomesWrite, then?: () => void) => {
    setError(null);
    setOutcomes(sourceId, next).then(then, (e: Error) => setError(e.message));
  };
  /** Your data decided: the steps it passes are recorded as they show. Discover relationships,
   * passed without a decision (nothing to relate, or not permitted), is closed with it. */
  const decideQuery = (query: "opened" | "done", then?: () => void) =>
    record(
      {
        tables: tablesOutcome,
        relationships: relatePassed ? relateClosed : outcomes.relationships,
        query,
      },
      then,
    );
  const openQuery = (graphql: string) =>
    decideQuery("opened", () => navigate("/query", { state: { query: graphql } }));
  const finishSetup = () =>
    record({
      tables: tablesOutcome ?? "skipped",
      relationships: relateClosed,
      query: outcomes.query ?? "skipped",
    });
  const onRegistered = (answers: TableRegistrationOutcome[]) => {
    setRegistered(registeredOf(answers));
    setViewing(null);
    // The source's tables are read again before the step is recorded, so the steps after it
    // count the tables just registered.
    void Promise.resolve(refetchTables()).then(() =>
      record({ tables: "registered", relationships: outcomes.relationships, query: outcomes.query }),
    );
  };
  const review = reviewing !== null && (
    <RelationshipReview
      tableIds={reviewing}
      onDone={(created) => {
        const fromStep = active === RELATE;
        setReviewing(null);
        void refetchRelationships();
        if (fromStep) {
          record(
            {
              tables: tablesOutcome,
              relationships: created > 0 ? "found" : "none_found",
              query: outcomes.query,
            },
            () => onRelated(created),
          );
        } else if (active === COLLAPSED) {
          // From the hint: the tables it named are reviewed, so setup is decided again from now.
          record({
            tables: tablesOutcome,
            relationships: created > 0 ? "found" : relationshipsOutcome,
            query: outcomes.query,
          });
        }
      }}
    />
  );
  const failed = error !== null && (
    <Alert color="red" data-testid="source-setup-error">
      {error}
    </Alert>
  );

  if (active === COLLAPSED) {
    const decidedAt = outcomes.decidedAt === null ? null : Date.parse(outcomes.decidedAt);
    const unrelatedSince = own.filter(
      (tbl) =>
        decidedAt !== null &&
        tbl.registeredAt !== null &&
        Date.parse(tbl.registeredAt) > decidedAt &&
        !relationships.some((r) => touches(r, new Set([tbl.id]))),
    );
    return (
      <Stack gap="xs">
        <Group gap={6} data-testid="source-setup-complete">
          <Check size={14} />
          <Text size="sm" c="dimmed">
            {t("sourceSetup.complete")}
          </Text>
          <Button
            size="compact-sm"
            variant="subtle"
            onClick={() => record({ tables: null, relationships: null, query: null })}
            data-testid="source-setup-again"
          >
            {t("sourceSetup.runAgain")}
          </Button>
        </Group>
        {unrelatedSince.length > 0 && (
          <Group gap="sm" wrap="nowrap" data-testid="source-setup-new-tables">
            <Text size="sm" c="dimmed">
              {t("sourceSetup.newTablesHint", { count: unrelatedSince.length })}
            </Text>
            {mayRelate && (
              <Button
                size="compact-sm"
                variant="default"
                onClick={() => setReviewing(unrelatedSince.map((tbl) => tbl.id))}
                data-testid="source-setup-new-tables-relate"
              >
                {t("sourceSetup.relateAction")}
              </Button>
            )}
          </Group>
        )}
        {failed}
        {review}
      </Stack>
    );
  }

  // The step whose content follows the strip: a done step gone back to, while opened, else the
  // current one.
  const shown = open && viewing !== null && viewing < active ? viewing : active;
  const skippedText = t("sourceSetup.skipped");
  const goBack = (to: number) => {
    if (to === CONNECT) onConnect();
    else if (to === RELATE) {
      if (mayRelate && own.length > 0) setReviewing(own.map((tbl) => tbl.id));
    } else if (open) setViewing(to);
    else onOpen(to);
  };
  const skip = (key: keyof SetupOutcomesWrite, write: SetupOutcomesWrite) => (
    <Button
      size="compact-sm"
      variant="default"
      onClick={() => record(write)}
      data-testid={`source-setup-skip-${key}`}
    >
      {t("sourceSetup.skip")}
    </Button>
  );
  const done = (
    <Button
      size="compact-sm"
      variant="default"
      onClick={() => decideQuery("done")}
      data-testid="source-setup-done"
    >
      {t("sourceSetup.done")}
    </Button>
  );
  const relateNote = shown === YOUR_DATA && relationshipsOutcome === null && relatePassed && (
    <Text size="sm" c="dimmed" data-testid="source-setup-relate-note">
      {nothingToRelate
        ? t("sourceSetup.nothingToRelateHint")
        : t("sourceSetup.relateNotAllowedHint")}
    </Text>
  );
  // The tables Your data shows: those just registered, else the source's own.
  const yourData: Registered = registered ?? {
    tableIds: own.map((tbl) => tbl.id),
    refused: [],
  };

  return (
    <Stack gap="xs" data-testid="source-setup-stepper" data-active={active} data-shown={shown}>
      <Group gap="sm" wrap="nowrap" align="flex-start">
        <div style={{ flex: 1, minWidth: 0 }}>
          <StepStrip
            steps={[
              { key: "connect", label: t("sourceSetup.connect") },
              {
                key: "register",
                label: t("sourceSetup.register"),
                description:
                  own.length > 0
                    ? t("sourceSetup.tablesCount", { count: own.length })
                    : tablesOutcome === "skipped"
                      ? skippedText
                      : undefined,
              },
              {
                key: "yourData",
                label: t("sourceSetup.yourData"),
                description: outcomes.query === "skipped" ? skippedText : undefined,
              },
              {
                key: "relate",
                label: t("sourceSetup.relate"),
                description:
                  relationshipsOutcome === "skipped"
                    ? skippedText
                    : relationshipsOutcome === "none_found" ||
                        (relationshipsOutcome === null && nothingToRelate && active > REGISTER)
                      ? t("sourceSetup.nothingToRelate")
                      : relationshipsOutcome === null && !mayRelate && active > REGISTER
                        ? t("sourceSetup.relateNotAllowed")
                        : undefined,
              },
            ]}
            active={active}
            onStepClick={goBack}
            label={t("sourceSetup.label")}
            testId="source-setup-step"
          />
        </div>
        <Button
          size="compact-sm"
          variant="subtle"
          onClick={finishSetup}
          data-testid="source-setup-finish"
        >
          {t("sourceSetup.finish")}
        </Button>
      </Group>
      {shown === REGISTER && !open && (
        <Group gap="sm" wrap="nowrap">
          <Text size="sm" c="dimmed">
            {t("sourceSetup.registerHint")}
          </Text>
          <Button size="compact-sm" onClick={() => onOpen(null)} data-testid="source-setup-register">
            {t("sourceSetup.registerAction")}
          </Button>
          {skip("tables", {
            tables: "skipped",
            relationships: outcomes.relationships,
            query: outcomes.query,
          })}
        </Group>
      )}
      {shown === REGISTER && open && (
        <Stack gap="xs" data-testid="source-setup-register-step">
          {active === REGISTER && (
            <Group gap="sm" wrap="nowrap">
              {skip("tables", {
                tables: "skipped",
                relationships: outcomes.relationships,
                query: outcomes.query,
              })}
            </Group>
          )}
          <RegisterStep sourceId={sourceId} onRegistered={onRegistered} onCancel={onClose} />
        </Stack>
      )}
      {shown === YOUR_DATA && !open && (
        <Stack gap={4}>
          {relateNote}
          <Group gap="sm" wrap="nowrap">
            {own.length > 0 ? (
              <>
                <QueryHandOff table={own[0]} role={actingRoles} onOpen={openQuery} />
                <Button
                  size="compact-sm"
                  variant="default"
                  onClick={() => onOpen(null)}
                  data-testid="source-setup-your-data"
                >
                  {t("sourceSetup.yourDataAction")}
                </Button>
              </>
            ) : (
              <Text size="sm" c="dimmed">
                {t("sourceSetup.nothingToQueryHint")}
              </Text>
            )}
            {done}
          </Group>
        </Stack>
      )}
      {shown === YOUR_DATA && open && (
        <Stack gap="xs">
          {relateNote}
          {active === YOUR_DATA && <Group gap="sm">{done}</Group>}
          {yourData.tableIds.length > 0 || yourData.refused.length > 0 ? (
            <YourDataStep registered={yourData} role={actingRoles} onOpenQuery={openQuery} />
          ) : (
            <Text size="sm" c="dimmed">
              {t("sourceSetup.nothingToQueryHint")}
            </Text>
          )}
        </Stack>
      )}
      {shown === RELATE && (
        <Group gap="sm" wrap="nowrap">
          <Text size="sm" c="dimmed">
            {t("sourceSetup.relateHint")}
          </Text>
          <Button
            size="compact-sm"
            onClick={() => setReviewing(own.map((tbl) => tbl.id))}
            data-testid="source-setup-relate"
          >
            {t("sourceSetup.relateAction")}
          </Button>
          {skip("relationships", {
            tables: tablesOutcome,
            relationships: "skipped",
            query: outcomes.query,
          })}
        </Group>
      )}
      {failed}
      {review}
    </Stack>
  );
}
