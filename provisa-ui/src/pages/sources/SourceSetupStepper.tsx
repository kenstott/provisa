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
// source's read view. Connect source, Register tables, Review samples, Discover relationships. A step is
// done once it has an outcome -- recorded per source on the server, shared by every user and
// environment -- or once it has produced its object (tables registered, a relationship touching
// them).
//
// The strip shows progress only -- done, skipped, current -- and holds no buttons: pressing a done
// step or the current one opens the flow on it. Opened (the panel's working surface, as it is after
// a save) the step's content follows the strip and its choices are the panel's command bar (its
// pinned head, PanelActions): the step's primary action, Skip step where the step may be skipped,
// and Finish later at the end. Register tables' primary is the form's own Register N tables, and it
// has no Skip step: the later steps read the tables it registers. Review samples shows each
// table's first rows as a tab and is done once every tab has been opened, or on Done (its Query it
// link opens the query explorer, which decides it too, recorded before it opens, so returning
// resumes at the next step); it has Done and Skip step. Discover relationships has Create relationships (a review, decided either
// way after a run, found or none found) and Skip step. Finish later closes the flow and keeps what
// is recorded. With every step decided the strip collapses to one "Setup complete" line with Run
// setup again; it never reopens on its own, but tables registered since, related to nothing, show
// as a one-line hint on that line.

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
  useTableDetails,
  useTables,
} from "../../hooks/useAdminQueries";
import { useCapability } from "../../hooks/useCapability";
import {
  useSetSourceSetupOutcomes,
  type SetupOutcomesWrite,
} from "../../hooks/useSourceSetupOutcomes";
import { StepStrip } from "../../components/StepStrip";
import { PanelActions } from "../../components/list/SplitPane";
import { PanelBody, PanelStrip } from "../../components/list/PanelBody";
import { RelationshipReview } from "../../components/relationships/RelationshipReview";
import { RegisterTableForm } from "../tables/RegisterTableForm";
import { ReviewSamplesStep } from "./ReviewSamplesStep";
import { registeredOf, type Registered } from "../tables/useManyTables";
import type { TableRegistrationOutcome } from "../../hooks/useTableQueries";
import type { Relationship, SourceSetupOutcomes } from "../../types/admin";

const CONNECT = 0;
const REGISTER = 1;
const REVIEW_SAMPLES = 2;
const RELATE = 3;
const COLLAPSED = 4;

function touches(r: Relationship, ids: Set<number>): boolean {
  return ids.has(r.sourceTableId) || (r.targetTableId !== null && ids.has(r.targetTableId));
}

/** Register tables, opened: Register Table with the source chosen, as the Tables page's form. */
function RegisterStep({
  sourceId,
  onRegistered,
}: {
  sourceId: string;
  onRegistered: (outcomes: TableRegistrationOutcome[]) => void;
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
      // No Cancel: the flow's command bar has Finish later.
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
  /** Leave the opened flow, keeping what is recorded: Finish later, or every step decided. */
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
  // What the registration just made answered: the tables Review samples shows and the ones refused.
  const [registered, setRegistered] = useState<Registered | null>(null);
  // The tables a review opened from here is scoped to: the source's, or (from the collapsed
  // line's hint) those registered since setup was decided. Null while no review is open.
  const [reviewing, setReviewing] = useState<number[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loading = tablesLoading || relationshipsLoading;
  const own = tables.filter((tbl) => tbl.sourceId === sourceId);
  const ownIds = new Set(own.map((tbl) => tbl.id));
  const related = relationships.some((r) => touches(r, ownIds));

  // Each step's outcome: recorded, or produced by its object -- the source's tables, or those the
  // registration here just made, before the list is read again.
  const tablesOutcome =
    outcomes.tables ??
    (own.length > 0 || (registered !== null && registered.tableIds.length > 0)
      ? "registered"
      : null);
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
        ? REVIEW_SAMPLES
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
  /** Review samples decided: the steps it passes are recorded as they show. Discover relationships,
   * passed without a decision (nothing to relate, or not permitted), is closed with it. */
  const decideQuery = (query: "opened" | "done" | "skipped", then?: () => void) =>
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
  const onRegistered = (answers: TableRegistrationOutcome[]) => {
    setRegistered(registeredOf(answers));
    setViewing(null);
    // The source's tables are read again before the step is recorded, so the steps after it
    // count the tables just registered.
    setError(null);
    Promise.resolve(refetchTables()).then(
      () =>
        record({
          tables: "registered",
          relationships: outcomes.relationships,
          query: outcomes.query,
        }),
      (e: Error) => setError(e.message),
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
  // A step in the strip opens it: Connect is the source's edit form; any other done step, or the
  // current one, opens the flow on it.
  const openStep = (to: number) => {
    if (to === CONNECT) onConnect();
    else if (open) setViewing(to === active ? null : to);
    else onOpen(to === active ? null : to);
  };
  const relateNote = shown === REVIEW_SAMPLES && relationshipsOutcome === null && relatePassed && (
    <Text size="sm" c="dimmed" data-testid="source-setup-relate-note">
      {nothingToRelate
        ? t("sourceSetup.nothingToRelateHint")
        : t("sourceSetup.relateNotAllowedHint")}
    </Text>
  );
  // The tables Review samples shows: those just registered, else the source's own.
  const samples: Registered = registered ?? {
    tableIds: own.map((tbl) => tbl.id),
    refused: [],
  };
  // The command bar, while opened: the current step's primary action and Skip step (a done step
  // gone back to is not decided again), then Finish later. Register tables' primary is its form's.
  const current = shown === active;
  const commandBar = open && (
    <PanelActions>
      {current && shown === REVIEW_SAMPLES && (
        <>
          <Button onClick={() => decideQuery("done")} data-testid="source-setup-done">
            {t("sourceSetup.done")}
          </Button>
          <Button onClick={() => decideQuery("skipped")} data-testid="source-setup-skip">
            {t("sourceSetup.skipStep")}
          </Button>
        </>
      )}
      {current && shown === RELATE && (
        <>
          <Button
            onClick={() => setReviewing(own.map((tbl) => tbl.id))}
            data-testid="source-setup-relate"
          >
            {t("sourceSetup.createRelationships")}
          </Button>
          <Button
            onClick={() =>
              record({ tables: tablesOutcome, relationships: "skipped", query: outcomes.query })
            }
            data-testid="source-setup-skip"
          >
            {t("sourceSetup.skipStep")}
          </Button>
        </>
      )}
      <Button className="panel-head-end" onClick={onClose} data-testid="source-setup-finish-later">
        {t("sourceSetup.finishLater")}
      </Button>
    </PanelActions>
  );

  return (
    <PanelBody data-testid="source-setup-stepper" data-active={active} data-shown={shown}>
      <PanelStrip>
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
              key: "reviewSamples",
              label: t("sourceSetup.reviewSamples"),
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
          onStepClick={openStep}
          currentSelectable
          label={t("sourceSetup.label")}
          testId="source-setup-step"
        />
      </PanelStrip>
      {open && shown === REGISTER && (
        <div data-testid="source-setup-register-step">
          <RegisterStep sourceId={sourceId} onRegistered={onRegistered} />
        </div>
      )}
      {open && shown === REVIEW_SAMPLES && relateNote}
      {open &&
        shown === REVIEW_SAMPLES &&
        (samples.tableIds.length > 0 || samples.refused.length > 0 ? (
          <ReviewSamplesStep
            registered={samples}
            role={actingRoles}
            onOpenQuery={openQuery}
            onAllOpened={() => decideQuery("done")}
          />
        ) : (
          <Text size="sm" c="dimmed">
            {t("sourceSetup.nothingToQueryHint")}
          </Text>
        ))}
      {open && shown === RELATE && (
        <Text size="sm" c="dimmed">
          {t("sourceSetup.relateHint")}
        </Text>
      )}
      {commandBar}
      {failed}
      {review}
    </PanelBody>
  );
}
