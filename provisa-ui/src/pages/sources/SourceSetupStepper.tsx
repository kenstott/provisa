// Copyright (c) 2026 Kenneth Stott
// Canary: 3f7b2c90-8e14-4d6a-b5c3-9a1e6d0f4b27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 / REQ-2262: the path from connecting a source to querying it, as a compact stepper at
// the top of the source's panel: Connect source, Register tables, Discover relationships, Query.
// A step is done (a check, pressed to go back to it), current, or not yet reached. A step is done
// once it has an outcome -- recorded per source on the server, shared by every user and
// environment -- or once it has produced its object (tables registered, a relationship touching
// them). Each step offers one way past it: Register tables beside Skip; Discover relationships
// finishes either way after a run (found or none found) and offers Skip before one; Query finishes
// on opening the explorer or Done. Finish setup, at the strip's end, closes every remaining step as
// skipped. With every step decided the strip collapses to one "Setup complete" line with Run setup
// again; it never reopens on its own, but tables registered since, related to nothing, show as a
// one-line hint on that line.

import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, Stack, Text } from "@mantine/core";
import { Check } from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { useRelationships, useTableAccess, useTables } from "../../hooks/useAdminQueries";
import { useCapability } from "../../hooks/useCapability";
import {
  useSetSourceSetupOutcomes,
  type SetupOutcomesWrite,
} from "../../hooks/useSourceSetupOutcomes";
import { StepStrip } from "../../components/StepStrip";
import { RelationshipReview } from "../../components/relationships/RelationshipReview";
import type { Relationship, SourceSetupOutcomes, TableRow } from "../../types/admin";

const CONNECT = 0;
const REGISTER = 1;
const RELATE = 2;
const QUERY = 3;
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

export function SourceSetupStepper({
  sourceId,
  outcomes,
  onConnect,
  onRegister,
}: {
  sourceId: string;
  /** The source's setup record, as the server holds it. */
  outcomes: SourceSetupOutcomes;
  /** Back to the source's connection: its edit form. */
  onConnect: () => void;
  /** Register Table with this source chosen (the panel's Register tables step). */
  onRegister: () => void;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { actingRoles } = useAuth();
  const mayRelate = useCapability("create_relationship");
  const { tables, loading: tablesLoading } = useTables({ columns: false, includeWithdrawn: true });
  const {
    relationships,
    loading: relationshipsLoading,
    refetch: refetchRelationships,
  } = useRelationships();
  const setOutcomes = useSetSourceSetupOutcomes();
  // The tables a review opened from here is scoped to: the source's, or (from the collapsed
  // line's hint) those registered since setup was decided. Null while no review is open.
  const [reviewing, setReviewing] = useState<number[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  if (tablesLoading || relationshipsLoading) return null;

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

  const active =
    tablesOutcome === null
      ? REGISTER
      : !relatePassed
        ? RELATE
        : outcomes.query === null
          ? QUERY
          : COLLAPSED;

  const record = (next: SetupOutcomesWrite, then?: () => void) => {
    setError(null);
    setOutcomes(sourceId, next).then(then, (e: Error) => setError(e.message));
  };
  /** What the steps before `step` are recorded as once `step` is decided: what they show. */
  const settledBefore = (step: number): SetupOutcomesWrite => ({
    tables: step > REGISTER ? tablesOutcome : outcomes.tables,
    relationships:
      step > RELATE
        ? (relationshipsOutcome ?? (nothingToRelate ? "none_found" : "skipped"))
        : outcomes.relationships,
    query: outcomes.query,
  });
  const finishSetup = () =>
    record({
      tables: tablesOutcome ?? "skipped",
      relationships: relationshipsOutcome ?? (nothingToRelate ? "none_found" : "skipped"),
      query: outcomes.query ?? "skipped",
    });
  const review = reviewing !== null && (
    <RelationshipReview
      tableIds={reviewing}
      onDone={(created) => {
        const fromStep = active === RELATE;
        setReviewing(null);
        void refetchRelationships();
        if (fromStep) {
          record({ ...settledBefore(RELATE), relationships: created > 0 ? "found" : "none_found" });
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

  const skippedText = t("sourceSetup.skipped");
  const goBack = (step: number) => {
    if (step === CONNECT) onConnect();
    else if (step === REGISTER) onRegister();
    else if (step === RELATE && mayRelate && own.length > 0) setReviewing(own.map((tbl) => tbl.id));
  };
  const skip = (step: number, key: keyof SetupOutcomesWrite) => (
    <Button
      size="compact-sm"
      variant="default"
      onClick={() => record({ ...settledBefore(step), [key]: "skipped" })}
      data-testid={`source-setup-skip-${key}`}
    >
      {t("sourceSetup.skip")}
    </Button>
  );

  return (
    <Stack gap="xs" data-testid="source-setup-stepper" data-active={active}>
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
                key: "relate",
                label: t("sourceSetup.relate"),
                description:
                  relationshipsOutcome === "skipped"
                    ? skippedText
                    : relationshipsOutcome === "none_found" ||
                        (relationshipsOutcome === null && active === QUERY && nothingToRelate)
                      ? t("sourceSetup.nothingToRelate")
                      : relationshipsOutcome === null && active === QUERY
                        ? t("sourceSetup.relateNotAllowed")
                        : undefined,
              },
              { key: "query", label: t("sourceSetup.query") },
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
      {active === REGISTER && (
        <Group gap="sm" wrap="nowrap">
          <Text size="sm" c="dimmed">
            {t("sourceSetup.registerHint")}
          </Text>
          <Button size="compact-sm" onClick={onRegister} data-testid="source-setup-register">
            {t("sourceSetup.registerAction")}
          </Button>
          {skip(REGISTER, "tables")}
        </Group>
      )}
      {active === RELATE && (
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
          {skip(RELATE, "relationships")}
        </Group>
      )}
      {active === QUERY && (
        <Stack gap={4}>
          {relationshipsOutcome === null && (
            <Text size="sm" c="dimmed" data-testid="source-setup-relate-note">
              {nothingToRelate
                ? t("sourceSetup.nothingToRelateHint")
                : t("sourceSetup.relateNotAllowedHint")}
            </Text>
          )}
          <Group gap="sm" wrap="nowrap">
            {own.length > 0 ? (
              <QueryHandOff
                table={own[0]}
                role={actingRoles}
                onOpen={(graphql) =>
                  record({ ...settledBefore(QUERY), query: "opened" }, () =>
                    navigate("/query", { state: { query: graphql } }),
                  )
                }
              />
            ) : (
              <Text size="sm" c="dimmed">
                {t("sourceSetup.nothingToQueryHint")}
              </Text>
            )}
            <Button
              size="compact-sm"
              variant="default"
              onClick={() => record({ ...settledBefore(QUERY), query: "done" })}
              data-testid="source-setup-done"
            >
              {t("sourceSetup.done")}
            </Button>
          </Group>
        </Stack>
      )}
      {failed}
      {review}
    </Stack>
  );
}
