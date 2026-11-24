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
// A step is done (a check, pressed to go back to it), current, or not yet reached. It shows while
// setup is incomplete -- no registered tables, or none of them related yet -- and then collapses
// to one "Setup complete" line. The relationships step runs discovery against every table in the
// model, across sources; with nothing to relate it says so and moves on to Query, which opens the
// query explorer on a starter query over the source's first table.

import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Button, Group, Stack, Stepper, Text } from "@mantine/core";
import { Check } from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { useRelationships, useTableAccess, useTables } from "../../hooks/useAdminQueries";
import { useCapability } from "../../hooks/useCapability";
import { RelationshipReview } from "../../components/relationships/RelationshipReview";
import type { Relationship, TableRow } from "../../types/admin";

const CONNECT = 0;
const REGISTER = 1;
const RELATE = 2;
const QUERY = 3;

function touches(r: Relationship, ids: Set<number>): boolean {
  return ids.has(r.sourceTableId) || (r.targetTableId !== null && ids.has(r.targetTableId));
}

function QueryHandOff({ table, role }: { table: TableRow; role: string | null }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { access } = useTableAccess(table.id, role);
  return (
    <Group gap="sm" wrap="nowrap">
      <Text size="sm" c="dimmed">
        {t("sourceSetup.queryHint", { table: table.alias || table.tableName })}
      </Text>
      <Button
        size="compact-sm"
        disabled={access === null}
        onClick={() => access && navigate("/query", { state: { query: access.graphql } })}
        data-testid="source-setup-query"
      >
        {t("sourceSetup.queryAction")}
      </Button>
    </Group>
  );
}

export function SourceSetupStepper({
  sourceId,
  onConnect,
  onRegister,
}: {
  sourceId: string;
  /** Back to the source's connection: its edit form. */
  onConnect: () => void;
  /** Register Table with this source chosen (the panel's Register tables step). */
  onRegister: () => void;
}) {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const mayRelate = useCapability("create_relationship");
  const { tables, loading: tablesLoading } = useTables({ columns: false, includeWithdrawn: true });
  const {
    relationships,
    loading: relationshipsLoading,
    refetch: refetchRelationships,
  } = useRelationships();
  const [reviewing, setReviewing] = useState(false);
  // A review that created nothing: discovery found nothing to relate these tables to.
  const [foundNone, setFoundNone] = useState(false);

  if (tablesLoading || relationshipsLoading) return null;

  const own = tables.filter((tbl) => tbl.sourceId === sourceId);
  const ownIds = new Set(own.map((tbl) => tbl.id));
  const related = relationships.some((r) => touches(r, ownIds));
  if (own.length > 0 && related) {
    return (
      <Group gap={6} data-testid="source-setup-complete">
        <Check size={14} />
        <Text size="sm" c="dimmed">
          {t("sourceSetup.complete")}
        </Text>
      </Group>
    );
  }

  // One table in the whole model has no other table to be related to.
  const nothingToRelate = tables.length < 2 || foundNone;
  const active = own.length === 0 ? REGISTER : nothingToRelate || !mayRelate ? QUERY : RELATE;

  const goBack = (step: number) => {
    if (step === CONNECT) onConnect();
    else if (step === REGISTER) onRegister();
    else if (step === RELATE && mayRelate) setReviewing(true);
  };

  return (
    <Stack gap="xs" data-testid="source-setup-stepper" data-active={active}>
      <Stepper
        active={active}
        onStepClick={goBack}
        allowNextStepsSelect={false}
        size="xs"
        iconSize={22}
        aria-label={t("sourceSetup.label")}
      >
        <Stepper.Step label={t("sourceSetup.connect")} data-testid="source-setup-step-connect" />
        <Stepper.Step
          label={t("sourceSetup.register")}
          description={
            own.length > 0 ? t("sourceSetup.tablesCount", { count: own.length }) : undefined
          }
          data-testid="source-setup-step-register"
        />
        <Stepper.Step
          label={t("sourceSetup.relate")}
          description={
            active === QUERY
              ? nothingToRelate
                ? t("sourceSetup.nothingToRelate")
                : t("sourceSetup.relateNotAllowed")
              : undefined
          }
          data-testid="source-setup-step-relate"
        />
        <Stepper.Step label={t("sourceSetup.query")} data-testid="source-setup-step-query" />
      </Stepper>
      {active === REGISTER && (
        <Group gap="sm" wrap="nowrap">
          <Text size="sm" c="dimmed">
            {t("sourceSetup.registerHint")}
          </Text>
          <Button size="compact-sm" onClick={onRegister} data-testid="source-setup-register">
            {t("sourceSetup.registerAction")}
          </Button>
        </Group>
      )}
      {active === RELATE && (
        <Group gap="sm" wrap="nowrap">
          <Text size="sm" c="dimmed">
            {t("sourceSetup.relateHint")}
          </Text>
          <Button
            size="compact-sm"
            onClick={() => setReviewing(true)}
            data-testid="source-setup-relate"
          >
            {t("sourceSetup.relateAction")}
          </Button>
        </Group>
      )}
      {active === QUERY && (
        <Stack gap={4}>
          <Text size="sm" c="dimmed" data-testid="source-setup-relate-note">
            {nothingToRelate
              ? t("sourceSetup.nothingToRelateHint")
              : t("sourceSetup.relateNotAllowedHint")}
          </Text>
          <QueryHandOff table={own[0]} role={actingRoles} />
        </Stack>
      )}
      {reviewing && (
        <RelationshipReview
          tableIds={own.map((tbl) => tbl.id)}
          onDone={(created) => {
            setReviewing(false);
            if (created === 0) setFoundNone(true);
            void refetchRelationships();
          }}
        />
      )}
    </Stack>
  );
}
