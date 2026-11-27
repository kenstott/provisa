// Copyright (c) 2026 Kenneth Stott
// Canary: 6f1c8e24-9b3a-4d57-a2e6-0c7d5b9f3a18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1586: two ticked relationships that share a middle table offer "Join through <table>" in the
// list's bulk bar. It opens a short form with the joined relationship's name, its cardinality and
// its two ends filled in; saving creates one junction-backed relationship, and the two stay.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Text, TextInput } from "@mantine/core";
import { ModalLayout } from "../modal/ModalLayout";
import { Route } from "lucide-react";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { serverMessage } from "../../i18n/serverMessage";
import {
  useJoinThrough,
  useJoinThroughPlan,
  type JoinThroughPlan,
} from "../../hooks/useRelationshipQueries";

export interface JoinThroughActionProps {
  /** The ticked relationships, in list order: the first is the joined relationship's source. */
  ids: readonly string[];
  /** Called once the joined relationship is saved, with its id. */
  onJoined: (id: string) => void;
}

/** The bulk bar's Join through: shown only for two relationships that share a middle table. */
export function JoinThroughAction({ ids, onJoined }: JoinThroughActionProps) {
  const { t } = useTranslation();
  const { plan } = useJoinThroughPlan(ids);
  const [open, setOpen] = useState(false);
  if (plan === null) return null;
  return (
    <>
      <Button
        size="xs"
        variant="light"
        leftSection={<Route size={14} />}
        onClick={() => setOpen(true)}
        data-testid="relationships-join-through"
      >
        {t("joinThrough.action", { table: plan.viaTable })}
      </Button>
      {open && (
        <JoinThroughForm
          ids={[ids[0], ids[1]]}
          plan={plan}
          onClose={() => setOpen(false)}
          onJoined={(id) => {
            setOpen(false);
            onJoined(id);
          }}
        />
      )}
    </>
  );
}

export function JoinThroughForm({
  ids,
  plan,
  onClose,
  onJoined,
}: {
  ids: readonly [string, string];
  plan: JoinThroughPlan;
  onClose: () => void;
  onJoined: (id: string) => void;
}) {
  const { t } = useTranslation();
  const { joinThrough, saving } = useJoinThrough();
  const [name, setName] = useState(plan.name);
  const [problem, setProblem] = useState<string | null>(null);

  const save = async () => {
    setProblem(null);
    const id = name.trim();
    const result = await joinThrough(ids[0], ids[1], id);
    if (!result.success) {
      setProblem(serverMessage(result, result.message));
      return;
    }
    onJoined(id);
  };

  const end = (table: string, column: string) => `${table}.${column}`;
  return (
    <ModalLayout
      opened
      onClose={onClose}
      title={t("joinThrough.title", { table: plan.viaTable })}
      data-testid="join-through-modal"
    >
      <FormLayout>
        <FormField size="full">
          <Text size="sm" c="dimmed" data-testid="join-through-shape">
            {plan.shape === "junction"
              ? t("joinThrough.junction", { table: plan.viaTable })
              : t("joinThrough.sharedDimension", { table: plan.viaTable })}
          </Text>
        </FormField>
        <FormField size="medium">
          <TextInput
            label={t("joinThrough.name")}
            required
            value={name}
            onChange={(e) => setName(e.currentTarget.value)}
            data-testid="join-through-name"
          />
        </FormField>
        <FormField size="short">
          <TextInput
            label={t("joinThrough.cardinality")}
            value={t("joinThrough.manyToMany")}
            readOnly
            data-testid="join-through-cardinality"
          />
        </FormField>
        <FormField size="medium">
          <TextInput
            label={t("joinThrough.from")}
            value={end(plan.sourceTable, plan.sourceColumn)}
            readOnly
            data-testid="join-through-from"
          />
        </FormField>
        <FormField size="medium">
          <TextInput
            label={t("joinThrough.to")}
            value={end(plan.targetTable, plan.targetColumn)}
            readOnly
            data-testid="join-through-to"
          />
        </FormField>
        <FormField size="long">
          <TextInput
            label={t("joinThrough.through")}
            value={`${plan.viaTable} (${plan.viaSourceColumn}, ${plan.viaTargetColumn})`}
            readOnly
            data-testid="join-through-via"
          />
        </FormField>
        {plan.attributes.length > 0 && (
          <FormField size="long">
            <TextInput
              label={t("joinThrough.attributes")}
              value={plan.attributes.join(", ")}
              readOnly
              data-testid="join-through-attributes"
            />
          </FormField>
        )}
        <FormFooter
          onSave={save}
          onCancel={onClose}
          saving={saving}
          saveProps={{ disabled: name.trim() === "", "data-testid": "join-through-save" }}
          cancelProps={{ "data-testid": "join-through-cancel" }}
          problem={problem}
        />
      </FormLayout>
    </ModalLayout>
  );
}
