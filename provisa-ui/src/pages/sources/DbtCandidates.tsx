// Copyright (c) 2026 Kenneth Stott
// Canary: 19c7e4d2-0b83-4a6f-92de-7fa1c5b3e048
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2228: the models a dbt source offers as data products. A steward decides each one.

import { Button, Select, Text, TextInput } from "@mantine/core";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { useRoles } from "../../hooks/useAdminQueries";
import { requestFailed, serverMessage } from "../../i18n/serverMessage";
import type { DbtCandidate } from "./dbt";

async function readJson<T>(resp: Response, op: string): Promise<T> {
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed(op, resp.status)));
  }
  return (await resp.json()) as T;
}

const messageOf = (err: unknown) => (err instanceof Error ? err.message : String(err));

const fetchCandidates = async (sourceId: string) =>
  readJson<DbtCandidate[]>(
    await fetch(`/admin/dbt/candidates/${encodeURIComponent(sourceId)}`),
    "dbt candidates",
  );

function CandidateRow({
  candidate,
  sourceId,
  onAccepted,
}: {
  candidate: DbtCandidate;
  sourceId: string;
  onAccepted: () => Promise<void>;
}) {
  const { t } = useTranslation();
  const { roles } = useRoles();
  const [productId, setProductId] = useState(candidate.suggested_id);
  const [ownerRole, setOwnerRole] = useState(candidate.owner_role ?? "");
  const [accepting, setAccepting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const accept = async () => {
    setAccepting(true);
    setError(null);
    try {
      await readJson(
        await fetch(`/admin/dbt/candidates/${encodeURIComponent(sourceId)}/accept`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            unique_id: candidate.unique_id,
            product_id: productId.trim(),
            owner_role: ownerRole,
          }),
        }),
        "dbt candidate",
      );
      await onAccepted();
    } catch (err) {
      setError(messageOf(err));
    } finally {
      setAccepting(false);
    }
  };

  const columns = candidate.columns
    .map((c) => (c.required ? `${c.name} (${t("dbtFormSection.candidateRequired")})` : c.name))
    .join(", ");
  const tests = candidate.tests
    .map((test) => (test.column ? `${test.name}(${test.column})` : test.name))
    .join(", ");

  return (
    <>
      <FormField size="full">
        <Text fw={600} size="sm" data-testid="dbt-candidate-name">
          {candidate.name}
        </Text>
        <Text size="sm" c="dimmed" data-testid="dbt-candidate-facts">
          {candidate.version && (
            <>
              {t("dbtFormSection.candidateVersion", { version: candidate.version })}
              <br />
            </>
          )}
          {candidate.owner && (
            <>
              {t("dbtFormSection.candidateOwner", {
                owner: candidate.owner.name ?? candidate.owner.email,
              })}
              <br />
            </>
          )}
          {t("dbtFormSection.candidateColumns", { columns })}
          {tests && (
            <>
              <br />
              {t("dbtFormSection.candidateTests", { tests })}
            </>
          )}
        </Text>
      </FormField>
      {candidate.products.length > 0 ? (
        <FormField size="full">
          <Text size="sm" data-testid="dbt-candidate-became">
            {t("dbtFormSection.candidateBecame", { product: candidate.products[0] })}
          </Text>
        </FormField>
      ) : candidate.table_id === null ? (
        <FormField size="full">
          <Text size="sm" data-testid="dbt-candidate-register-first">
            {t("dbtFormSection.candidateRegisterFirst")}
          </Text>
        </FormField>
      ) : (
        <>
          <FormField size="medium">
            <TextInput
              required
              label={t("dbtFormSection.candidateProductId")}
              value={productId}
              onChange={(e) => setProductId(e.target.value)}
              data-testid="dbt-candidate-id"
            />
          </FormField>
          <FormField size="medium">
            <Select
              label={t("dbtFormSection.candidateOwnerRole")}
              value={ownerRole || null}
              onChange={(v) => setOwnerRole(v ?? "")}
              data={roles.map((r) => r.id)}
              searchable
              clearable
              data-testid="dbt-candidate-owner"
            />
          </FormField>
          <FormField size="full">
            <Button
              type="button"
              variant="default"
              loading={accepting}
              disabled={!productId.trim()}
              onClick={() => void accept()}
              data-testid="dbt-candidate-accept"
            >
              {t("dbtFormSection.candidateAccept")}
            </Button>
            {error && (
              <Text c="red" size="sm" mt="xs" data-testid="dbt-candidate-error">
                {error}
              </Text>
            )}
          </FormField>
        </>
      )}
    </>
  );
}

export function DbtCandidates({ sourceId }: { sourceId: string }) {
  const { t } = useTranslation();
  const [candidates, setCandidates] = useState<DbtCandidate[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let current = true;
    fetchCandidates(sourceId)
      .then((held) => {
        if (current) setCandidates(held);
      })
      .catch((err: unknown) => {
        if (current) setError(messageOf(err));
      });
    return () => {
      current = false;
    };
  }, [sourceId]);

  const reload = async () => setCandidates(await fetchCandidates(sourceId));

  return (
    <>
      <FormField size="full">
        <Text fw={600} size="sm">
          {t("dbtFormSection.candidatesTitle")}
        </Text>
        {error && (
          <Text c="red" size="sm" data-testid="dbt-candidates-error">
            {error}
          </Text>
        )}
        {candidates?.length === 0 && (
          <Text size="sm" c="dimmed" data-testid="dbt-candidates-none">
            {t("dbtFormSection.candidatesNone")}
          </Text>
        )}
        {candidates && candidates.length > 0 && (
          <Text size="sm" c="dimmed">
            {t("dbtFormSection.candidatesIntro")}
          </Text>
        )}
      </FormField>
      {candidates?.map((c) => (
        <CandidateRow key={c.unique_id} candidate={c} sourceId={sourceId} onAccepted={reload} />
      ))}
    </>
  );
}
