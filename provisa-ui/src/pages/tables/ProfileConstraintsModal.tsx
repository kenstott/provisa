// Copyright (c) 2026 Kenneth Stott
// Canary: 8d3f0b72-e6a1-4c95-b2d8-7a4c1e9f0b56
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Group,
  Loader,
  Modal,
  ScrollArea,
  Select,
  Table,
  Text,
  Textarea,
  Title,
} from "@mantine/core";
import {
  decideConstraint,
  exportConstraint,
  fetchConstraints,
  fetchProfileRun,
  fetchProfileRuns,
  forgetConstraint,
  type CheckerTable,
  type ConstraintDecision,
} from "../../api/profiler";
import { useAuth } from "../../context/AuthContext";

type Proposal = Record<string, unknown>;

function columns(column: unknown, other: unknown): string {
  return other == null ? String(column) : `${String(column)} ≤ ${String(other)}`;
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// REQ-1934 PROPOSED CONSTRAINTS: the constraints the latest run proposes, each accepted (as proposed
// or edited) or dismissed here; an accepted one is checked by every later run and may be exported to
// a checker whose contract scans the table.
export function ProfileConstraintsModal({
  tableId,
  tableName,
  onClose,
}: {
  tableId: number;
  tableName: string;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const { role } = useAuth();
  const [proposals, setProposals] = useState<Proposal[] | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [decisions, setDecisions] = useState<ConstraintDecision[]>([]);
  const [checkers, setCheckers] = useState<CheckerTable[]>([]);
  const [checker, setChecker] = useState<string | null>(null);
  const [editing, setEditing] = useState<{ index: number; text: string } | null>(null);
  const [notice, setNotice] = useState<{ ok: boolean; text: string } | null>(null);

  const reload = useCallback(
    () =>
      fetchConstraints(tableId)
        .then((r) => {
          setDecisions(r.decisions);
          setCheckers(r.checkers);
        })
        .catch((e: unknown) => setNotice({ ok: false, text: message(e) })),
    [tableId],
  );

  useEffect(() => {
    void reload();
  }, [reload]);

  useEffect(() => {
    if (!role) return;
    fetchProfileRuns(tableId)
      .then(async (runs) => {
        const latest = runs.find((r) => r.status === "succeeded");
        if (!latest) {
          setProposals([]);
          return;
        }
        const results = await fetchProfileRun(tableId, latest.run_id, role.id);
        setRunId(latest.run_id);
        setProposals(results.constraints ?? []);
      })
      .catch((e: unknown) => setNotice({ ok: false, text: message(e) }));
  }, [tableId, role]);

  const decide = async (p: Proposal, status: "accepted" | "dismissed", definition?: string) => {
    if (runId == null) return;
    try {
      await decideConstraint(tableId, {
        kind: String(p.constraint),
        column: String(p.column_name),
        otherColumn: (p.other_column as string | null) ?? null,
        definition: JSON.parse(definition ?? String(p.definition)) as Record<string, unknown>,
        evidence: String(p.evidence),
        share: (p.share as number | null) ?? null,
        sampled: Boolean(p.sampled),
        status,
        runId,
      });
      setEditing(null);
      setNotice(null);
      await reload();
    } catch (e: unknown) {
      setNotice({ ok: false, text: message(e) });
    }
  };

  const decidedAs = (p: Proposal) =>
    decisions.find(
      (d) =>
        d.kind === p.constraint &&
        d.column_name === p.column_name &&
        (d.other_column ?? null) === ((p.other_column as string | null) ?? null),
    )?.status ?? null;

  const exportOne = async (d: ConstraintDecision) => {
    try {
      const r = await exportConstraint(tableId, d.id, checker == null ? null : Number(checker));
      setNotice({
        ok: true,
        text: r.added
          ? t("profileConstraintsModal.exported", { table: r.checkerTable.tableName })
          : t("profileConstraintsModal.alreadyExported", { table: r.checkerTable.tableName }),
      });
    } catch (e: unknown) {
      setNotice({ ok: false, text: message(e) });
    }
  };

  const withdraw = async (d: ConstraintDecision) => {
    try {
      await forgetConstraint(tableId, d.id);
      await reload();
    } catch (e: unknown) {
      setNotice({ ok: false, text: message(e) });
    }
  };

  return (
    <Modal
      opened
      onClose={onClose}
      size="90%"
      title={t("profileConstraintsModal.title", { table: tableName })}
      data-testid="profile-constraints-modal"
    >
      {notice && (
        <Alert color={notice.ok ? "green" : "red"} mb="xs" data-testid="constraints-notice">
          {notice.text}
        </Alert>
      )}
      <Title order={6}>{t("profileConstraintsModal.proposed")}</Title>
      {proposals == null && <Loader size="sm" />}
      {proposals != null && proposals.length === 0 && (
        <Text size="sm" c="dimmed" data-testid="constraints-none">
          {t("profileConstraintsModal.none")}
        </Text>
      )}
      {proposals != null && proposals.length > 0 && (
        <ScrollArea h={280}>
          <Table withTableBorder fz="xs" data-testid="constraints-proposed">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("profileConstraintsModal.constraint")}</Table.Th>
                <Table.Th>{t("profileConstraintsModal.columns")}</Table.Th>
                <Table.Th>{t("profileConstraintsModal.definition")}</Table.Th>
                <Table.Th>{t("profileConstraintsModal.evidence")}</Table.Th>
                <Table.Th>{t("profileConstraintsModal.share")}</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {proposals.map((p, i) => {
                const status = decidedAs(p);
                return (
                  <Table.Tr key={i} data-testid={`constraint-proposal-${i}`}>
                    <Table.Td>
                      {t(`profileConstraintsModal.kind_${String(p.constraint)}`)}
                      {p.sampled === true && (
                        <Badge ml="xs" size="xs" variant="light">
                          {t("profileConstraintsModal.sampled")}
                        </Badge>
                      )}
                    </Table.Td>
                    <Table.Td>{columns(p.column_name, p.other_column)}</Table.Td>
                    <Table.Td>
                      {editing?.index === i ? (
                        <Textarea
                          autosize
                          minRows={2}
                          value={editing.text}
                          onChange={(e) => setEditing({ index: i, text: e.currentTarget.value })}
                          data-testid={`constraint-edit-${i}`}
                        />
                      ) : (
                        <Text size="xs" style={{ wordBreak: "break-all" }}>
                          {String(p.definition)}
                        </Text>
                      )}
                    </Table.Td>
                    <Table.Td>{String(p.evidence)}</Table.Td>
                    <Table.Td>{((Number(p.share) || 0) * 100).toFixed(1)}%</Table.Td>
                    <Table.Td>
                      {status != null ? (
                        <Badge size="xs" data-testid={`constraint-status-${i}`}>
                          {t(`profileConstraintsModal.status_${status}`)}
                        </Badge>
                      ) : editing?.index === i ? (
                        <Group gap={4} wrap="nowrap">
                          <Button
                            size="compact-xs"
                            onClick={() => decide(p, "accepted", editing.text)}
                            data-testid={`constraint-save-${i}`}
                          >
                            {t("profileConstraintsModal.acceptEdited")}
                          </Button>
                          <Button
                            size="compact-xs"
                            variant="default"
                            onClick={() => setEditing(null)}
                          >
                            {t("profileConstraintsModal.cancel")}
                          </Button>
                        </Group>
                      ) : (
                        <Group gap={4} wrap="nowrap">
                          <Button
                            size="compact-xs"
                            onClick={() => decide(p, "accepted")}
                            data-testid={`constraint-accept-${i}`}
                          >
                            {t("profileConstraintsModal.accept")}
                          </Button>
                          <Button
                            size="compact-xs"
                            variant="light"
                            onClick={() => setEditing({ index: i, text: String(p.definition) })}
                            data-testid={`constraint-editbtn-${i}`}
                          >
                            {t("profileConstraintsModal.edit")}
                          </Button>
                          <Button
                            size="compact-xs"
                            variant="default"
                            onClick={() => decide(p, "dismissed")}
                            data-testid={`constraint-dismiss-${i}`}
                          >
                            {t("profileConstraintsModal.dismiss")}
                          </Button>
                        </Group>
                      )}
                    </Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </ScrollArea>
      )}
      <Title order={6} mt="md">
        {t("profileConstraintsModal.decided")}
      </Title>
      {checkers.length > 1 && (
        <Select
          size="xs"
          mt="xs"
          label={t("profileConstraintsModal.checkerLabel")}
          data={checkers.map((c) => ({ value: String(c.id), label: `${c.tableName} (${c.checker})` }))}
          value={checker}
          onChange={setChecker}
          data-testid="constraints-checker"
        />
      )}
      {decisions.length === 0 ? (
        <Text size="sm" c="dimmed" data-testid="constraints-none-decided">
          {t("profileConstraintsModal.noneDecided")}
        </Text>
      ) : (
        <Table withTableBorder fz="xs" mt="xs" data-testid="constraints-decided">
          <Table.Tbody>
            {decisions.map((d) => (
              <Table.Tr key={d.id} data-testid={`constraint-decided-${d.id}`}>
                <Table.Td>{t(`profileConstraintsModal.kind_${d.kind}`)}</Table.Td>
                <Table.Td>{columns(d.column_name, d.other_column)}</Table.Td>
                <Table.Td>
                  <Text size="xs" style={{ wordBreak: "break-all" }}>
                    {d.definition}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Badge size="xs">{t(`profileConstraintsModal.status_${d.status}`)}</Badge>
                </Table.Td>
                <Table.Td>
                  <Group gap={4} wrap="nowrap">
                    {d.status === "accepted" && (
                      <Button
                        size="compact-xs"
                        variant="light"
                        onClick={() => exportOne(d)}
                        data-testid={`constraint-export-${d.id}`}
                      >
                        {t("profileConstraintsModal.export")}
                      </Button>
                    )}
                    <Button
                      size="compact-xs"
                      variant="default"
                      onClick={() => withdraw(d)}
                      data-testid={`constraint-withdraw-${d.id}`}
                    >
                      {t("profileConstraintsModal.withdraw")}
                    </Button>
                  </Group>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Modal>
  );
}
