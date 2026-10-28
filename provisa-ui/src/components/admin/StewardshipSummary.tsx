// Copyright (c) 2026 Kenneth Stott
// Canary: 8f1d2a63-5b7e-4c90-a3d4-e6b20c9f7a15
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2144 (A STEWARD SEES THE IMPACT OF EACH ACTION): the stewardship summary -- the terms agents
// can see, the ~grounded terms each waiting on one approval, and the approved terms that reach no
// data -- and the defined, grounded drafts a steward approves together.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Anchor,
  Badge,
  Button,
  Checkbox,
  Group,
  Loader,
  Modal,
  SimpleGrid,
  Stack,
  Text,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { CheckCheck } from "lucide-react";
import {
  bulkApproveTerms,
  fetchStewardshipSummary,
  listApprovableTerms,
  type StewardshipSummary as Summary,
  type TermRef,
} from "../../api/glossary";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { TermStatusBadge } from "./TermRoleBadge";

function TermLink({
  term,
  navigateToTerm,
}: {
  term: TermRef;
  navigateToTerm: (termId: number) => void;
}) {
  return (
    <Anchor
      component="button"
      type="button"
      size="sm"
      onClick={() => navigateToTerm(term.term_id)}
      data-testid={`stewardship-term-${term.term_id}`}
    >
      {term.name}
    </Anchor>
  );
}

function Section({
  title,
  count,
  testId,
  children,
}: {
  title: string;
  count: number;
  testId: string;
  children: React.ReactNode;
}) {
  return (
    <Stack gap={6} align="flex-start" data-testid={testId}>
      <Group gap={6} wrap="nowrap">
        <Title order={6}>{title}</Title>
        <Badge size="sm" variant="light" color="gray" data-testid={`${testId}-count`}>
          {count}
        </Badge>
      </Group>
      {children}
    </Stack>
  );
}

/** The summary, read when shown and again after a bulk approval (``onApproved`` tells the page). */
export function StewardshipSummary({
  canEdit,
  navigateToTerm,
  onApproved,
}: {
  canEdit: boolean;
  navigateToTerm: (termId: number) => void;
  onApproved: () => void;
}) {
  const { t } = useTranslation();
  const [summary, setSummary] = useState<Summary | null>(null);
  const [error, setError] = useState("");
  const [seq, setSeq] = useState(0);
  const [approvable, setApprovable] = useState<TermRef[] | null>(null);
  const [picked, setPicked] = useState<number[]>([]);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    let live = true;
    fetchStewardshipSummary().then(
      (s) => live && setSummary(s),
      (e: unknown) => live && setError(e instanceof Error ? e.message : String(e)),
    );
    return () => {
      live = false;
    };
  }, [seq]);

  const openBulk = useCallback(async () => {
    setError("");
    try {
      const rows = await listApprovableTerms();
      setApprovable(rows);
      setPicked(rows.map((r) => r.term_id));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  const approve = async () => {
    setSaving(true);
    setError("");
    try {
      const { approved } = await bulkApproveTerms(picked);
      notifications.show({
        color: "green",
        message: t("glossaryTab.bulkApproveDone", { count: approved.length }),
      });
      setApprovable(null);
      setSeq((n) => n + 1);
      onApproved();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  if (summary === null) {
    return error ? (
      <Alert color="red">{error}</Alert>
    ) : (
      <Group justify="center" py="md">
        <Loader size="xs" />
      </Group>
    );
  }
  const none = (
    <Text size="xs" c="dimmed">
      {t("glossaryTab.noneListed")}
    </Text>
  );
  return (
    <Stack gap="md" pb="md" data-testid="glossary-stewardship">
      <Group justify="space-between" wrap="wrap">
        <Title order={4}>{t("glossaryTab.stewardshipTitle")}</Title>
        {canEdit && (
          <Button
            variant="light"
            leftSection={<CheckCheck size={14} />}
            onClick={() => void openBulk()}
            data-testid="glossary-bulk-approve-btn"
          >
            {t("glossaryTab.bulkApprove")}
          </Button>
        )}
      </Group>
      {error && (
        <Alert color="red" data-testid="glossary-stewardship-error">
          {error}
        </Alert>
      )}
      <SimpleGrid cols={{ base: 1, md: 3 }} spacing="lg">
        <Section
          title={t("glossaryTab.agentVisibleTitle")}
          count={summary.agent_visible.length}
          testId="stewardship-agent-visible"
        >
          {summary.agent_visible.length === 0
            ? none
            : summary.agent_visible.map((term) => (
                <Group key={term.term_id} gap={6} wrap="nowrap">
                  <TermLink term={term} navigateToTerm={navigateToTerm} />
                  {term.status === "draft" && <TermStatusBadge status="draft" />}
                </Group>
              ))}
        </Section>
        <Section
          title={t("glossaryTab.waitingTitle")}
          count={summary.waiting_on_one_approval.length}
          testId="stewardship-waiting"
        >
          {summary.waiting_on_one_approval.length === 0
            ? none
            : summary.waiting_on_one_approval.map((term) => (
                <Text key={term.term_id} size="sm">
                  <TermLink term={term} navigateToTerm={navigateToTerm} />{" "}
                  <Text span size="xs" c="dimmed">
                    {t("glossaryTab.waitingApprove", {
                      terms: term.approve.map((a) => a.name).join(", "),
                    })}
                  </Text>
                </Text>
              ))}
        </Section>
        <Section
          title={t("glossaryTab.approvedNotGroundedTitle")}
          count={summary.approved_not_grounded.length}
          testId="stewardship-approved-not-grounded"
        >
          {summary.approved_not_grounded.length === 0
            ? none
            : summary.approved_not_grounded.map((term) => (
                <TermLink key={term.term_id} term={term} navigateToTerm={navigateToTerm} />
              ))}
        </Section>
      </SimpleGrid>
      {approvable !== null && (
        <Modal
          opened
          onClose={() => setApprovable(null)}
          title={t("glossaryTab.bulkApproveTitle")}
          data-testid="glossary-bulk-approve"
        >
          <Stack gap="sm">
            {approvable.length === 0 ? (
              <Text size="sm" c="dimmed">
                {t("glossaryTab.bulkApproveNone")}
              </Text>
            ) : (
              <>
                <Text size="sm">{t("glossaryTab.bulkApproveBody")}</Text>
                <FormLayout>
                  <FormField size="full">
                    <Checkbox.Group
                      value={picked.map(String)}
                      onChange={(v) => setPicked(v.map(Number))}
                    >
                      <Stack gap={4} mah={320} style={{ overflow: "auto" }}>
                        {approvable.map((term) => (
                          <Checkbox
                            key={term.term_id}
                            value={String(term.term_id)}
                            label={term.name}
                            data-testid={`glossary-bulk-approve-${term.term_id}`}
                          />
                        ))}
                      </Stack>
                    </Checkbox.Group>
                  </FormField>
                </FormLayout>
              </>
            )}
            <FormFooter
              onCancel={() => setApprovable(null)}
              action={
                <Button
                  color="teal"
                  leftSection={<CheckCheck size={14} />}
                  disabled={picked.length === 0}
                  loading={saving}
                  onClick={() => void approve()}
                  data-testid="glossary-bulk-approve-confirm"
                >
                  {t("glossaryTab.bulkApproveConfirm", { count: picked.length })}
                </Button>
              }
            />
          </Stack>
        </Modal>
      )}
    </Stack>
  );
}
