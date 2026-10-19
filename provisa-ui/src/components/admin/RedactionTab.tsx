// Copyright (c) 2026 Kenneth Stott
// Canary: 09b914e2-6c55-4353-9075-fbb37a544c16
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  SegmentedControl,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Plus, Trash2 } from "lucide-react";
import {
  fetchRedaction,
  saveRedactionRules,
  type RedactionRule,
  type RedactionState,
} from "../../api/admin";
import { FormFooter } from "../form/FormLayout";

type Kind = "pattern" | "reference";

interface Row {
  name: string;
  kind: Kind;
  value: string;
}

const toRows = (rules: RedactionRule[]): Row[] =>
  rules.map((r) => ({
    name: r.name,
    kind: r.reference !== null ? "reference" : "pattern",
    value: r.reference ?? r.pattern ?? "",
  }));

const toRules = (rows: Row[]): RedactionRule[] =>
  rows.map((r) => ({
    name: r.name.trim(),
    pattern: r.kind === "pattern" ? r.value : null,
    reference: r.kind === "reference" ? r.value.trim() : null,
  }));

/** One editable rule list: the org's, or the deployment's for a caller with platform_settings. */
function RulesEditor({
  scope,
  rules,
  editable,
  onSaved,
}: {
  scope: "org" | "deployment";
  rules: RedactionRule[];
  editable: boolean;
  onSaved: (s: RedactionState) => void;
}) {
  const { t } = useTranslation();
  const [rows, setRows] = useState<Row[]>(() => toRows(rules));
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  const change = (i: number, patch: Partial<Row>) =>
    setRows((rs) => rs.map((r, j) => (j === i ? { ...r, ...patch } : r)));

  const save = async () => {
    setSaving(true);
    setMsg("");
    setError("");
    try {
      onSaved(await saveRedactionRules(scope, toRules(rows)));
      setMsg(t("redactionTab.saved"));
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (!editable) {
    return rules.length === 0 ? (
      <Text size="sm" c="dimmed" data-testid={`redaction-${scope}-empty`}>
        {t("redactionTab.none")}
      </Text>
    ) : (
      <Table data-testid={`redaction-${scope}-table`}>
        <Table.Tbody>
          {rules.map((r) => (
            <Table.Tr key={r.name}>
              <Table.Td>{r.name}</Table.Td>
              <Table.Td ff="monospace">{r.reference ?? r.pattern}</Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    );
  }

  return (
    <Stack gap="xs" data-testid={`redaction-${scope}-editor`}>
      {rows.length === 0 && (
        <Text size="sm" c="dimmed">
          {t("redactionTab.none")}
        </Text>
      )}
      {rows.map((r, i) => (
        <Group key={i} gap="xs" align="flex-end" wrap="nowrap">
          <TextInput
            label={i === 0 ? t("redactionTab.name") : undefined}
            value={r.name}
            onChange={(e) => change(i, { name: e.currentTarget.value })}
            w={200}
            data-testid={`redaction-${scope}-name-${i}`}
          />
          <SegmentedControl
            value={r.kind}
            onChange={(v) => change(i, { kind: v as Kind })}
            data={[
              { value: "pattern", label: t("redactionTab.kindPattern") },
              { value: "reference", label: t("redactionTab.kindReference") },
            ]}
            data-testid={`redaction-${scope}-kind-${i}`}
          />
          <TextInput
            label={i === 0 ? t("redactionTab.value") : undefined}
            value={r.value}
            placeholder={
              r.kind === "pattern"
                ? t("redactionTab.patternPlaceholder")
                : t("redactionTab.referencePlaceholder")
            }
            onChange={(e) => change(i, { value: e.currentTarget.value })}
            ff="monospace"
            style={{ flex: 1, maxWidth: 560 }}
            data-testid={`redaction-${scope}-value-${i}`}
          />
          <ActionIcon
            variant="subtle"
            color="red"
            aria-label={t("redactionTab.remove")}
            onClick={() => setRows((rs) => rs.filter((_, j) => j !== i))}
            data-testid={`redaction-${scope}-remove-${i}`}
          >
            <Trash2 size={16} />
          </ActionIcon>
        </Group>
      ))}
      <FormFooter>
        <Button
          variant="default"
          leftSection={<Plus size={14} />}
          onClick={() => setRows((rs) => [...rs, { name: "", kind: "pattern", value: "" }])}
          data-testid={`redaction-${scope}-add`}
        >
          {t("redactionTab.add")}
        </Button>
        <Button onClick={save} loading={saving} data-testid={`redaction-${scope}-save`}>
          {t("redactionTab.save")}
        </Button>
        {msg && <Text c="green">{msg}</Text>}
        {error && <Text c="red">{error}</Text>}
      </FormFooter>
    </Stack>
  );
}

/**
 * REQ-2087: the one list of redaction rules applied to spans, logs, audit text and error
 * messages. The built-ins are a starter set; the org adds patterns for its own sensitive formats.
 * The automatic rule's secrets are shown by name only.
 */
export function RedactionTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<RedactionState | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchRedaction()
      .then(setS)
      .catch((e) => setError(String(e)));
  }, []);

  if (error && !s) return <Alert color="red">{error}</Alert>;
  if (!s) return <Text>{t("redactionTab.loading")}</Text>;

  return (
    <Stack gap="md" data-testid="redaction">
      <Text c="dimmed" size="sm">
        {t("redactionTab.intro")}
      </Text>
      <Alert color="yellow" variant="light" data-testid="redaction-best-effort">
        {t("redactionTab.bestEffort")}
      </Alert>

      {s.unresolved.length > 0 && (
        <Alert color="red" variant="light" data-testid="redaction-unresolved">
          {t("redactionTab.unresolved", { names: s.unresolved.join(", ") })}
        </Alert>
      )}

      <Title order={5}>{t("redactionTab.orgTitle")}</Title>
      {/* Keyed on the saved rules, so a save starts the editor again from what the server holds. */}
      <RulesEditor key={JSON.stringify(s.org)} scope="org" rules={s.org} editable onSaved={setS} />

      <Title order={5}>{t("redactionTab.deploymentTitle")}</Title>
      <RulesEditor
        key={JSON.stringify(s.deployment)}
        scope="deployment"
        rules={s.deployment}
        editable={s.can_edit_deployment}
        onSaved={setS}
      />

      <Title order={5}>{t("redactionTab.automaticTitle")}</Title>
      <Text size="sm" c="dimmed">
        {t("redactionTab.automaticHelp", { count: s.resolved_at_use })}
      </Text>
      <Group gap="xs" data-testid="redaction-automatic">
        {s.automatic.length === 0 ? (
          <Text size="sm" c="dimmed">
            {t("redactionTab.none")}
          </Text>
        ) : (
          s.automatic.map((name) => (
            <Badge key={name} variant="light" tt="none">
              {name}
            </Badge>
          ))
        )}
      </Group>

      <Title order={5}>{t("redactionTab.builtinTitle")}</Title>
      <Table data-testid="redaction-builtins">
        <Table.Tbody>
          {s.builtins.map((r) => (
            <Table.Tr key={r.name}>
              <Table.Td>{r.name}</Table.Td>
              <Table.Td ff="monospace" style={{ wordBreak: "break-all" }}>
                {r.pattern}
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Stack>
  );
}
