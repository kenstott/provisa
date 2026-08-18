// Copyright (c) 2026 Kenneth Stott
// Canary: 9d3f6b21-84ec-4a57-b0d9-2f18c7ae5406
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Checkbox,
  Code,
  FileButton,
  Group,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, TriangleAlert, Upload } from "lucide-react";
import {
  applyImport,
  fileToBase64,
  previewImport,
  type ImportFlavor,
  type ImportPreview,
} from "../../api/importer";

// REQ-1483: the interactive form of the `provisa.hasura_v2` / `provisa.ddn` converters.
//
// The flow is deliberately two-step. Converting someone's Hasura project is a guess about their
// intent in places (which schema becomes which domain, which connection a source really points
// at), so the conversion result is shown — counts, warnings and the config itself, editable —
// and nothing reaches the org until the administrator approves it.

const FLAVORS: { value: ImportFlavor; labelKey: string }[] = [
  { value: "auto", labelKey: "flavorAuto" },
  { value: "hasura_v2", labelKey: "flavorV2" },
  { value: "ddn", labelKey: "flavorDdn" },
];

export function ImportTab() {
  const { t } = useTranslation();
  const [file, setFile] = useState<File | null>(null);
  const [flavor, setFlavor] = useState<ImportFlavor>("auto");
  // Target domain per schema (v2) or subgraph (DDN) the upload turned out to carry. Seeded from the
  // first conversion, since the names cannot be known before the file is parsed.
  const [domains, setDomains] = useState<Record<string, string>>({});
  const [preview, setPreview] = useState<ImportPreview | null>(null);
  const [yamlText, setYamlText] = useState("");
  const [replace, setReplace] = useState(false);
  const [busy, setBusy] = useState("");
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  const runPreview = async (map: Record<string, string>) => {
    if (!file) return;
    setBusy("preview");
    setError("");
    setMsg("");
    setPreview(null);
    try {
      // Only renames travel to the server; a name left as it arrived is not a mapping.
      const domain_map: Record<string, string> = {};
      for (const [from, to] of Object.entries(map))
        if (to.trim() && to.trim() !== from) domain_map[from] = to.trim();
      const result = await previewImport({
        filename: file.name,
        content_b64: await fileToBase64(file),
        flavor,
        domain_map,
        source_overrides: {},
      });
      setPreview(result);
      setYamlText(result.config_yaml);
      setDomains(Object.fromEntries(result.discovered_domains.map((d) => [d, map[d] ?? d])));
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  };

  const runApply = async () => {
    setBusy("apply");
    setError("");
    setMsg("");
    try {
      const result = await applyImport(yamlText, replace);
      setMsg(
        t("importTab.applied", {
          sources: result.summary.sources,
          tables: result.summary.tables,
          roles: result.summary.roles,
        }),
      );
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  };

  const counts = preview
    ? [
        { key: "sources", value: preview.summary.sources },
        { key: "domains", value: preview.summary.domains },
        { key: "tables", value: preview.summary.tables },
        { key: "columns", value: preview.summary.columns },
        { key: "roles", value: preview.summary.roles },
        { key: "relationships", value: preview.summary.relationships },
        { key: "rlsRules", value: preview.summary.rls_rules },
      ]
    : [];

  return (
    <Stack gap="md">
      <Text size="sm" c="dimmed">
        {t("importTab.intro")}
      </Text>

      <Group align="flex-end">
        <FileButton onChange={setFile} accept=".zip,.yaml,.yml,.json,.hml">
          {(props) => (
            <Button {...props} leftSection={<Upload size={16} />} variant="light">
              {t("importTab.chooseFile")}
            </Button>
          )}
        </FileButton>
        <Select
          label={t("importTab.flavor")}
          data={FLAVORS.map((f) => ({ value: f.value, label: t(`importTab.${f.labelKey}`) }))}
          value={flavor}
          onChange={(v) => setFlavor((v as ImportFlavor) ?? "auto")}
          w={260}
        />
        <Button
          onClick={() => runPreview(domains)}
          disabled={!file}
          loading={busy === "preview"}
          aria-label={t("importTab.preview")}
        >
          {t("importTab.preview")}
        </Button>
      </Group>
      <Text size="sm">{file ? file.name : t("importTab.noFile")}</Text>

      {error && (
        <Alert color="red" icon={<TriangleAlert size={16} />}>
          {error}
        </Alert>
      )}
      {msg && (
        <Alert color="green" icon={<Check size={16} />}>
          {msg}
        </Alert>
      )}

      {preview && (
        <Stack gap="md">
          <Group>
            <Title order={4}>{t("importTab.summaryHeading")}</Title>
            <Badge>{t(`importTab.flavor_${preview.flavor}`)}</Badge>
          </Group>
          <SimpleGrid cols={{ base: 2, sm: 4 }}>
            {counts.map((c) => (
              <Stack key={c.key} gap={0}>
                <Text fw={700}>{c.value}</Text>
                <Text size="xs" c="dimmed">
                  {t(`importTab.count_${c.key}`)}
                </Text>
              </Stack>
            ))}
          </SimpleGrid>
          <Text size="sm">
            {t("importTab.sourceList")} <Code>{preview.summary.source_ids.join(", ")}</Code>
          </Text>
          <Text size="sm">
            {t("importTab.roleList")} <Code>{preview.summary.role_ids.join(", ")}</Code>
          </Text>

          {/* One row per name the upload actually carries — the conversion has to run before these
              are known, which is why the mapping lives here and re-runs it. */}
          {preview.discovered_domains.length > 0 && (
            <Stack gap="xs">
              <Text fw={500} size="sm">
                {t("importTab.domainMap")}
              </Text>
              <Text size="xs" c="dimmed">
                {t("importTab.domainMapHelp")}
              </Text>
              {preview.discovered_domains.map((from) => (
                <Group key={from} gap="xs" align="center">
                  <Code w={220}>{from}</Code>
                  <TextInput
                    w={220}
                    aria-label={t("importTab.domainTo", { from })}
                    value={domains[from] ?? from}
                    onChange={(e) => setDomains({ ...domains, [from]: e.currentTarget.value })}
                  />
                </Group>
              ))}
              <Group>
                <Button
                  variant="light"
                  onClick={() => runPreview(domains)}
                  loading={busy === "preview"}
                >
                  {t("importTab.remap")}
                </Button>
              </Group>
            </Stack>
          )}

          <Title order={4}>
            {t("importTab.warningsHeading", { count: preview.warnings.length })}
          </Title>
          {preview.warnings.length === 0 ? (
            <Text size="sm" c="dimmed">
              {t("importTab.noWarnings")}
            </Text>
          ) : (
            <Table>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("importTab.warningCategory")}</Table.Th>
                  <Table.Th>{t("importTab.warningMessage")}</Table.Th>
                  <Table.Th>{t("importTab.warningLocation")}</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {preview.warnings.map((w, i) => (
                  <Table.Tr key={i}>
                    <Table.Td>{w.category}</Table.Td>
                    <Table.Td>{w.message}</Table.Td>
                    <Table.Td>{w.source_path}</Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          )}

          <Title order={4}>{t("importTab.configHeading")}</Title>
          <Text size="xs" c="dimmed">
            {t("importTab.configHelp")}
          </Text>
          <Textarea
            aria-label={t("importTab.configHeading")}
            value={yamlText}
            onChange={(e) => setYamlText(e.currentTarget.value)}
            autosize
            minRows={12}
            maxRows={30}
            styles={{ input: { fontFamily: "monospace" } }}
          />

          <Checkbox
            label={t("importTab.replace")}
            description={t("importTab.replaceHelp")}
            checked={replace}
            onChange={(e) => setReplace(e.currentTarget.checked)}
          />
          <Group>
            <Button
              onClick={runApply}
              loading={busy === "apply"}
              color={replace ? "red" : undefined}
            >
              {t("importTab.apply")}
            </Button>
          </Group>
        </Stack>
      )}
    </Stack>
  );
}
