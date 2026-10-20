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
  Autocomplete,
  Badge,
  Button,
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
  type SourceOverride,
} from "../../api/importer";
import { useDomains } from "../../hooks/useAdminQueries";
import { FieldGroup, FormField, FormLayout } from "../form/FormLayout";
import { assignableDomainIds } from "../../lib/internalDomains";

const EMPTY_OVERRIDE: SourceOverride = {
  host: "",
  port: "",
  database: "",
  username: "",
  password: "",
};

/** REQ-1687: only what the administrator filled in travels; a blank field keeps the conversion's guess. */
function overridesToSend(
  sources: Record<string, SourceOverride>,
): Record<string, Record<string, string | number>> {
  const out: Record<string, Record<string, string | number>> = {};
  for (const [id, o] of Object.entries(sources)) {
    const fields: Record<string, string | number> = {};
    if (o.host.trim()) fields.host = o.host.trim();
    if (o.port.trim()) fields.port = Number(o.port);
    if (o.database.trim()) fields.database = o.database.trim();
    if (o.username.trim()) fields.username = o.username.trim();
    if (o.password) fields.password = o.password;
    if (Object.keys(fields).length > 0) out[id] = fields;
  }
  return out;
}

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
  // REQ-1687: connection per discovered SQL source, seeded from what the conversion guessed. The
  // preview is design time: with a reachable connection it types every column from the source.
  const [sources, setSources] = useState<Record<string, SourceOverride>>({});
  const { domains: existingDomains } = useDomains();
  const existingDomainIds = assignableDomainIds(existingDomains.map((d) => d.id));
  const [preview, setPreview] = useState<ImportPreview | null>(null);
  const [yamlText, setYamlText] = useState("");
  const [busy, setBusy] = useState("");
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  const runPreview = async (
    map: Record<string, string>,
    overrides: Record<string, SourceOverride> = sources,
  ) => {
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
        source_overrides: overridesToSend(overrides),
      });
      setPreview(result);
      setYamlText(result.config_yaml);
      setDomains(Object.fromEntries(result.discovered_domains.map((d) => [d, map[d] ?? d])));
      setSources(
        Object.fromEntries(
          (result.discovered_sources ?? []).map((s) => [
            s.id,
            overrides[s.id] ?? {
              ...EMPTY_OVERRIDE,
              host: s.host,
              port: s.port ? String(s.port) : "",
              database: s.database,
              username: s.username,
            },
          ]),
        ),
      );
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
      const result = await applyImport(yamlText);
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

      <FormLayout>
        <FieldGroup row>
          <FileButton onChange={setFile} accept=".zip,.yaml,.yml,.json,.hml">
            {(props) => (
              <Button {...props} leftSection={<Upload size={16} />} variant="light">
                {t("importTab.chooseFile")}
              </Button>
            )}
          </FileButton>
          <FormField size="medium">
            <Select
              label={t("importTab.flavor")}
              data={FLAVORS.map((f) => ({ value: f.value, label: t(`importTab.${f.labelKey}`) }))}
              value={flavor}
              onChange={(v) => setFlavor((v as ImportFlavor) ?? "auto")}
            />
          </FormField>
          <Button
            onClick={() => runPreview(domains)}
            disabled={!file}
            loading={busy === "preview"}
            aria-label={t("importTab.preview")}
          >
            {t("importTab.preview")}
          </Button>
        </FieldGroup>
      </FormLayout>
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
              <FormLayout>
                {preview.discovered_domains.map((from) => {
                  const to = domains[from] ?? from;
                  const isNew = to.trim() !== "" && !existingDomainIds.includes(to.trim());
                  return (
                    <FieldGroup row key={from} style={{ alignItems: "center" }}>
                      <Code w={220}>{from}</Code>
                      {/* Existing domains are offered; a name not among them creates a domain. */}
                      <FormField size="medium">
                        <Autocomplete
                          aria-label={t("importTab.domainTo", { from })}
                          data={existingDomainIds}
                          value={to}
                          onChange={(v) => setDomains({ ...domains, [from]: v })}
                          data-testid={`import-domain-${from}`}
                        />
                      </FormField>
                      {isNew && (
                        <Badge variant="light" data-testid={`import-domain-new-${from}`}>
                          {t("importTab.domainNew")}
                        </Badge>
                      )}
                    </FieldGroup>
                  );
                })}
              </FormLayout>
            </Stack>
          )}

          {/* REQ-1687: the export names its databases by environment variable; the connection is
              the administrator's to supply, and with it the preview types every column. */}
          {(preview.discovered_sources ?? []).length > 0 && (
            <Stack gap="xs">
              <Text fw={500} size="sm">
                {t("importTab.sourceConnections")}
              </Text>
              <Text size="xs" c="dimmed">
                {t("importTab.sourceConnectionsHelp")}
              </Text>
              <FormLayout>
                {(preview.discovered_sources ?? []).map((s) => {
                  const o = sources[s.id] ?? EMPTY_OVERRIDE;
                  const set = (patch: Partial<SourceOverride>) =>
                    setSources({ ...sources, [s.id]: { ...o, ...patch } });
                  return (
                    <FieldGroup row key={s.id} style={{ alignItems: "flex-end" }}>
                      <Code w={140}>
                        {s.id} ({s.type})
                      </Code>
                      <FormField size="medium">
                        <TextInput
                          label={t("importTab.host")}
                          value={o.host}
                          onChange={(e) => set({ host: e.currentTarget.value })}
                          data-testid={`import-source-host-${s.id}`}
                        />
                      </FormField>
                      <FormField size="short">
                        <TextInput
                          label={t("importTab.port")}
                          value={o.port}
                          onChange={(e) => set({ port: e.currentTarget.value })}
                          data-testid={`import-source-port-${s.id}`}
                        />
                      </FormField>
                      <FormField size="medium">
                        <TextInput
                          label={t("importTab.database")}
                          value={o.database}
                          onChange={(e) => set({ database: e.currentTarget.value })}
                          data-testid={`import-source-database-${s.id}`}
                        />
                      </FormField>
                      <FormField size="medium">
                        <TextInput
                          label={t("importTab.username")}
                          value={o.username}
                          onChange={(e) => set({ username: e.currentTarget.value })}
                          data-testid={`import-source-username-${s.id}`}
                        />
                      </FormField>
                      <FormField size="medium">
                        <TextInput
                          label={t("importTab.password")}
                          type="password"
                          value={o.password}
                          onChange={(e) => set({ password: e.currentTarget.value })}
                          data-testid={`import-source-password-${s.id}`}
                        />
                      </FormField>
                    </FieldGroup>
                  );
                })}
              </FormLayout>
            </Stack>
          )}

          {(preview.discovered_domains.length > 0 ||
            (preview.discovered_sources ?? []).length > 0) && (
            <Group>
              <Button
                variant="light"
                onClick={() => runPreview(domains, sources)}
                loading={busy === "preview"}
                data-testid="import-reconvert"
              >
                {t("importTab.remap")}
              </Button>
            </Group>
          )}

          <ReportSection
            testId="import-carried"
            heading={t("importTab.carriedHeading", { count: preview.carried?.length ?? 0 })}
            empty={t("importTab.noneCarried")}
            detailLabel={t("importTab.carriedAs")}
            rows={(preview.carried ?? []).map((c) => [c.category, c.item, c.detail])}
          />
          <ReportSection
            testId="import-mapped"
            heading={t("importTab.mappedHeading", { count: preview.mapped?.length ?? 0 })}
            empty={t("importTab.noneMapped")}
            detailLabel={t("importTab.mappedTo")}
            rows={(preview.mapped ?? []).map((m) => [m.category, m.item, m.detail])}
          />
          <ReportSection
            testId="import-warnings"
            heading={t("importTab.warningsHeading", { count: preview.warnings.length })}
            empty={t("importTab.noWarnings")}
            detailLabel={t("importTab.warningMessage")}
            rows={preview.warnings.map((w) => [w.category, w.source_path, w.message])}
            itemLabel={t("importTab.warningLocation")}
          />

          <Title order={4}>{t("importTab.configHeading")}</Title>
          <Text size="xs" c="dimmed">
            {t("importTab.configHelp")}
          </Text>
          <FormLayout>
            <FormField size="full">
              <Textarea
                aria-label={t("importTab.configHeading")}
                value={yamlText}
                onChange={(e) => setYamlText(e.currentTarget.value)}
                autosize
                minRows={12}
                maxRows={30}
                styles={{ input: { fontFamily: "monospace" } }}
              />
            </FormField>
          </FormLayout>

          <Group>
            <Button onClick={runApply} loading={busy === "apply"}>
              {t("importTab.apply")}
            </Button>
          </Group>
        </Stack>
      )}
    </Stack>
  );
}

/** One part of the import report: what each object of the upload became. */
function ReportSection({
  testId,
  heading,
  empty,
  detailLabel,
  itemLabel,
  rows,
}: {
  testId: string;
  heading: string;
  empty: string;
  detailLabel: string;
  itemLabel?: string;
  rows: [string, string, string][];
}) {
  const { t } = useTranslation();
  // A warning names where in the upload it arose only sometimes; no column of blanks.
  const showItem = rows.some(([, item]) => item);
  return (
    <Stack gap="xs" data-testid={testId}>
      <Title order={4}>{heading}</Title>
      {rows.length === 0 ? (
        <Text size="sm" c="dimmed">
          {empty}
        </Text>
      ) : (
        <Table>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("importTab.warningCategory")}</Table.Th>
              {showItem && <Table.Th>{itemLabel ?? t("importTab.reportItem")}</Table.Th>}
              <Table.Th>{detailLabel}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.map(([category, item, detail], i) => (
              <Table.Tr key={i}>
                <Table.Td>{category}</Table.Td>
                {showItem && <Table.Td>{item}</Table.Td>}
                <Table.Td>{detail}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}
