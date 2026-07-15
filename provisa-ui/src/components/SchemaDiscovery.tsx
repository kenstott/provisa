// Copyright (c) 2026 Kenneth Stott
// Canary: a03f3a03-b6a1-4788-99c9-ce711ccd8264
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
  Button,
  Card,
  Checkbox,
  Group,
  Select,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Trash2 } from "lucide-react";
import { discoverSourceSchema, fetchIrTypes } from "../api/admin";
import type { DiscoveredColumn } from "../api/admin";
import { useRegisterTable } from "../hooks/useAdminQueries";
import { IR_TYPES_FALLBACK, toIrType } from "../irTypes";

interface ColumnRow {
  selected: boolean;
  name: string;
  type: string;
  alias: string;
  description: string;
  sourcePath: string;
}

interface SchemaDiscoveryProps {
  sourceId: string;
  sourceType: string;
  onClose: () => void;
  onRegistered?: () => void;
}

function toColumnRows(cols: DiscoveredColumn[]): ColumnRow[] {
  return cols.map((c) => ({
    selected: true,
    name: c.name,
    type: toIrType(c.type),
    alias: "",
    description: c.description,
    sourcePath: c.source_path,
  }));
}

/** Hint fields vary by source type. */
function DiscoverHints({
  sourceType,
  hints,
  setHints,
}: {
  sourceType: string;
  hints: Record<string, string>;
  setHints: (h: Record<string, string>) => void;
}) {
  const { t } = useTranslation();
  if (sourceType === "mongodb") {
    return (
      <TextInput
        label={t("schemaDiscovery.collection")}
        value={hints.collection ?? ""}
        onChange={(e) => setHints({ ...hints, collection: e.currentTarget.value })}
        placeholder={t("schemaDiscovery.collectionPlaceholder")}
      />
    );
  }
  if (sourceType === "elasticsearch") {
    return (
      <TextInput
        label={t("schemaDiscovery.indexPattern")}
        value={hints.index ?? ""}
        onChange={(e) => setHints({ ...hints, index: e.currentTarget.value })}
        placeholder={t("schemaDiscovery.indexPatternPlaceholder")}
      />
    );
  }
  if (sourceType === "cassandra") {
    return (
      <>
        <TextInput
          label={t("schemaDiscovery.keyspace")}
          value={hints.keyspace ?? ""}
          onChange={(e) => setHints({ ...hints, keyspace: e.currentTarget.value })}
          placeholder={t("schemaDiscovery.keyspacePlaceholder")}
        />
        <TextInput
          label={t("schemaDiscovery.table")}
          value={hints.table ?? ""}
          onChange={(e) => setHints({ ...hints, table: e.currentTarget.value })}
          placeholder={t("schemaDiscovery.tablePlaceholder")}
        />
      </>
    );
  }
  if (sourceType === "prometheus") {
    return (
      <TextInput
        label={t("schemaDiscovery.metricName")}
        value={hints.metric ?? ""}
        onChange={(e) => setHints({ ...hints, metric: e.currentTarget.value })}
        placeholder={t("schemaDiscovery.metricNamePlaceholder")}
      />
    );
  }
  return null;
}

export function SchemaDiscovery({
  sourceId,
  sourceType,
  onClose,
  onRegistered,
}: SchemaDiscoveryProps) {
  const { t } = useTranslation();
  const [columns, setColumns] = useState<ColumnRow[]>([]);
  // IR type vocabulary for the per-column type dropdown (REQ-846), from the backend.
  const [irTypes, setIrTypes] = useState<string[]>(IR_TYPES_FALLBACK);
  useEffect(() => {
    fetchIrTypes()
      .then((t) => {
        if (t.length > 0) setIrTypes(t);
      })
      .catch(() => setIrTypes(IR_TYPES_FALLBACK));
  }, []);
  const [hints, setHints] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [regForm, setRegForm] = useState({
    domainId: "",
    schemaName: "default",
    tableName: "",
  });
  const [registering, setRegistering] = useState(false);
  const { registerTable } = useRegisterTable();

  const handleDiscover = async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await discoverSourceSchema(sourceId, hints);
      setColumns(toColumnRows(resp.columns));
      if (resp.columns.length === 0) {
        setError(t("schemaDiscovery.noColumnsDiscovered"));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const toggleAll = (checked: boolean) => {
    setColumns(columns.map((c) => ({ ...c, selected: checked })));
  };

  const updateColumn = (idx: number, patch: Partial<ColumnRow>) => {
    setColumns(columns.map((c, i) => (i === idx ? { ...c, ...patch } : c)));
  };

  const addManualColumn = () => {
    setColumns([
      ...columns,
      {
        selected: true,
        name: "",
        type: "text",
        alias: "",
        description: "",
        sourcePath: "",
      },
    ]);
  };

  const removeColumn = (idx: number) => {
    setColumns(columns.filter((_, i) => i !== idx));
  };

  const handleRegister = async () => {
    const selected = columns.filter((c) => c.selected && c.name.trim());
    if (selected.length === 0) {
      setError(t("schemaDiscovery.selectColumnRequired"));
      return;
    }
    if (!regForm.domainId.trim() || !regForm.tableName.trim()) {
      setError(t("schemaDiscovery.domainTableRequired"));
      return;
    }
    setRegistering(true);
    setError(null);
    try {
      const result = await registerTable({
        sourceId,
        domainId: regForm.domainId,
        schemaName: regForm.schemaName,
        tableName: regForm.tableName,
        columns: selected.map((c) => ({
          name: c.alias || c.name,
          visibleTo: ["*"],
          alias: c.alias || undefined,
          description: c.description || undefined,
          // Persist the steward's assigned canonical IR type (REQ-846).
          dataType: c.type || undefined,
        })),
      });
      if (!result.success) throw new Error(result.message);
      onRegistered?.();
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRegistering(false);
    }
  };

  const allSelected = columns.length > 0 && columns.every((c) => c.selected);
  const irTypeOptions = irTypes.map((it) => ({ value: it, label: it }));

  return (
    <Card withBorder mt="md" data-testid="schema-discovery">
      <Group justify="space-between" align="center">
        <Title order={4} m={0}>
          {t("schemaDiscovery.title", { sourceId })}
        </Title>
        <Button variant="default" size="xs" onClick={onClose}>
          {t("schemaDiscovery.close")}
        </Button>
      </Group>

      {error && (
        <Alert color="red" mt="sm" data-testid="schema-discovery-error">
          {error}
        </Alert>
      )}

      <Group gap="sm" wrap="wrap" align="flex-end" mt="sm">
        <DiscoverHints sourceType={sourceType} hints={hints} setHints={setHints} />
        <Button onClick={handleDiscover} loading={loading} data-testid="discover-schema-btn">
          {loading ? t("schemaDiscovery.discovering") : t("schemaDiscovery.discover")}
        </Button>
        <Button variant="light" size="xs" onClick={addManualColumn} data-testid="add-column-btn">
          {t("schemaDiscovery.addColumn")}
        </Button>
      </Group>

      {columns.length > 0 && (
        <>
          <Table.ScrollContainer minWidth={720} mt="md">
            <Table withTableBorder verticalSpacing="xs">
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>
                    <Checkbox
                      checked={allSelected}
                      onChange={(e) => toggleAll(e.currentTarget.checked)}
                      aria-label={t("schemaDiscovery.selectAllColumns")}
                    />
                  </Table.Th>
                  <Table.Th>{t("schemaDiscovery.colName")}</Table.Th>
                  <Table.Th>{t("schemaDiscovery.colType")}</Table.Th>
                  <Table.Th>{t("schemaDiscovery.colAlias")}</Table.Th>
                  <Table.Th>{t("schemaDiscovery.colDescription")}</Table.Th>
                  <Table.Th>{t("schemaDiscovery.colSourcePath")}</Table.Th>
                  <Table.Th>
                    <Text span visibleFrom="xs" fz="sm" fw={600}>
                      {t("schemaDiscovery.colActions")}
                    </Text>
                  </Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {columns.map((col, idx) => (
                  <Table.Tr key={idx} opacity={col.selected ? 1 : 0.5}>
                    <Table.Td>
                      <Checkbox
                        checked={col.selected}
                        onChange={(e) => updateColumn(idx, { selected: e.currentTarget.checked })}
                        aria-label={t("schemaDiscovery.selectColumn", {
                          name: col.name || idx + 1,
                        })}
                      />
                    </Table.Td>
                    <Table.Td>
                      <TextInput
                        aria-label={t("schemaDiscovery.colName")}
                        value={col.name}
                        onChange={(e) => updateColumn(idx, { name: e.currentTarget.value })}
                        placeholder={t("schemaDiscovery.namePlaceholder")}
                        w="8rem"
                      />
                    </Table.Td>
                    <Table.Td>
                      <Select
                        aria-label={t("schemaDiscovery.colType")}
                        data={irTypeOptions}
                        value={col.type}
                        onChange={(v) => v && updateColumn(idx, { type: v })}
                        allowDeselect={false}
                        w="9rem"
                      />
                    </Table.Td>
                    <Table.Td>
                      <TextInput
                        aria-label={t("schemaDiscovery.colAlias")}
                        value={col.alias}
                        onChange={(e) => updateColumn(idx, { alias: e.currentTarget.value })}
                        placeholder={t("schemaDiscovery.aliasOptional")}
                        w="7rem"
                      />
                    </Table.Td>
                    <Table.Td>
                      <TextInput
                        aria-label={t("schemaDiscovery.colDescription")}
                        value={col.description}
                        onChange={(e) =>
                          updateColumn(idx, { description: e.currentTarget.value })
                        }
                        placeholder={t("schemaDiscovery.descriptionOptional")}
                        w="10rem"
                      />
                    </Table.Td>
                    <Table.Td c="dimmed" fz="xs">
                      {col.sourcePath}
                    </Table.Td>
                    <Table.Td>
                      <ActionIcon
                        variant="subtle"
                        color="red"
                        aria-label={t("schemaDiscovery.removeColumn", {
                          name: col.name || idx + 1,
                        })}
                        onClick={() => removeColumn(idx)}
                      >
                        <Trash2 size={14} />
                      </ActionIcon>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>

          <Title order={5} mt="md" mb="xs">
            {t("schemaDiscovery.registerTableHeading")}
          </Title>
          <Group gap="sm" wrap="wrap" align="flex-end">
            <TextInput
              label={t("schemaDiscovery.domainId")}
              required
              value={regForm.domainId}
              onChange={(e) => setRegForm({ ...regForm, domainId: e.currentTarget.value })}
              placeholder={t("schemaDiscovery.domainIdPlaceholder")}
            />
            <TextInput
              label={t("schemaDiscovery.schema")}
              value={regForm.schemaName}
              onChange={(e) => setRegForm({ ...regForm, schemaName: e.currentTarget.value })}
            />
            <TextInput
              label={t("schemaDiscovery.tableName")}
              required
              value={regForm.tableName}
              onChange={(e) => setRegForm({ ...regForm, tableName: e.currentTarget.value })}
              placeholder={t("schemaDiscovery.tableNamePlaceholder")}
            />
            <Button
              onClick={handleRegister}
              loading={registering}
              data-testid="register-table-btn"
            >
              {registering ? t("schemaDiscovery.registering") : t("schemaDiscovery.register")}
            </Button>
          </Group>
        </>
      )}
    </Card>
  );
}
