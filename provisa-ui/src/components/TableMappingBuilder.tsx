// Copyright (c) 2026 Kenneth Stott
// Canary: 0b1ef98f-6266-47e5-a7ba-65d971c54efb
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
  ActionIcon,
  Button,
  Card,
  Checkbox,
  Group,
  Pill,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Plus, Trash2, X } from "lucide-react";

const TRINO_TYPES = [
  "VARCHAR", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
  "DOUBLE", "REAL", "DECIMAL", "BOOLEAN", "DATE",
  "TIMESTAMP", "VARBINARY", "JSON",
];

interface ColumnDef {
  name: string;
  type: string;
  field: string;      // Redis field mapping / ES dot-path / Mongo JSONPath
}

interface TableMappingBuilderProps {
  sourceType: string;
  onSave: (mapping: TableMapping) => void;
  onCancel: () => void;
}

export interface TableMapping {
  sourceType: string;
  tableName: string;
  // Redis-specific
  keyPattern?: string;
  keyColumn?: string;
  valueType?: string;
  // MongoDB-specific
  collection?: string;
  discover?: boolean;
  // Elasticsearch-specific
  indexPattern?: string;
  // Prometheus-specific
  metric?: string;
  labels?: string[];
  valueColumn?: string;
  defaultRange?: string;
  // Common
  columns: ColumnDef[];
}

function emptyColumn(): ColumnDef {
  return { name: "", type: "VARCHAR", field: "" };
}

/** Redis: key pattern, key column, value type, column rows with field mapping */
function RedisForm({ mapping, setMapping }: {
  mapping: TableMapping;
  setMapping: (m: TableMapping) => void;
}) {
  const { t } = useTranslation();
  const redisValueTypes = [
    { value: "hash", label: t("tableMappingBuilder.valueTypeHash") },
    { value: "string", label: t("tableMappingBuilder.valueTypeString") },
    { value: "zset", label: t("tableMappingBuilder.valueTypeZset") },
    { value: "list", label: t("tableMappingBuilder.valueTypeList") },
  ];
  return (
    <>
      <TextInput
        label={t("tableMappingBuilder.keyPattern")}
        value={mapping.keyPattern ?? ""}
        onChange={(e) => setMapping({ ...mapping, keyPattern: e.target.value })}
        placeholder={t("tableMappingBuilder.keyPatternPlaceholder")}
      />
      <TextInput
        label={t("tableMappingBuilder.keyColumn")}
        value={mapping.keyColumn ?? ""}
        onChange={(e) => setMapping({ ...mapping, keyColumn: e.target.value })}
        placeholder={t("tableMappingBuilder.keyColumnPlaceholder")}
      />
      <Select
        label={t("tableMappingBuilder.valueType")}
        data={redisValueTypes}
        value={mapping.valueType ?? "hash"}
        onChange={(value) => setMapping({ ...mapping, valueType: value ?? "hash" })}
        allowDeselect={false}
      />
    </>
  );
}

/** MongoDB: collection name, discover toggle, column rows with JSONPath */
function MongoForm({ mapping, setMapping }: {
  mapping: TableMapping;
  setMapping: (m: TableMapping) => void;
}) {
  const { t } = useTranslation();
  return (
    <>
      <TextInput
        label={t("tableMappingBuilder.collection")}
        value={mapping.collection ?? ""}
        onChange={(e) => setMapping({ ...mapping, collection: e.target.value })}
        placeholder={t("tableMappingBuilder.collectionPlaceholder")}
      />
      <Checkbox
        label={t("tableMappingBuilder.autoDiscoverSchema")}
        checked={mapping.discover ?? false}
        onChange={(e) => setMapping({ ...mapping, discover: e.target.checked })}
        mt="1.5rem"
      />
    </>
  );
}

/** Elasticsearch: index pattern, column rows with dot-path */
function ElasticsearchForm({ mapping, setMapping }: {
  mapping: TableMapping;
  setMapping: (m: TableMapping) => void;
}) {
  const { t } = useTranslation();
  return (
    <>
      <TextInput
        label={t("tableMappingBuilder.indexPattern")}
        value={mapping.indexPattern ?? ""}
        onChange={(e) => setMapping({ ...mapping, indexPattern: e.target.value })}
        placeholder={t("tableMappingBuilder.indexPatternPlaceholder")}
      />
      <Checkbox
        label={t("tableMappingBuilder.autoDiscoverSchema")}
        checked={mapping.discover ?? false}
        onChange={(e) => setMapping({ ...mapping, discover: e.target.checked })}
        mt="1.5rem"
      />
    </>
  );
}

/** Prometheus: metric name, label checkboxes, value column, time range */
function PrometheusForm({ mapping, setMapping }: {
  mapping: TableMapping;
  setMapping: (m: TableMapping) => void;
}) {
  const { t } = useTranslation();
  const [labelInput, setLabelInput] = useState("");

  const addLabel = () => {
    const trimmed = labelInput.trim();
    if (trimmed && !(mapping.labels ?? []).includes(trimmed)) {
      setMapping({ ...mapping, labels: [...(mapping.labels ?? []), trimmed] });
      setLabelInput("");
    }
  };

  const removeLabel = (label: string) => {
    setMapping({ ...mapping, labels: (mapping.labels ?? []).filter((l) => l !== label) });
  };

  return (
    <>
      <TextInput
        label={t("tableMappingBuilder.metricName")}
        value={mapping.metric ?? ""}
        onChange={(e) => setMapping({ ...mapping, metric: e.target.value })}
        placeholder={t("tableMappingBuilder.metricNamePlaceholder")}
      />
      <TextInput
        label={t("tableMappingBuilder.valueColumnName")}
        value={mapping.valueColumn ?? "value"}
        onChange={(e) => setMapping({ ...mapping, valueColumn: e.target.value })}
      />
      <TextInput
        label={t("tableMappingBuilder.defaultTimeRange")}
        value={mapping.defaultRange ?? "1h"}
        onChange={(e) => setMapping({ ...mapping, defaultRange: e.target.value })}
        placeholder={t("tableMappingBuilder.defaultTimeRangePlaceholder")}
      />
      <div style={{ gridColumn: "1 / -1" }}>
        <Text component="label" size="sm" fw={500}>
          {t("tableMappingBuilder.labelsAsColumns")}
        </Text>
        <Group gap="0.25rem" mb="0.25rem" mt="0.25rem">
          {(mapping.labels ?? []).map((label) => (
            <Pill
              key={label}
              withRemoveButton
              removeButtonProps={{ "aria-label": t("tableMappingBuilder.removeLabel", { label }) }}
              onRemove={() => removeLabel(label)}
            >
              {label}
            </Pill>
          ))}
        </Group>
        <Group gap="0.25rem">
          <TextInput
            value={labelInput}
            onChange={(e) => setLabelInput(e.target.value)}
            placeholder={t("tableMappingBuilder.labelPlaceholder")}
            w="10rem"
            aria-label={t("tableMappingBuilder.labelsAsColumns")}
            onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); addLabel(); } }}
          />
          <Button onClick={addLabel} size="xs" variant="default">
            {t("tableMappingBuilder.addLabel")}
          </Button>
        </Group>
      </div>
    </>
  );
}

export function TableMappingBuilder({ sourceType, onSave, onCancel }: TableMappingBuilderProps) {
  const { t } = useTranslation();

  const fieldLabel = (st: string): string => {
    switch (st) {
      case "redis": return t("tableMappingBuilder.fieldRedis");
      case "mongodb": return t("tableMappingBuilder.fieldMongo");
      case "elasticsearch": return t("tableMappingBuilder.fieldElasticsearch");
      default: return t("tableMappingBuilder.fieldDefault");
    }
  };

  const [mapping, setMapping] = useState<TableMapping>({
    sourceType,
    tableName: "",
    keyPattern: "",
    keyColumn: "",
    valueType: "hash",
    collection: "",
    discover: false,
    indexPattern: "",
    metric: "",
    labels: [],
    valueColumn: "value",
    defaultRange: "1h",
    columns: [emptyColumn()],
  });

  const addColumn = () => {
    setMapping({ ...mapping, columns: [...mapping.columns, emptyColumn()] });
  };

  const removeColumn = (idx: number) => {
    setMapping({ ...mapping, columns: mapping.columns.filter((_, i) => i !== idx) });
  };

  const updateColumn = (idx: number, patch: Partial<ColumnDef>) => {
    setMapping({
      ...mapping,
      columns: mapping.columns.map((c, i) => (i === idx ? { ...c, ...patch } : c)),
    });
  };

  const handleSave = () => {
    if (!mapping.tableName.trim()) return;
    onSave(mapping);
  };

  const typeOptions = TRINO_TYPES.map((ty) => ({ value: ty, label: ty }));

  return (
    <Card withBorder mt="1rem" data-testid="table-mapping-builder">
      <Group justify="space-between" align="center">
        <Title order={4} m={0}>
          {t("tableMappingBuilder.title", { sourceType })}
        </Title>
        <ActionIcon
          variant="subtle"
          aria-label={t("tableMappingBuilder.close")}
          onClick={onCancel}
        >
          <X size={16} />
        </ActionIcon>
      </Group>

      <SimpleGrid cols={2} spacing="0.5rem" mt="0.5rem">
        <TextInput
          required
          label={t("tableMappingBuilder.tableName")}
          value={mapping.tableName}
          onChange={(e) => setMapping({ ...mapping, tableName: e.target.value })}
          placeholder={t("tableMappingBuilder.tableNamePlaceholder")}
        />

        {sourceType === "redis" && <RedisForm mapping={mapping} setMapping={setMapping} />}
        {sourceType === "mongodb" && <MongoForm mapping={mapping} setMapping={setMapping} />}
        {sourceType === "elasticsearch" && <ElasticsearchForm mapping={mapping} setMapping={setMapping} />}
        {sourceType === "prometheus" && <PrometheusForm mapping={mapping} setMapping={setMapping} />}
      </SimpleGrid>

      {sourceType !== "prometheus" && (
        <Stack gap="0.25rem" mt="1rem">
          <Group gap="0.5rem">
            <Title order={5} m={0}>
              {t("tableMappingBuilder.columns")}
            </Title>
            <Button
              onClick={addColumn}
              size="xs"
              variant="default"
              leftSection={<Plus size={12} />}
              data-testid="add-column-button"
            >
              {t("tableMappingBuilder.addColumn")}
            </Button>
          </Group>
          <Table>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("tableMappingBuilder.columnName")}</Table.Th>
                <Table.Th>{t("tableMappingBuilder.columnType")}</Table.Th>
                <Table.Th>{fieldLabel(sourceType)}</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {mapping.columns.map((col, idx) => (
                <Table.Tr key={idx}>
                  <Table.Td>
                    <TextInput
                      aria-label={t("tableMappingBuilder.columnName")}
                      value={col.name}
                      onChange={(e) => updateColumn(idx, { name: e.target.value })}
                      placeholder={t("tableMappingBuilder.columnNamePlaceholder")}
                      w="8rem"
                    />
                  </Table.Td>
                  <Table.Td>
                    <Select
                      aria-label={t("tableMappingBuilder.columnType")}
                      data={typeOptions}
                      value={col.type}
                      onChange={(value) => updateColumn(idx, { type: value ?? "VARCHAR" })}
                      allowDeselect={false}
                    />
                  </Table.Td>
                  <Table.Td>
                    <TextInput
                      aria-label={fieldLabel(sourceType)}
                      value={col.field}
                      onChange={(e) => updateColumn(idx, { field: e.target.value })}
                      placeholder={
                        sourceType === "mongodb"
                          ? t("tableMappingBuilder.fieldPlaceholderMongo")
                          : t("tableMappingBuilder.fieldPlaceholderDefault")
                      }
                      w="10rem"
                    />
                  </Table.Td>
                  <Table.Td>
                    <ActionIcon
                      variant="subtle"
                      color="red"
                      aria-label={t("tableMappingBuilder.removeColumn", { index: idx + 1 })}
                      onClick={() => removeColumn(idx)}
                    >
                      <Trash2 size={14} />
                    </ActionIcon>
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Stack>
      )}

      <Group mt="0.75rem">
        <Button onClick={handleSave} data-testid="save-mapping-button">
          {t("tableMappingBuilder.saveMapping")}
        </Button>
      </Group>
    </Card>
  );
}
