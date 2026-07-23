// Copyright (c) 2026 Kenneth Stott
// Canary: 938fb033-8525-4c25-a0fb-cab187faba70
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Button,
  Group,
  MultiSelect,
  SegmentedControl,
  Select,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import { Trash2 } from "lucide-react";
import { useRegisterEntity, useRegisterFact, useTables } from "../../hooks/useAdminQueries";
import type { Domain, RegisteredTable, TableColumn } from "../../types/admin";
import { FieldLabel } from "./FieldLabel";
import { normalizeDomain } from "./helpers";

// REQ-1164: declarative entity/fact modeling. The form collects the spec and the backend lowers it
// to the underlying MV (+ relationships) — the star dimension/fact or Data Vault hub/satellite/link.

// A key must support exact equality — the delta anti-join and snapshot dedup partition on it. Approximate
// numerics, JSON, and binary columns are excluded from the key picker (equality is unreliable/meaningless).
const _t = (c: TableColumn) => (c.dataType ?? "").toLowerCase();
const _KEY_INELIGIBLE = /(float|double|real|json|jsonb|blob|bytea|array|geometry|geography)/;
const _NUMERIC = /(int|numeric|decimal|double|real|float|number|money)/;
const _isKeyable = (c: TableColumn) => !_KEY_INELIGIBLE.test(_t(c));
const _isNumeric = (c: TableColumn) => _NUMERIC.test(_t(c));

const _AGGS = ["sum", "avg", "min", "max", "count"] as const;

// User-defined tables only — exclude Provisa's own system sources/domains from the source picker.
// Mirrors provisa/core/config_loader.py::_SYSTEM_SOURCE_IDS and domain_policy._SYSTEM_DOMAIN_IDS.
const _SYSTEM_SOURCE_IDS = new Set(["provisa-admin", "provisa-otel", "__provisa__"]);
const _SYSTEM_DOMAIN_IDS = new Set(["", "meta", "ops"]);
const _isUserTable = (tbl: RegisteredTable) =>
  !_SYSTEM_SOURCE_IDS.has(tbl.sourceId) && !_SYSTEM_DOMAIN_IDS.has(tbl.domainId);

interface ModelingFormProps {
  domains: Domain[];
  onSuccess: () => void;
  onCancel: () => void;
}

interface DimRow {
  entity: string;
  via: string;
}

export function ModelingForm({ domains, onSuccess, onCancel }: ModelingFormProps) {
  const { t } = useTranslation();
  const { registerEntity } = useRegisterEntity();
  const { registerFact } = useRegisterFact();
  const { tables } = useTables();
  const [kind, setKind] = useState<"entity" | "fact">("entity");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // On success the dialog switches to a confirmation panel (what was created + how to revise it)
  // rather than silently closing — the modeling form is create-only, so the user needs to know the
  // result is now an ordinary table edited elsewhere. null = still editing the form.
  const [created, setCreated] = useState<{
    kind: "entity" | "fact";
    name: string;
    domainId: string;
    historized: boolean;
    dimCount: number;
  } | null>(null);

  const [name, setName] = useState("");
  const [source, setSource] = useState<string | null>(null);
  const [domainId, setDomainId] = useState("");
  // entity
  const [key, setKey] = useState<string[]>([]);
  const [attributes, setAttributes] = useState<string[]>([]);
  const [history, setHistory] = useState("none");
  // fact
  const [grain, setGrain] = useState<string[]>([]);
  const [measures, setMeasures] = useState<Record<string, string>>({}); // column -> agg
  const [dimensions, setDimensions] = useState<DimRow[]>([]);

  // Source relations and their columns come from the user-defined registered tables, referenced as
  // {domain}.{table_name} — the domain-namespaced query-surface name the view lowers to.
  const userTables = useMemo(() => tables.filter(_isUserTable), [tables]);
  const _ref = (tbl: RegisteredTable) => `${normalizeDomain(tbl.domainId)}.${tbl.tableName}`;
  const sourceOptions = useMemo(
    () => userTables.map((tbl) => ({ value: _ref(tbl), label: _ref(tbl) })),
    [userTables],
  );
  const selectedTable = useMemo(
    () => userTables.find((tbl) => _ref(tbl) === source) ?? null,
    [userTables, source],
  );
  const columns = selectedTable?.columns ?? [];
  const entityOptions = useMemo(
    () => userTables.map((tbl) => ({ value: tbl.tableName, label: tbl.tableName })),
    [userTables],
  );

  const colLabel = (c: TableColumn) => (c.dataType ? `${c.columnName} (${c.dataType})` : c.columnName);
  const allColumnOptions = columns.map((c) => ({ value: c.columnName, label: colLabel(c) }));
  const keyColumnOptions = columns.map((c) => ({
    value: c.columnName,
    label: _isKeyable(c) ? colLabel(c) : `${colLabel(c)} — ${t("modelingForm.notKeyable")}`,
    disabled: !_isKeyable(c),
  }));
  const numericColumnOptions = columns.filter(_isNumeric).map((c) => ({
    value: c.columnName,
    label: colLabel(c),
  }));

  const noSource = !selectedTable;

  const submit = async () => {
    setError(null);
    if (!name.trim() || !source || !domainId) {
      setError(t("modelingForm.errorRequired"));
      return;
    }
    setBusy(true);
    try {
      let result;
      if (kind === "entity") {
        result = await registerEntity({
          name: name.trim(),
          source,
          domainId,
          key,
          attributes,
          history,
        });
      } else {
        const parsedMeasures = Object.entries(measures).map(([column, agg]) => ({ column, agg }));
        const parsedDims = dimensions
          .filter((d) => d.entity && d.via)
          .map((d) => ({ entity: d.entity, via: d.via }));
        result = await registerFact({
          name: name.trim(),
          source,
          domainId,
          grain,
          measures: parsedMeasures,
          dimensions: parsedDims,
        });
      }
      if (!result.success) {
        setError(result.message);
        return;
      }
      setCreated({
        kind,
        name: name.trim(),
        domainId,
        historized: kind === "entity" && history !== "none",
        dimCount: dimensions.filter((d) => d.entity && d.via).length,
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  if (created) {
    const kindLabel = t(`modelingForm.${created.kind}`);
    return (
      <Stack gap="md" data-testid="modeling-created">
        <Alert color="green" title={t("modelingForm.createdTitle", { name: created.name })}>
          <Stack gap="xs">
            <Text size="sm">
              {t("modelingForm.createdMv", {
                kind: kindLabel,
                name: created.name,
                domain: created.domainId,
              })}
            </Text>
            {created.historized && <Text size="sm">{t("modelingForm.createdHistory")}</Text>}
            {created.dimCount > 0 && (
              <Text size="sm">{t("modelingForm.createdDims", { count: created.dimCount })}</Text>
            )}
            <Text size="sm" fw={600} mt="xs">
              {t("modelingForm.reviseTitle")}
            </Text>
            <Text size="sm">{t("modelingForm.reviseBody", { name: created.name })}</Text>
          </Stack>
        </Alert>
        <Group justify="flex-end">
          <Button
            variant="default"
            onClick={() => {
              setCreated(null);
              setName("");
              setSource(null);
              setKey([]);
              setAttributes([]);
              setHistory("none");
              setGrain([]);
              setMeasures({});
              setDimensions([]);
            }}
            data-testid="modeling-create-another"
          >
            {t("modelingForm.createAnother")}
          </Button>
          <Button onClick={onSuccess} data-testid="modeling-done">
            {t("modelingForm.done")}
          </Button>
        </Group>
      </Stack>
    );
  }

  return (
    <Stack gap="sm">
      <Stack gap={4}>
        <FieldLabel text={t("modelingForm.kindLabel")} help={t("modelingForm.kindHelp")} />
        <SegmentedControl
          data-testid="modeling-kind"
          value={kind}
          onChange={(v) => setKind(v as "entity" | "fact")}
          data={[
            { value: "entity", label: t("modelingForm.entity") },
            { value: "fact", label: t("modelingForm.fact") },
          ]}
        />
      </Stack>
      {error && (
        <Alert color="red" data-testid="modeling-error">
          {error}
        </Alert>
      )}
      <TextInput
        label={<FieldLabel text={t("modelingForm.nameLabel")} help={t("modelingForm.nameHelp")} required />}
        value={name}
        onChange={(e) => setName(e.currentTarget.value)}
        placeholder={kind === "entity" ? "Customer" : "Sales"}
        data-testid="modeling-name"
      />
      <Select
        label={<FieldLabel text={t("modelingForm.sourceLabel")} help={t("modelingForm.sourceHelp")} required />}
        searchable
        data={sourceOptions}
        value={source}
        onChange={setSource}
        placeholder={kind === "entity" ? "raw.customers" : "raw.orders"}
        comboboxProps={{ withinPortal: true }}
        data-testid="modeling-source"
      />
      <Select
        label={<FieldLabel text={t("modelingForm.domainLabel")} help={t("modelingForm.domainHelp")} required />}
        data={domains.map((d) => ({ value: d.id, label: d.id }))}
        value={domainId}
        onChange={(v) => setDomainId(v ?? "")}
        comboboxProps={{ withinPortal: true }}
        data-testid="modeling-domain"
      />
      {kind === "entity" ? (
        <>
          <MultiSelect
            label={<FieldLabel text={t("modelingForm.keyLabel")} help={t("modelingForm.keyHelp")} required />}
            description={t("modelingForm.keyHint")}
            searchable
            data={keyColumnOptions}
            value={key}
            onChange={setKey}
            disabled={noSource}
            placeholder={noSource ? t("modelingForm.selectSourceFirst") : "id"}
            comboboxProps={{ withinPortal: true }}
            data-testid="modeling-key"
          />
          <MultiSelect
            label={<FieldLabel text={t("modelingForm.attributesLabel")} help={t("modelingForm.attributesHelp")} />}
            searchable
            data={allColumnOptions}
            value={attributes}
            onChange={setAttributes}
            disabled={noSource}
            placeholder={noSource ? t("modelingForm.selectSourceFirst") : "name, region, tier"}
            comboboxProps={{ withinPortal: true }}
            data-testid="modeling-attributes"
          />
          {!noSource && (
            <Button
              variant="subtle"
              size="compact-xs"
              w="fit-content"
              onClick={() => setAttributes(columns.map((c) => c.columnName))}
              data-testid="modeling-attributes-all"
            >
              {t("modelingForm.selectAll")}
            </Button>
          )}
          <Select
            label={<FieldLabel text={t("modelingForm.historyLabel")} help={t("modelingForm.historyHelp")} />}
            data={[
              { value: "none", label: t("modelingForm.historyNone") },
              { value: "scd2", label: t("modelingForm.historyScd2") },
              { value: "snapshot", label: t("modelingForm.historySnapshot") },
            ]}
            value={history}
            onChange={(v) => setHistory(v ?? "none")}
            comboboxProps={{ withinPortal: true }}
            allowDeselect={false}
            data-testid="modeling-history"
          />
        </>
      ) : (
        <>
          <MultiSelect
            label={<FieldLabel text={t("modelingForm.grainLabel")} help={t("modelingForm.grainHelp")} required />}
            description={t("modelingForm.keyHint")}
            searchable
            data={keyColumnOptions}
            value={grain}
            onChange={setGrain}
            disabled={noSource}
            placeholder={noSource ? t("modelingForm.selectSourceFirst") : "order_id"}
            comboboxProps={{ withinPortal: true }}
            data-testid="modeling-grain"
          />
          <MultiSelect
            label={<FieldLabel text={t("modelingForm.measuresLabel")} help={t("modelingForm.measuresHelp")} />}
            description={t("modelingForm.measuresHint")}
            searchable
            data={numericColumnOptions}
            value={Object.keys(measures)}
            onChange={(cols) =>
              setMeasures((prev) => {
                const next: Record<string, string> = {};
                for (const c of cols) next[c] = prev[c] ?? "sum";
                return next;
              })
            }
            disabled={noSource}
            placeholder={noSource ? t("modelingForm.selectSourceFirst") : "amount, quantity"}
            comboboxProps={{ withinPortal: true }}
            data-testid="modeling-measures"
          />
          {Object.keys(measures).map((col) => (
            <Group key={col} gap="sm" wrap="nowrap" pl="sm">
              <Text size="sm" style={{ flex: 1 }}>
                {col}
              </Text>
              <Select
                data={_AGGS.map((a) => ({ value: a, label: a }))}
                value={measures[col]}
                onChange={(v) => setMeasures((prev) => ({ ...prev, [col]: v ?? "sum" }))}
                comboboxProps={{ withinPortal: true }}
                allowDeselect={false}
                w={120}
                data-testid={`modeling-measure-agg-${col}`}
              />
            </Group>
          ))}
          <div>
            <div style={{ marginBottom: 4 }}>
              <FieldLabel
                text={t("modelingForm.dimensionsLabel")}
                help={t("modelingForm.dimensionsHelp")}
              />
            </div>
            <Stack gap="xs">
              {dimensions.map((dim, i) => (
                <Group key={i} gap="sm" wrap="nowrap">
                  <Select
                    placeholder={t("modelingForm.dimensionEntity")}
                    searchable
                    data={entityOptions}
                    value={dim.entity || null}
                    onChange={(v) =>
                      setDimensions((prev) =>
                        prev.map((d, j) => (j === i ? { ...d, entity: v ?? "" } : d)),
                      )
                    }
                    comboboxProps={{ withinPortal: true }}
                    style={{ flex: 1 }}
                    data-testid={`modeling-dim-entity-${i}`}
                  />
                  <Select
                    placeholder={t("modelingForm.dimensionVia")}
                    searchable
                    data={allColumnOptions}
                    value={dim.via || null}
                    onChange={(v) =>
                      setDimensions((prev) =>
                        prev.map((d, j) => (j === i ? { ...d, via: v ?? "" } : d)),
                      )
                    }
                    disabled={noSource}
                    comboboxProps={{ withinPortal: true }}
                    style={{ flex: 1 }}
                    data-testid={`modeling-dim-via-${i}`}
                  />
                  <ActionIcon
                    variant="subtle"
                    color="red"
                    onClick={() => setDimensions((prev) => prev.filter((_, j) => j !== i))}
                    data-testid={`modeling-dim-remove-${i}`}
                  >
                    <Trash2 size={16} />
                  </ActionIcon>
                </Group>
              ))}
              <Button
                variant="light"
                size="xs"
                onClick={() => setDimensions((prev) => [...prev, { entity: "", via: "" }])}
                data-testid="modeling-dim-add"
              >
                {t("modelingForm.addDimension")}
              </Button>
            </Stack>
          </div>
        </>
      )}
      <Group justify="flex-end" mt="sm">
        <Button variant="default" onClick={onCancel}>
          {t("modelingForm.cancel")}
        </Button>
        <Button onClick={submit} loading={busy} data-testid="modeling-submit">
          {t("modelingForm.submit")}
        </Button>
      </Group>
    </Stack>
  );
}
