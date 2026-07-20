// Copyright (c) 2026 Kenneth Stott
// Canary: 938fb033-8525-4c25-a0fb-cab187faba70
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, SegmentedControl, Select, Stack, TextInput } from "@mantine/core";
import { useRegisterEntity, useRegisterFact } from "../../hooks/useAdminQueries";
import type { Domain } from "../../types/admin";

// REQ-1164: declarative entity/fact modeling. The form collects the spec and the backend lowers it
// to the underlying MV (+ relationships) — the star dimension/fact or Data Vault hub/satellite/link.

const _list = (s: string) =>
  s
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);

interface ModelingFormProps {
  domains: Domain[];
  onSuccess: () => void;
  onCancel: () => void;
}

export function ModelingForm({ domains, onSuccess, onCancel }: ModelingFormProps) {
  const { t } = useTranslation();
  const { registerEntity } = useRegisterEntity();
  const { registerFact } = useRegisterFact();
  const [kind, setKind] = useState<"entity" | "fact">("entity");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [name, setName] = useState("");
  const [source, setSource] = useState("");
  const [domainId, setDomainId] = useState("");
  // entity
  const [key, setKey] = useState("");
  const [attributes, setAttributes] = useState("");
  const [history, setHistory] = useState("none");
  // fact
  const [grain, setGrain] = useState("");
  const [measures, setMeasures] = useState(""); // "amount:sum, qty:sum"
  const [dimensions, setDimensions] = useState(""); // "Customer:customer_id, Product:product_id"

  const submit = async () => {
    setError(null);
    if (!name.trim() || !source.trim() || !domainId) {
      setError(t("modelingForm.errorRequired"));
      return;
    }
    setBusy(true);
    try {
      let result;
      if (kind === "entity") {
        result = await registerEntity({
          name: name.trim(),
          source: source.trim(),
          domainId,
          key: _list(key),
          attributes: _list(attributes),
          history,
        });
      } else {
        const parsedMeasures = _list(measures).map((m) => {
          const [column, agg] = m.split(":").map((s) => s.trim());
          return { column, agg: agg || "sum" };
        });
        const parsedDims = _list(dimensions).map((d) => {
          const [entity, via] = d.split(":").map((s) => s.trim());
          return { entity, via };
        });
        result = await registerFact({
          name: name.trim(),
          source: source.trim(),
          domainId,
          grain: _list(grain),
          measures: parsedMeasures,
          dimensions: parsedDims,
        });
      }
      if (!result.success) {
        setError(result.message);
        return;
      }
      onSuccess();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Stack gap="sm">
      <SegmentedControl
        data-testid="modeling-kind"
        value={kind}
        onChange={(v) => setKind(v as "entity" | "fact")}
        data={[
          { value: "entity", label: t("modelingForm.entity") },
          { value: "fact", label: t("modelingForm.fact") },
        ]}
      />
      {error && (
        <Alert color="red" data-testid="modeling-error">
          {error}
        </Alert>
      )}
      <TextInput
        label={t("modelingForm.nameLabel")}
        required
        value={name}
        onChange={(e) => setName(e.currentTarget.value)}
        placeholder={kind === "entity" ? "Customer" : "Sales"}
        data-testid="modeling-name"
      />
      <TextInput
        label={t("modelingForm.sourceLabel")}
        required
        value={source}
        onChange={(e) => setSource(e.currentTarget.value)}
        placeholder={kind === "entity" ? "raw.customers" : "raw.orders"}
        data-testid="modeling-source"
      />
      <Select
        label={t("modelingForm.domainLabel")}
        required
        data={domains.map((d) => ({ value: d.id, label: d.id }))}
        value={domainId}
        onChange={(v) => setDomainId(v ?? "")}
        comboboxProps={{ withinPortal: true }}
        data-testid="modeling-domain"
      />
      {kind === "entity" ? (
        <>
          <TextInput
            label={t("modelingForm.keyLabel")}
            required
            value={key}
            onChange={(e) => setKey(e.currentTarget.value)}
            placeholder="id"
            data-testid="modeling-key"
          />
          <TextInput
            label={t("modelingForm.attributesLabel")}
            value={attributes}
            onChange={(e) => setAttributes(e.currentTarget.value)}
            placeholder="name, region, tier"
            data-testid="modeling-attributes"
          />
          <Select
            label={t("modelingForm.historyLabel")}
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
          <TextInput
            label={t("modelingForm.grainLabel")}
            required
            value={grain}
            onChange={(e) => setGrain(e.currentTarget.value)}
            placeholder="order_id"
            data-testid="modeling-grain"
          />
          <TextInput
            label={t("modelingForm.measuresLabel")}
            value={measures}
            onChange={(e) => setMeasures(e.currentTarget.value)}
            placeholder="amount:sum, quantity:sum"
            data-testid="modeling-measures"
          />
          <TextInput
            label={t("modelingForm.dimensionsLabel")}
            value={dimensions}
            onChange={(e) => setDimensions(e.currentTarget.value)}
            placeholder="Customer:customer_id, Product:product_id"
            data-testid="modeling-dimensions"
          />
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
