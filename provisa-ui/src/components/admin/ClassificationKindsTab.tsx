// Copyright (c) 2026 Kenneth Stott
// Canary: 740e92b6-5eeb-4322-9f00-c45ca986961c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147: the profiler's classification kinds, each with its test, its threshold, whether it is
// sensitive and the mask it offers. An organisation administrator enables or disables a kind here;
// a disabled kind is not tested by any profile run and raises no suggestions. Everything else about
// a kind is shipped with the product and shown as it is.

import { useEffect, useState } from "react";
import { Alert, Badge, Loader, Stack, Switch, Table, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  fetchClassificationKinds,
  setEnabledClassificationKinds,
  type ClassificationKind,
} from "../../api/profiler";

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function ClassificationKindsTab() {
  const { t } = useTranslation();
  const [kinds, setKinds] = useState<ClassificationKind[] | null>(null);
  const [canEdit, setCanEdit] = useState(false);
  const [saving, setSaving] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchClassificationKinds()
      .then((r) => {
        setKinds(r.kinds);
        setCanEdit(r.canEdit);
      })
      .catch((e: unknown) => setError(message(e)));
  }, []);

  const toggle = async (kind: ClassificationKind, enabled: boolean) => {
    if (kinds == null) return;
    const wanted = kinds.filter((k) => (k.id === kind.id ? enabled : k.enabled)).map((k) => k.id);
    setSaving(kind.id);
    try {
      const r = await setEnabledClassificationKinds(wanted);
      setKinds(r.kinds);
      setError(null);
    } catch (e: unknown) {
      setError(message(e));
    } finally {
      setSaving(null);
    }
  };

  return (
    <Stack gap="xs" data-testid="classification-kinds-tab">
      <Text size="sm" c="dimmed">
        {t("classificationKinds.intro")}
      </Text>
      {error && (
        <Alert color="red" data-testid="classification-kinds-error">
          {error}
        </Alert>
      )}
      {kinds == null && error == null && <Loader size="sm" />}
      {kinds != null && (
        <Table withTableBorder striped fz="sm" data-testid="classification-kinds">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("classificationKinds.kindHeader")}</Table.Th>
              <Table.Th>{t("classificationKinds.testHeader")}</Table.Th>
              <Table.Th>{t("classificationKinds.thresholdHeader")}</Table.Th>
              <Table.Th>{t("classificationKinds.maskHeader")}</Table.Th>
              <Table.Th>{t("classificationKinds.enabledHeader")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {kinds.map((k) => (
              <Table.Tr key={k.id} data-testid={`classification-kind-${k.id}`}>
                <Table.Td style={{ maxWidth: 260 }}>
                  {t(`classificationKinds.kind.${k.id}`)}
                  {k.sensitive && (
                    <Badge ml="xs" size="xs" color="red" variant="light">
                      {t("classificationKinds.sensitive")}
                    </Badge>
                  )}
                </Table.Td>
                <Table.Td style={{ maxWidth: 520 }}>
                  {t(`classificationKinds.test.${k.id}`)}
                </Table.Td>
                <Table.Td>{`${Math.round(k.threshold * 100)}%`}</Table.Td>
                <Table.Td>
                  {k.maskPreset
                    ? t(`maskList.preset.${k.maskPreset}`)
                    : t("classificationKinds.noMask")}
                </Table.Td>
                <Table.Td>
                  <Switch
                    checked={k.enabled}
                    disabled={!canEdit || saving != null}
                    onChange={(e) => void toggle(k, e.currentTarget.checked)}
                    aria-label={t("classificationKinds.enabledAria", {
                      kind: t(`classificationKinds.kind.${k.id}`),
                    })}
                    data-testid={`classification-kind-enabled-${k.id}`}
                  />
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}
