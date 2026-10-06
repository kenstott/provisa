// Copyright (c) 2026 Kenneth Stott
// Canary: 829d06cf-f5f4-47a8-a5be-0bf35ec8d8d9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Button,
  Group,
  NumberInput,
  SegmentedControl,
  Select,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import type { DatasetRelationship, FanoutCondition, FanoutCount } from "../../../api/synthetic";

type Kind = "fixed" | "range" | "measured";

function kindOf(count: FanoutCount): Kind {
  if ("fixed" in count) return "fixed";
  if ("low" in count) return "range";
  return "measured";
}

function countOf(kind: Kind): FanoutCount {
  if (kind === "fixed") return { fixed: 0 };
  if (kind === "range") return { low: 0, high: 1 };
  return { measured: true };
}

// REQ-1939, CONDITIONAL FAN-OUT: a relationship's child counts by a condition on the parent row.
// The first condition a parent meets decides its count; one meeting none takes the relationship's
// measured fan-out. Only the relationships whose parent and child the dataset generates are offered.
export function DatasetConditions({
  relationships,
  value,
  onChange,
}: {
  relationships: DatasetRelationship[];
  value: FanoutCondition[];
  onChange: (next: FanoutCondition[]) => void;
}) {
  const { t } = useTranslation();
  const set = (i: number, patch: Partial<FanoutCondition>) =>
    onChange(value.map((c, j) => (j === i ? { ...c, ...patch } : c)));
  const options = relationships.map((r) => ({
    value: r.id,
    label: t("syntheticDatasets.relationshipOption", {
      parent: r.parentTable,
      child: r.childTable,
    }),
  }));
  return (
    <Stack gap="xs" data-testid="synthetic-conditions">
      <Title order={6}>{t("syntheticDatasets.conditionsTitle")}</Title>
      <Text size="xs" c="dimmed">
        {t("syntheticDatasets.conditionsHelp")}
      </Text>
      {value.map((c, i) => {
        const count = c.count;
        return (
          <Group key={i} align="end" data-testid={`synthetic-condition-${i}`}>
            <Select
              label={t("syntheticDatasets.relationship")}
              data={options}
              value={c.relationship || null}
              onChange={(v) => v && set(i, { relationship: v })}
              allowDeselect={false}
            />
            <TextInput
              label={t("syntheticDatasets.condition")}
              placeholder="status = 'cancelled'"
              value={c.condition}
              onChange={(e) => set(i, { condition: e.currentTarget.value })}
            />
            <SegmentedControl
              aria-label={t("syntheticDatasets.countKind")}
              value={kindOf(count)}
              onChange={(k) => set(i, { count: countOf(k as Kind) })}
              data={[
                { value: "fixed", label: t("syntheticDatasets.countFixed") },
                { value: "range", label: t("syntheticDatasets.countRange") },
                { value: "measured", label: t("syntheticDatasets.countMeasured") },
              ]}
            />
            {"fixed" in count && (
              <NumberInput
                label={t("syntheticDatasets.count")}
                min={0}
                allowDecimal={false}
                value={count.fixed}
                onChange={(v) => set(i, { count: { fixed: Number(v) } })}
              />
            )}
            {"low" in count && (
              <>
                <NumberInput
                  label={t("syntheticDatasets.low")}
                  min={0}
                  allowDecimal={false}
                  value={count.low}
                  onChange={(v) => set(i, { count: { ...count, low: Number(v) } })}
                />
                <NumberInput
                  label={t("syntheticDatasets.high")}
                  min={0}
                  allowDecimal={false}
                  value={count.high}
                  onChange={(v) => set(i, { count: { ...count, high: Number(v) } })}
                />
              </>
            )}
            <ActionIcon
              variant="subtle"
              aria-label={t("syntheticDatasets.remove")}
              onClick={() => onChange(value.filter((_, j) => j !== i))}
            >
              ×
            </ActionIcon>
          </Group>
        );
      })}
      <Group>
        <Button
          size="xs"
          variant="light"
          disabled={relationships.length === 0}
          onClick={() =>
            onChange([...value, { relationship: "", condition: "", count: { fixed: 0 } }])
          }
          data-testid="synthetic-add-condition"
        >
          {t("syntheticDatasets.addCondition")}
        </Button>
      </Group>
    </Stack>
  );
}
