// Copyright (c) 2026 Kenneth Stott
// Canary: 7b2e5f91-3a4d-4c8e-6f0b-9d1a2c8e4b37
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import {
  ActionIcon,
  Alert,
  Checkbox,
  Fieldset,
  Group,
  NumberInput,
  Select,
  Stack,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { Info } from "lucide-react";
import { useTranslation } from "react-i18next";
import type { RegisteredTable, Source, LiveDeliveryConfig, LiveOutputConfig } from "../../types/admin";
import type { PlatformSettings } from "../../api/admin";
import { isWatermarkEligible } from "./helpers";

interface LiveDeliveryFieldsetProps {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  editingColumnTypes: Record<string, string>;
  sources: Source[];
  settings: PlatformSettings | null;
}

export function LiveDeliveryFieldset({
  editingTable,
  setEditingTable,
  editingColumnTypes,
  sources,
}: LiveDeliveryFieldsetProps) {
  const { t } = useTranslation();
  const src = sources.find((s) => s.id === editingTable.sourceId);
  const stype = (src?.type ?? "").toLowerCase();
  const isEngineDerived = stype === "trino" || src?.id === "__provisa__";
  // Live Delivery is the OUTBOUND axis. The mechanism — repeat a push stream
  // vs. append/replace poll — is DERIVED from the effective Change Signal
  // (the inbound axis), never re-chosen here (REQ-932: change_signal subsumes
  // the legacy live.strategy). Live Delivery only owns the outbound choices:
  // on/off, watermark selection (append vs replace), poll interval, outputs.
  const effectiveSignal =
    editingTable.changeSignal || src?.changeSignal || "ttl";
  const isPushSignal = ["native", "debezium", "kafka"].includes(
    effectiveSignal,
  );
  const live = editingTable.live;
  const setLive = (patch: Partial<LiveDeliveryConfig>) =>
    setEditingTable(
      live
        ? { ...editingTable, live: { ...live, ...patch } }
        : editingTable,
    );
  const wmCols = editingTable.columns.filter((c) => {
    const dt = editingColumnTypes[c.columnName];
    return !dt || isWatermarkEligible(dt);
  });
  // Append vs replace is derived, not chosen (mirrors change_signal.select_landing_shape):
  // a poll signal WITH a watermark appends past MAX(watermark); WITHOUT one it
  // full-replaces and the output content-hash suppresses unchanged ripples.
  const effWatermark = live?.watermarkColumn || "";
  const pollMode: "append" | "replace" | null = isPushSignal
    ? null
    : effWatermark
      ? "append"
      : "replace";
  const refreshInterval = editingTable.materialize
    ? editingTable.mvRefreshInterval
    : null;
  const kafkaOut =
    live?.outputs.find((o) => o.type === "kafka") ?? null;
  const setKafkaOut = (patch: Partial<LiveOutputConfig>) => {
    if (!live) return;
    const others = live.outputs.filter((o) => o.type !== "kafka");
    const base: LiveOutputConfig = kafkaOut ?? {
      type: "kafka",
      topic: "",
      keyColumn: null,
      bootstrapServers: "",
    };
    setLive({ outputs: [...others, { ...base, ...patch }] });
  };
  const defaultLive: LiveDeliveryConfig = {
    queryId: `${editingTable.sourceId}.${editingTable.tableName}`,
    watermarkColumn: isPushSignal
      ? ""
      : editingTable.watermarkColumn || wmCols[0]?.columnName || "",
    pollInterval: 10,
    // Derived from the effective Change Signal; kept only for the REQ-932
    // legacy read-through in reconcile (change_signal is authoritative).
    strategy: (isPushSignal
      ? effectiveSignal
      : "poll") as LiveDeliveryConfig["strategy"],
    kafka: null,
    outputs: [],
  };
  return (
    <Fieldset
      legend={
        live
          ? t("liveDeliveryFieldset.legendActive")
          : t("liveDeliveryFieldset.legend")
      }
      style={{ gridColumn: "1 / -1" }}
    >
      {isEngineDerived && !editingTable.materialize && (
        <Alert color="yellow" variant="light" mb="sm" p="xs">
          <Text size="xs">
            {t("liveDeliveryFieldset.materializeWarningPre")}
            <em>{t("liveDeliveryFieldset.materializeWarningEm")}</em>
            {t("liveDeliveryFieldset.materializeWarningPost")}
          </Text>
        </Alert>
      )}
      <Checkbox
        data-testid="live-delivery-enable"
        checked={!!live}
        label={t("liveDeliveryFieldset.enableLabel")}
        onChange={(e) =>
          setEditingTable({
            ...editingTable,
            live: e.currentTarget.checked ? defaultLive : null,
          })
        }
      />
      {live && (
        <Stack gap="sm" mt="sm">
          <Text data-testid="live-mechanism" size="xs" c="dimmed">
            {isPushSignal ? (
              <>
                {t("liveDeliveryFieldset.mechanismPushPre")}
                <strong>{effectiveSignal}</strong>
                {t("liveDeliveryFieldset.mechanismPushMid")}
                <em>{t("liveDeliveryFieldset.mechanismPushEm")}</em>
                {t("liveDeliveryFieldset.mechanismPushPost")}
              </>
            ) : pollMode === "append" ? (
              <>
                {t("liveDeliveryFieldset.mechanismAppendPre")}
                <strong>{effWatermark}</strong>
                {t("liveDeliveryFieldset.mechanismAppendPost")}
              </>
            ) : (
              t("liveDeliveryFieldset.mechanismReplace")
            )}
          </Text>
          <TextInput
            readOnly
            label={t("liveDeliveryFieldset.queryIdLabel")}
            value={`${editingTable.sourceId}.${editingTable.tableName}`}
            styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
          />
          {!isPushSignal && (
            <>
              {wmCols.length > 0 && (
                <div>
                  <Group gap={4} mb={2}>
                    <Text size="sm" component="label" htmlFor="live-delivery-watermark-column">
                      {t("liveDeliveryFieldset.watermarkColumnLabel")}
                    </Text>
                    <Tooltip
                      label={t("liveDeliveryFieldset.watermarkColumnTooltip")}
                      multiline
                      w={280}
                      withArrow
                    >
                      <ActionIcon
                        variant="subtle"
                        color="gray"
                        size="xs"
                        aria-label={t(
                          "liveDeliveryFieldset.watermarkColumnTooltip",
                        )}
                      >
                        <Info size={12} />
                      </ActionIcon>
                    </Tooltip>
                  </Group>
                  <Select
                    id="live-delivery-watermark-column"
                    data-testid="watermark-column-select"
                    value={live.watermarkColumn ?? ""}
                    onChange={(value) =>
                      setLive({ watermarkColumn: value ?? "" })
                    }
                    data={[
                      {
                        value: "",
                        label: t("liveDeliveryFieldset.watermarkNoneOption"),
                      },
                      ...wmCols.map((c) => ({
                        value: c.columnName,
                        label: c.columnName,
                      })),
                    ]}
                    allowDeselect={false}
                    comboboxProps={{ withinPortal: true }}
                  />
                </div>
              )}
              <div>
                <Group gap={4} mb={2}>
                  <Text size="sm" component="label" htmlFor="live-delivery-poll-interval">
                    {t("liveDeliveryFieldset.pollIntervalLabel")}
                  </Text>
                  <Tooltip
                    label={t("liveDeliveryFieldset.pollIntervalTooltip")}
                    multiline
                    w={280}
                    withArrow
                  >
                    <ActionIcon
                      variant="subtle"
                      color="gray"
                      size="xs"
                      aria-label={t(
                        "liveDeliveryFieldset.pollIntervalTooltip",
                      )}
                    >
                      <Info size={12} />
                    </ActionIcon>
                  </Tooltip>
                </Group>
                <NumberInput
                  id="live-delivery-poll-interval"
                  data-testid="poll-interval-input"
                  min={1}
                  value={live.pollInterval}
                  onChange={(value) =>
                    setLive({
                      pollInterval: Number(value) || 10,
                    })
                  }
                />
                {refreshInterval != null &&
                  live.pollInterval < refreshInterval && (
                    <Text size="xs" c="yellow.7" mt={4}>
                      {t("liveDeliveryFieldset.pollFasterWarning", {
                        seconds: refreshInterval,
                      })}
                    </Text>
                  )}
              </div>
            </>
          )}
          {isPushSignal && (
            <Text size="xs" c="dimmed">
              {effectiveSignal === "debezium"
                ? t("liveDeliveryFieldset.debeziumNote")
                : effectiveSignal === "kafka"
                  ? t("liveDeliveryFieldset.kafkaNote")
                  : t("liveDeliveryFieldset.nativeNote")}
            </Text>
          )}
          {/* REQ-813: inbound Kafka transport params, editable when the change signal is Kafka. */}
          {effectiveSignal === "kafka" && (
            <Stack gap="xs" pl="lg">
              <Text size="xs" fw={600}>
                {t("liveDeliveryFieldset.kafkaConsumeHeading")}
              </Text>
              <TextInput
                label={t("liveDeliveryFieldset.kafkaConsumeTopicLabel")}
                value={live.kafka?.topic ?? ""}
                onChange={(e) =>
                  setLive({ kafka: { ...(live.kafka ?? { topic: "" }), topic: e.currentTarget.value } })
                }
                placeholder="orders.cdc"
                data-testid="live-kafka-topic"
              />
              <TextInput
                label={t("liveDeliveryFieldset.kafkaConsumeFormatLabel")}
                value={live.kafka?.format ?? ""}
                onChange={(e) =>
                  setLive({
                    kafka: { ...(live.kafka ?? { topic: "" }), format: e.currentTarget.value || undefined },
                  })
                }
                placeholder="json"
                data-testid="live-kafka-format"
              />
              <TextInput
                label={t("liveDeliveryFieldset.kafkaConsumeKeyColumnLabel")}
                value={live.kafka?.keyColumn ?? ""}
                onChange={(e) =>
                  setLive({
                    kafka: {
                      ...(live.kafka ?? { topic: "" }),
                      keyColumn: e.currentTarget.value || null,
                    },
                  })
                }
                placeholder="(optional)"
                data-testid="live-kafka-key-column"
              />
            </Stack>
          )}
          <div>
            <Text size="xs" fw={600} mb={4}>
              {t("liveDeliveryFieldset.outputsHeading")}
            </Text>
            <Checkbox
              checked
              readOnly
              disabled
              label={t("liveDeliveryFieldset.sseFanoutLabel")}
            />
            <Checkbox
              data-testid="kafka-sink-checkbox"
              checked={!!kafkaOut}
              label={t("liveDeliveryFieldset.kafkaSinkLabel")}
              onChange={(e) => {
                if (e.currentTarget.checked) setKafkaOut({});
                else
                  setLive({
                    outputs: live.outputs.filter(
                      (o) => o.type !== "kafka",
                    ),
                  });
              }}
            />
            {kafkaOut && (
              <Stack gap="xs" mt="xs" pl="lg">
                <TextInput
                  label={t("liveDeliveryFieldset.kafkaBootstrapLabel")}
                  value={kafkaOut.bootstrapServers ?? ""}
                  placeholder={t(
                    "liveDeliveryFieldset.kafkaBootstrapPlaceholder",
                  )}
                  onChange={(e) =>
                    setKafkaOut({
                      bootstrapServers: e.currentTarget.value,
                    })
                  }
                />
                <TextInput
                  label={t("liveDeliveryFieldset.kafkaTopicLabel")}
                  value={kafkaOut.topic ?? ""}
                  onChange={(e) =>
                    setKafkaOut({ topic: e.currentTarget.value })
                  }
                />
                <TextInput
                  label={t("liveDeliveryFieldset.kafkaKeyColumnLabel")}
                  value={kafkaOut.keyColumn ?? ""}
                  onChange={(e) =>
                    setKafkaOut({
                      keyColumn: e.currentTarget.value || null,
                    })
                  }
                />
              </Stack>
            )}
          </div>
        </Stack>
      )}
    </Fieldset>
  );
}
