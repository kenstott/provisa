// Copyright (c) 2026 Kenneth Stott
// Canary: 1c7f5a93-2d4e-4b86-9a0f-e3b8d6c2a714
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2216: how the members a request is addressed to are told of it -- by Provisa's email (the
// default), by the org's workflow tool subscribed to ops.requests (events only), or both. Saved as
// it is changed.
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Group, SegmentedControl, Text } from "@mantine/core";
import { LabelWithHelp } from "../form/FormLayout";
import {
  fetchRequestDelivery,
  saveRequestDelivery,
  type RequestDelivery,
} from "../../api/requestDelivery";

export function RequestDeliverySetting({ onError }: { onError: (message: string) => void }) {
  const { t } = useTranslation();
  const [delivery, setDelivery] = useState<RequestDelivery | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    fetchRequestDelivery()
      .then(setDelivery)
      .catch((e) => onError(e instanceof Error ? e.message : String(e)));
  }, [onError]);

  const change = async (next: RequestDelivery) => {
    const before = delivery;
    setDelivery(next);
    setSaving(true);
    try {
      setDelivery(await saveRequestDelivery(next));
    } catch (e) {
      setDelivery(before);
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Group gap="xs" wrap="nowrap" data-testid="request-delivery">
      <Text size="sm" component="div">
        <LabelWithHelp
          label={t("requestsPage.delivery")}
          help={t("requestsPage.deliveryHelp")}
          textId="request-delivery-label"
        />
      </Text>
      <SegmentedControl
        size="xs"
        aria-labelledby="request-delivery-label"
        value={delivery ?? ""}
        disabled={delivery === null || saving}
        onChange={(v) => change(v as RequestDelivery)}
        data={[
          { value: "email", label: t("requestsPage.deliveryEmail") },
          { value: "events", label: t("requestsPage.deliveryEvents") },
          { value: "both", label: t("requestsPage.deliveryBoth") },
        ]}
        data-testid="request-delivery-control"
      />
    </Group>
  );
}
