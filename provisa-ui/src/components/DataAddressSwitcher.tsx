// Copyright (c) 2026 Kenneth Stott
// Canary: 1a7c4e92-3f05-4d68-b9e7-5c2a8d0f6b31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1916 (amended 2026-11-02): the browser setting choosing where a coordinator-served UI's data
// calls go: one of the addresses the coordinator offers for its region, or one the user types. A UI
// served by an every node sends its data calls to its own origin and shows nothing here.

import { useEffect, useState } from "react";
import { Button, Group, Menu, Text, TextInput } from "@mantine/core";
import { Check, ChevronDown, Server } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  chooseDataAddress,
  dataAddressFor,
  dataAddressOffer,
  isDataAddress,
  type DataAddressOffer,
} from "../lib/dataAddress";

/** The address as shown: its host (and port), the part that tells one node from another. */
function shown(address: string): string {
  return new URL(address).host;
}

export function DataAddressSwitcher() {
  const { t } = useTranslation();
  const [offer, setOffer] = useState<DataAddressOffer | null>(null);
  const [failed, setFailed] = useState(false);
  const [active, setActive] = useState<string | null>(null);
  const [typed, setTyped] = useState("");

  useEffect(() => {
    let cancelled = false;
    dataAddressOffer()
      .then((got) => {
        if (cancelled) return;
        setOffer(got);
        setActive(dataAddressFor(got));
      })
      .catch(() => {
        if (!cancelled) setFailed(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // A failed ask is an error to show, never an absence: it must not read as "an every node".
  if (failed) {
    return (
      <Text size="xs" c="red" data-testid="data-address-load-error">
        {t("dataAddress.loadError")}
      </Text>
    );
  }
  if (offer === null || !offer.choose) return null;

  function choose(address: string) {
    chooseDataAddress(address);
    setActive(address);
  }

  const typedValid = isDataAddress(typed.trim());
  const listed = active !== null && offer.addresses.includes(active);

  return (
    <Menu position="bottom-end" withinPortal transitionProps={{ duration: 0 }}>
      <Menu.Target>
        <Button
          variant="default"
          size="compact-sm"
          leftSection={<Server size={14} aria-hidden />}
          rightSection={<ChevronDown size={14} aria-hidden />}
          data-testid="data-address-trigger"
          aria-label={t("dataAddress.label")}
        >
          {active === null ? t("dataAddress.none") : shown(active)}
        </Button>
      </Menu.Target>
      <Menu.Dropdown miw={300}>
        <Menu.Label>{t("dataAddress.heading")}</Menu.Label>
        {offer.addresses.length === 0 && (
          <Text size="xs" c="dimmed" px="sm" py={4} data-testid="data-address-none-offered">
            {t("dataAddress.noneOffered")}
          </Text>
        )}
        {offer.addresses.map((address) => {
          const selected = address === active;
          return (
            <Menu.Item
              key={address}
              role="option"
              aria-selected={selected}
              data-address-option={address}
              leftSection={selected ? <Check size={14} aria-hidden /> : undefined}
              onClick={() => choose(address)}
            >
              {address}
            </Menu.Item>
          );
        })}
        {active !== null && !listed && (
          <Menu.Item
            role="option"
            aria-selected
            leftSection={<Check size={14} aria-hidden />}
            data-testid="data-address-typed-active"
          >
            {active}
          </Menu.Item>
        )}
        <Menu.Divider />
        <Group gap="xs" px="sm" py={4} wrap="nowrap" align="flex-start">
          <TextInput
            size="xs"
            flex={1}
            aria-label={t("dataAddress.typedLabel")}
            placeholder={t("dataAddress.typedPlaceholder")}
            value={typed}
            onChange={(e) => setTyped(e.currentTarget.value)}
            error={typed.trim() !== "" && !typedValid ? t("dataAddress.typedInvalid") : undefined}
            data-testid="data-address-typed"
          />
          <Button
            size="xs"
            disabled={!typedValid}
            onClick={() => {
              choose(typed.trim());
              setTyped("");
            }}
            data-testid="data-address-use"
          >
            {t("dataAddress.use")}
          </Button>
        </Group>
      </Menu.Dropdown>
    </Menu>
  );
}
