// Copyright (c) 2026 Kenneth Stott
// Canary: 2e8a5c17-9b3d-4f60-a7c2-e14b6d9f3a58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: the model for one discovery run. It opens on the model assigned to relationship
// inference; it lists every vendor with a key, and the chosen vendor's models as the vendor
// serves them. A vendor that lists no models takes the model's name typed. The chosen model's two
// limits are shown under it (REQ-464). The choice never changes the assignment.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Select, TextInput } from "@mantine/core";
import {
  DiscoveryError,
  fetchModelLimits,
  fetchVendorModels,
  type DiscoveryModels,
  type LimitsSource,
} from "../../api/admin";
import { FieldGroup, FormField } from "../form/FormLayout";

interface Limits {
  input: number;
  output: number;
  source: LimitsSource;
}

/** A vendor's models as listed, or why they could not be: `typed` when the vendor lists none. */
type Listing =
  | { state: "loading" }
  | { state: "listed"; models: string[] }
  | { state: "typed" }
  | { state: "failed"; error: string };

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function DiscoverModelField({
  models,
  vendor,
  model,
  onChange,
  disabled,
}: {
  models: DiscoveryModels;
  vendor: string;
  model: string;
  onChange: (vendor: string, model: string) => void;
  disabled: boolean;
}) {
  const { t, i18n } = useTranslation();
  const assigned = models.assigned;
  const [listing, setListing] = useState<Listing>({ state: "loading" });
  const [limits, setLimits] = useState<Limits | null>(null);
  const [limitsError, setLimitsError] = useState("");

  useEffect(() => {
    if (!vendor) return;
    let live = true;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- a new vendor's listing is read
    setListing({ state: "loading" });
    fetchVendorModels(vendor)
      .then((listed) => live && setListing({ state: "listed", models: listed }))
      .catch((e) => {
        if (!live) return;
        setListing(
          e instanceof DiscoveryError && e.code === "discovery.vendor_unknown"
            ? { state: "typed" }
            : { state: "failed", error: errorText(e) },
        );
      });
    return () => {
      live = false;
    };
  }, [vendor]);

  const isAssigned = assigned !== null && vendor === assigned.vendor && model === assigned.model;
  useEffect(() => {
    if (!vendor || !model) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- no model, no limits
      setLimits(null);
      return;
    }
    if (isAssigned && assigned) {
      setLimits({
        input: assigned.input_tokens,
        output: assigned.output_tokens,
        source: assigned.limits_source,
      });
      setLimitsError("");
      return;
    }
    let live = true;
    // A typed name is read once the typing pauses.
    const timer = setTimeout(() => {
      fetchModelLimits(vendor, model)
        .then((l) => {
          if (!live) return;
          setLimits({ input: l.input_tokens, output: l.output_tokens, source: l.source });
          setLimitsError("");
        })
        .catch((e) => {
          if (!live) return;
          setLimits(null);
          setLimitsError(errorText(e));
        });
    }, 300);
    return () => {
      live = false;
      clearTimeout(timer);
    };
  }, [vendor, model, isAssigned, assigned]);

  const number = (n: number) => n.toLocaleString(i18n.language);
  const limitsText = limits
    ? t(limits.source === "default" ? "discoverDialog.limitsDefault" : "discoverDialog.limits", {
        input: number(limits.input),
        output: number(limits.output),
      })
    : "";
  // The assigned model is offered with its vendor's list, whatever the list holds.
  const listed = listing.state === "listed" ? listing.models : [];
  const offered =
    assigned && vendor === assigned.vendor && !listed.includes(assigned.model)
      ? [assigned.model, ...listed]
      : listed;

  return (
    <FieldGroup>
      <FormField size="medium">
        <Select
          label={t("discoverDialog.vendor")}
          data={models.vendors}
          value={vendor || null}
          onChange={(v) =>
            v && onChange(v, assigned && v === assigned.vendor ? assigned.model : "")
          }
          allowDeselect={false}
          disabled={disabled}
          comboboxProps={{ withinPortal: true }}
          data-testid="discover-vendor"
        />
      </FormField>
      {listing.state === "typed" ? (
        <FormField size="long" hint={limitsText || t("discoverDialog.modelTypedHint")}>
          <TextInput
            label={t("discoverDialog.modelTyped")}
            value={model}
            onChange={(e) => onChange(vendor, e.currentTarget.value)}
            disabled={disabled}
            error={limitsError || undefined}
            descriptionProps={{ "data-testid": "discover-limits" }}
            data-testid="discover-model-typed"
          />
        </FormField>
      ) : (
        <FormField size="long" help={t("discoverDialog.modelHelp")} hint={limitsText}>
          <Select
            label={t("discoverDialog.model")}
            data={offered}
            value={model || null}
            onChange={(m) => m && onChange(vendor, m)}
            allowDeselect={false}
            searchable
            disabled={disabled || listing.state === "loading"}
            error={
              listing.state === "failed" ? listing.error : limitsError ? limitsError : undefined
            }
            comboboxProps={{ withinPortal: true }}
            descriptionProps={{ "data-testid": "discover-limits" }}
            data-testid="discover-model"
          />
        </FormField>
      )}
    </FieldGroup>
  );
}
