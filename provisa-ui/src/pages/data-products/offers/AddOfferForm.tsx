// Copyright (c) 2026 Kenneth Stott
// Canary: 7d1b3e58-2a9c-4f60-b4e7-8c5a0f2d9e13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 OFFERS: the owner's form that adds an offer to a product. Its role is picked from the
// roles the owner may grant (AN OFFER'S ROLE IS ONE ITS OWNER MAY GRANT) that reach every dataset of
// the product, and the picked role's masks and row rules on them are shown before saving. The data
// level is full, a sample (views over the datasets, named here) or synthetic; quality results and
// profiles are the offer's own choices (a synthetic offer has no profiles); "open" is offered only
// when the role reads the data as public; a duration dates what accepting it grants. Its terms of
// use -- purpose, usage and limitations -- are prose, shown on its card and in its contracts, and
// agreed to by whoever bids for it. The summary says the offer in one sentence. The server still
// refuses what it must, by name.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Checkbox,
  Divider,
  Loader,
  NumberInput,
  Select,
  Stack,
  Table,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import {
  addOffer,
  fetchGrantableRoles,
  type DataLevel,
  type OfferInput,
} from "../../../api/dataProductOffers";
import { fetchTableAccess, type TableAccess } from "../../../api/roleRights";
import { MultiSelect } from "../../../components/MultiSelect";
import { usePanelDirty } from "../../../components/list/panelLeave";
import { FormChecks, FormField, FormFooter, FormLayout } from "../../../components/form/FormLayout";
import { AccessCells, AccessHeads } from "../../security/AccessCells";
import { offerSummary, TERMS_OF_USE } from "./offerTerms";

export interface OfferDataset {
  id: number;
  name: string;
}

const EMPTY: OfferInput = {
  name: "",
  role_id: "",
  data_level: "full",
  sample_views: [],
  dq_results: true,
  profiles: false,
  open: false,
  duration_days: null,
  purpose: "",
  usage: "",
  limitations: "",
};

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** The role's access to each column of a dataset it reads. */
function roleColumns(access: TableAccess, role: string) {
  return access.columns.flatMap((c) =>
    c.access.filter((a) => a.role === role && a.read).map((a) => ({ column: c.name, access: a })),
  );
}

/** Whether ``role`` reads at least one column of every dataset. */
function reaches(accesses: TableAccess[], role: string): boolean {
  return accesses.every((a) => roleColumns(a, role).length > 0);
}

/** Whether every column ``role`` reads of every dataset is read as public data. */
function readsAsPublic(accesses: TableAccess[], role: string): boolean {
  return accesses.every((a) => {
    const cols = roleColumns(a, role);
    return cols.length > 0 && cols.every((c) => c.access.reach === "public");
  });
}

export function AddOfferForm({
  productId,
  datasets,
  views,
  onSaved,
  onCancel,
}: {
  productId: string;
  /** The product's datasets, each offer covering all of them. */
  datasets: OfferDataset[];
  /** The views over the product's datasets a sample may name. */
  views: OfferDataset[];
  onSaved: () => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [offer, setOffer] = useState<OfferInput>(EMPTY);
  const [roles, setRoles] = useState<string[] | null>(null);
  const [accesses, setAccesses] = useState<TableAccess[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const set = (patch: Partial<OfferInput>) => setOffer((o) => ({ ...o, ...patch }));
  // Leaving the panel asks first once anything is filled in.
  usePanelDirty(JSON.stringify(offer) !== JSON.stringify(EMPTY), t("dataProductOffers.addTitle"));

  const datasetIds = datasets.map((d) => d.id).join(",");
  useEffect(() => {
    let live = true;
    Promise.all([
      fetchGrantableRoles(productId),
      Promise.all(
        datasetIds
          .split(",")
          .filter(Boolean)
          .map((id) => fetchTableAccess(Number(id))),
      ),
    ]).then(
      ([grantable, access]) => {
        if (!live) return;
        setRoles(grantable);
        setAccesses(access);
      },
      (e: unknown) => live && setError(message(e)),
    );
    return () => {
      live = false;
    };
  }, [productId, datasetIds]);

  // The roles the owner may grant that reach every dataset of the product.
  const reachable = useMemo(
    () => (roles && accesses ? roles.filter((r) => reaches(accesses, r)) : []),
    [roles, accesses],
  );
  const publicData = !!accesses && !!offer.role_id && readsAsPublic(accesses, offer.role_id);
  const synthetic = offer.data_level === "synthetic";
  const sampleMissing = offer.data_level === "sample" && offer.sample_views.length === 0;
  const ready = offer.name.trim() !== "" && offer.role_id !== "" && !sampleMissing;

  const save = () =>
    addOffer(productId, {
      ...offer,
      name: offer.name.trim(),
      purpose: offer.purpose.trim(),
      usage: offer.usage.trim(),
      limitations: offer.limitations.trim(),
    }).then(onSaved, (e: unknown) => setError(message(e)));

  if (error && roles === null) {
    return (
      <Alert color="red" data-testid="add-offer-error">
        {error}
      </Alert>
    );
  }
  if (roles === null || accesses === null)
    return <Loader size="sm" data-testid="add-offer-loading" />;

  return (
    <FormLayout data-testid="add-offer-form">
      <FormField size="medium">
        <TextInput
          label={t("dataProductOffers.name")}
          required
          value={offer.name}
          onChange={(e) => set({ name: e.currentTarget.value })}
          data-testid="add-offer-name"
        />
      </FormField>
      <FormField size="medium" hint={t("dataProductOffers.roleHint")}>
        <Select
          label={t("dataProductOffers.role")}
          required
          searchable
          data={reachable}
          value={offer.role_id || null}
          onChange={(v) => set({ role_id: v ?? "", open: false })}
          nothingFoundMessage={t("dataProductOffers.noGrantableRole")}
          data-testid="add-offer-role"
        />
      </FormField>
      {offer.role_id !== "" && (
        <FormField size="full">
          <Stack gap="xs" data-testid="add-offer-role-access">
            <Text size="sm" fw={500}>
              {t("dataProductOffers.roleAccess", { role: offer.role_id })}
            </Text>
            {accesses.map((access) => (
              <div key={access.table_id} data-testid={`add-offer-role-access-${access.table_id}`}>
                <Text size="xs" ff="monospace" c="dimmed">
                  {access.name}
                </Text>
                <Table.ScrollContainer minWidth={480}>
                  <Table withTableBorder verticalSpacing={4} fz="xs">
                    <Table.Thead>
                      <Table.Tr>
                        <Table.Th>{t("roleRights.column")}</Table.Th>
                        <AccessHeads />
                      </Table.Tr>
                    </Table.Thead>
                    <Table.Tbody>
                      {roleColumns(access, offer.role_id).map(({ column, access: a }) => (
                        <Table.Tr key={column}>
                          <Table.Td ff="monospace">{column}</Table.Td>
                          <AccessCells access={a} />
                        </Table.Tr>
                      ))}
                    </Table.Tbody>
                  </Table>
                </Table.ScrollContainer>
              </div>
            ))}
          </Stack>
        </FormField>
      )}
      <FormField size="short">
        <Select
          label={t("dataProductOffers.dataLevel")}
          allowDeselect={false}
          data={(["full", "sample", "synthetic"] as const).map((level) => ({
            value: level,
            label: t(`dataProductOffers.level_${level}`),
          }))}
          value={offer.data_level}
          onChange={(v) => {
            const level = (v ?? "full") as DataLevel;
            set({
              data_level: level,
              sample_views: level === "sample" ? offer.sample_views : [],
              profiles: level === "synthetic" ? false : offer.profiles,
            });
          }}
          data-testid="add-offer-level"
        />
      </FormField>
      {offer.data_level === "sample" && (
        <FormField size="long" hint={t("dataProductOffers.sampleViewsHint")}>
          <MultiSelect
            label={t("dataProductOffers.sampleViewsLabel")}
            required
            options={views.map((v) => ({ id: String(v.id), label: v.name }))}
            value={offer.sample_views.map(String)}
            onChange={(ids) => set({ sample_views: ids.map(Number) })}
            placeholder={
              views.length === 0
                ? t("dataProductOffers.noSampleViews")
                : t("dataProductOffers.sampleViewsPlaceholder")
            }
          />
        </FormField>
      )}
      <FormField size="short" hint={t("dataProductOffers.durationHint")}>
        <NumberInput
          label={t("dataProductOffers.duration")}
          min={1}
          allowDecimal={false}
          value={offer.duration_days ?? ""}
          onChange={(v) => set({ duration_days: typeof v === "number" && v > 0 ? v : null })}
          data-testid="add-offer-duration"
        />
      </FormField>
      <FormChecks>
        <FormField size="short">
          <Checkbox
            label={t("dataProductOffers.includeQuality")}
            checked={offer.dq_results}
            onChange={(e) => set({ dq_results: e.currentTarget.checked })}
            data-testid="add-offer-dq"
          />
        </FormField>
        <FormField
          size="short"
          help={synthetic ? t("dataProductOffers.syntheticNoProfiles") : undefined}
        >
          <Checkbox
            label={t("dataProductOffers.includeProfiles")}
            checked={offer.profiles}
            disabled={synthetic}
            onChange={(e) => set({ profiles: e.currentTarget.checked })}
            data-testid="add-offer-profiles"
          />
        </FormField>
        <FormField
          size="short"
          help={publicData ? t("dataProductOffers.openHint") : t("dataProductOffers.openNotPublic")}
        >
          <Checkbox
            label={t("dataProductOffers.open")}
            checked={offer.open}
            disabled={!publicData}
            onChange={(e) => set({ open: e.currentTarget.checked })}
            data-testid="add-offer-open"
          />
        </FormField>
      </FormChecks>
      <FormField size="full">
        <Divider label={t("dataProductOffers.termsOfUse")} labelPosition="left" />
      </FormField>
      {TERMS_OF_USE.map((term) => (
        <FormField key={term} size="full" hint={t(`dataProductOffers.termHint_${term}`)}>
          <Textarea
            label={t(`dataProductOffers.term_${term}`)}
            autosize
            minRows={2}
            value={offer[term]}
            onChange={(e) => set({ [term]: e.currentTarget.value })}
            data-testid={`add-offer-${term}`}
          />
        </FormField>
      ))}
      <FormField size="full">
        <Text size="sm" data-testid="add-offer-summary">
          {offerSummary(offer, datasets.length, t)}
        </Text>
      </FormField>
      <FormFooter
        onSave={save}
        onCancel={onCancel}
        saveProps={{ disabled: !ready, "data-testid": "add-offer-save" }}
        problem={error}
        data-testid="add-offer-footer"
      />
    </FormLayout>
  );
}
