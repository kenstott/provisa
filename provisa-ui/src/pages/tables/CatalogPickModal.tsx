// Copyright (c) 2026 Kenneth Stott
// Canary: 3d8f1b62-7a04-4c95-b1e6-5f2c8d0a7e19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: From catalog and Refresh from catalog.
//
// From catalog opens the pick list of each catalog of the table's domain: the asset the table is
// linked to, else the one for the same physical table, is preselected, the rest are ranked by
// name and searchable, and the steward picks one or none. The picked asset's description, column
// descriptions (matched by column name, adjustable one by one), owners, tags and glossary terms
// are then offered beside the table's own, each shown with its catalog, and the steward accepts
// what the table takes. Refresh from catalog opens that review on the asset the table is linked
// to. Nothing is matched by a model; tags and terms apply only where Provisa has one of the same
// name, and owners are shown, never stored.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Anchor,
  Badge,
  Button,
  Checkbox,
  Group,
  Loader,
  Modal,
  Radio,
  ScrollArea,
  Select,
  Stack,
  Table,
  Tabs,
  Text,
  TextInput,
} from "@mantine/core";
import { ExternalLink, TriangleAlert } from "lucide-react";
import { FormChecks, FormField, FormFooter, FormLayout } from "../../components/form/FormLayout";
import {
  applyCatalogOffer,
  fetchCatalogCandidates,
  fetchCatalogOffer,
  type CatalogCandidates,
  type CatalogOffer,
  type Imported,
} from "../../api/catalogLinks";

const NONE = "";

/** Which asset is being reviewed: picked from a catalog's list, or the linked one (refresh). */
interface Reviewing {
  catalog: string;
  ref?: string;
}

export function CatalogPickModal({
  tableId,
  opened,
  refresh,
  onClose,
  onApplied,
}: {
  tableId: number;
  opened: boolean;
  /** Refresh from catalog: review the asset the table is linked to in this catalog. */
  refresh?: string | null;
  onClose: () => void;
  onApplied: (catalog: string, imported: Imported) => void;
}) {
  const { t } = useTranslation();
  return (
    <Modal
      opened={opened}
      onClose={onClose}
      size="xl"
      title={refresh ? t("catalogLink.reviewTitle") : t("catalogLink.pickTitle")}
      data-testid="catalog-pick-modal"
    >
      {opened && (
        // Remounted on each opening, so a new pick starts from the list.
        <PickFlow
          key={refresh ?? ""}
          tableId={tableId}
          refresh={refresh ?? null}
          onClose={onClose}
          onApplied={onApplied}
        />
      )}
    </Modal>
  );
}

function PickFlow({
  tableId,
  refresh,
  onClose,
  onApplied,
}: {
  tableId: number;
  refresh: string | null;
  onClose: () => void;
  onApplied: (catalog: string, imported: Imported) => void;
}) {
  const [reviewing, setReviewing] = useState<Reviewing | null>(
    refresh ? { catalog: refresh } : null,
  );
  return reviewing ? (
    <OfferReview
      tableId={tableId}
      reviewing={reviewing}
      onBack={refresh ? undefined : () => setReviewing(null)}
      onApplied={(imported) => {
        onApplied(reviewing.catalog, imported);
        onClose();
      }}
    />
  ) : (
    <PickList tableId={tableId} onPick={setReviewing} onNone={onClose} />
  );
}

function PickList({
  tableId,
  onPick,
  onNone,
}: {
  tableId: number;
  onPick: (reviewing: Reviewing) => void;
  onNone: () => void;
}) {
  const { t } = useTranslation();
  const [lists, setLists] = useState<CatalogCandidates[] | null>(null);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [tab, setTab] = useState<string | null>(null);
  const [chosen, setChosen] = useState<Record<string, string>>({});

  const load = useCallback(
    (text?: string) =>
      fetchCatalogCandidates(tableId, text)
        .then((found) => {
          setError("");
          setLists(found);
          setTab((current) => current ?? found[0]?.catalog ?? null);
          setChosen((prev) => {
            const next = { ...prev };
            for (const list of found) {
              if (!(list.catalog in next)) next[list.catalog] = list.preselected ?? NONE;
            }
            return next;
          });
        })
        .catch((e) => setError(String(e))),
    [tableId],
  );
  useEffect(() => {
    void load();
  }, [load]);

  if (error)
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="catalog-pick-error">
        {error}
      </Alert>
    );
  if (!lists)
    return (
      <Group gap="xs">
        <Loader size="sm" />
        <Text>{t("catalogLink.searching")}</Text>
      </Group>
    );
  if (lists.length === 0)
    return (
      <Text c="dimmed" size="sm" data-testid="catalog-pick-none">
        {t("catalogLink.noCatalogs")}
      </Text>
    );

  const list = lists.find((l) => l.catalog === tab);
  const picked = tab ? chosen[tab] : NONE;
  return (
    <FormLayout>
      <FormField size="long">
        <TextInput
          placeholder={t("catalogLink.searchPlaceholder")}
          value={search}
          onChange={(e) => setSearch(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") void load(search);
          }}
          data-testid="catalog-pick-search"
        />
      </FormField>
      <FormField size="full">
        <Tabs value={tab} onChange={setTab}>
          <Tabs.List>
            {lists.map((l) => (
              <Tabs.Tab
                key={l.catalog}
                value={l.catalog}
                data-testid={`catalog-pick-tab-${l.catalog}`}
              >
                {l.catalog} · {l.provider}
              </Tabs.Tab>
            ))}
          </Tabs.List>
        </Tabs>
        {list?.error ? (
          <Alert
            color="red"
            icon={<TriangleAlert size={16} />}
            data-testid="catalog-pick-list-error"
          >
            {list.error}
          </Alert>
        ) : (
          <ScrollArea.Autosize mah={360}>
            <Radio.Group
              value={picked}
              onChange={(v) => tab && setChosen((prev) => ({ ...prev, [tab]: v }))}
            >
              <Stack gap={6} data-testid="catalog-pick-assets">
                <Radio
                  value={NONE}
                  label={t("catalogLink.pickNone")}
                  data-testid="catalog-pick-asset-none"
                />
                {(list?.assets ?? []).map((a) => (
                  <Radio
                    key={a.ref}
                    value={a.ref}
                    data-testid={`catalog-pick-asset-${a.ref}`}
                    label={
                      <Stack gap={0}>
                        <Group gap="xs">
                          <Text size="sm" fw={500}>
                            {a.name}
                          </Text>
                          {a.ref === list?.linked && (
                            <Badge size="xs" variant="light">
                              {t("catalogLink.linked")}
                            </Badge>
                          )}
                          {a.ref === list?.preselected && a.ref !== list?.linked && (
                            <Badge size="xs" variant="light" color="green">
                              {t("catalogLink.samePhysical")}
                            </Badge>
                          )}
                        </Group>
                        {a.description && (
                          <Text size="xs" c="dimmed" lineClamp={2}>
                            {a.description}
                          </Text>
                        )}
                      </Stack>
                    }
                  />
                ))}
              </Stack>
            </Radio.Group>
          </ScrollArea.Autosize>
        )}
      </FormField>
      <FormFooter
        onCancel={onNone}
        cancelProps={{ "data-testid": "catalog-pick-cancel" }}
        action={
          <Button
            disabled={!tab || !picked}
            onClick={() => tab && picked && onPick({ catalog: tab, ref: picked })}
            data-testid="catalog-pick-next"
          >
            {t("catalogLink.next")}
          </Button>
        }
      />
    </FormLayout>
  );
}

const key = (column: string | null, name: string) => `${column ?? ""}|${name}`;

function OfferReview({
  tableId,
  reviewing,
  onBack,
  onApplied,
}: {
  tableId: number;
  reviewing: Reviewing;
  onBack?: () => void;
  onApplied: (imported: Imported) => void;
}) {
  const { t } = useTranslation();
  const [offer, setOffer] = useState<CatalogOffer | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [takeDescription, setTakeDescription] = useState(false);
  const [mapping, setMapping] = useState<Record<string, string | null>>({});
  const [takeColumn, setTakeColumn] = useState<Record<string, boolean>>({});
  const [takeTag, setTakeTag] = useState<Record<string, boolean>>({});
  const [takeTerm, setTakeTerm] = useState<Record<string, string>>({});

  useEffect(() => {
    fetchCatalogOffer(tableId, reviewing.catalog, reviewing.ref)
      .then((o) => {
        setOffer(o);
        setTakeDescription(
          !!o.description.catalog && o.description.catalog !== o.description.current,
        );
        const map: Record<string, string | null> = {};
        const take: Record<string, boolean> = {};
        for (const c of o.columns) {
          map[c.column] = c.catalog_column;
          const theirs = o.catalog_columns.find((cc) => cc.name === c.catalog_column);
          take[c.column] = !!theirs?.description && theirs.description !== c.current;
        }
        setMapping(map);
        setTakeColumn(take);
      })
      .catch((e) => setError(String(e)));
  }, [tableId, reviewing.catalog, reviewing.ref]);

  const byCatalogColumn = useMemo(
    () => Object.fromEntries((offer?.catalog_columns ?? []).map((c) => [c.name, c])),
    [offer],
  );

  if (error)
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="catalog-offer-error">
        {error}
      </Alert>
    );
  if (!offer)
    return (
      <Group gap="xs">
        <Loader size="sm" />
        <Text>{t("catalogLink.reading")}</Text>
      </Group>
    );

  const held = new Set(offer.current_tags.map(([c, tag]) => key(c || null, tag)));
  // Every tag the catalog offers, at the table or at the Provisa column it is mapped to.
  const offeredTags: { column: string | null; name: string; tag: string | null }[] = [
    ...offer.tags.map((x) => ({ column: null, name: x.name, tag: x.tag ?? null })),
    ...offer.columns.flatMap((c) => {
      const theirs = mapping[c.column] ? byCatalogColumn[mapping[c.column] as string] : undefined;
      return (theirs?.tags ?? []).map((x) => ({
        column: c.column,
        name: x.name,
        tag: x.tag ?? null,
      }));
    }),
  ];
  const offeredTerms: { column: string; name: string; term: string | null }[] =
    offer.columns.flatMap((c) => {
      const theirs = mapping[c.column] ? byCatalogColumn[mapping[c.column] as string] : undefined;
      return (theirs?.terms ?? []).map((x) => ({
        column: c.column,
        name: x.name,
        term: x.term ?? null,
      }));
    });

  const apply = async () => {
    setBusy(true);
    setError("");
    try {
      const imported = await applyCatalogOffer(tableId, offer.catalog, {
        ref: offer.asset.ref,
        description: takeDescription,
        columns: Object.fromEntries(
          Object.entries(mapping).filter(
            (entry): entry is [string, string] => entry[1] !== null && !!takeColumn[entry[0]],
          ),
        ),
        tags: offeredTags
          .filter((x) => x.tag && takeTag[key(x.column, x.tag)])
          .map((x) => ({ column: x.column, tag: x.tag as string })),
        terms: takeTerm,
      });
      onApplied(imported);
    } catch (e) {
      setError(String(e));
      setBusy(false);
    }
  };

  const source = `${offer.catalog} · ${offer.provider}`;
  return (
    <FormLayout data-testid="catalog-offer">
      <FormField size="full">
        <Group justify="space-between" wrap="nowrap">
          <Text size="sm" fw={600} data-testid="catalog-offer-asset">
            {offer.asset.name}
          </Text>
          {offer.asset.url && (
            <Anchor href={offer.asset.url} target="_blank" rel="noreferrer" size="sm">
              <Group gap={4}>
                {t("catalogLink.openInCatalog", { catalog: offer.catalog })}
                <ExternalLink size={12} />
              </Group>
            </Anchor>
          )}
        </Group>
      </FormField>

      <FormChecks>
        <FormField size="long">
          <Checkbox
            label={t("catalogLink.takeDescription", { catalog: source })}
            checked={takeDescription}
            disabled={!offer.description.catalog}
            onChange={(e) => setTakeDescription(e.currentTarget.checked)}
            data-testid="catalog-offer-description"
          />
        </FormField>
      </FormChecks>
      <FormField size="full">
        <Table withTableBorder layout="fixed">
          <Table.Tbody>
            <Table.Tr>
              <Table.Td w="20%">
                <Text size="xs" c="dimmed">
                  {t("catalogLink.inProvisa")}
                </Text>
              </Table.Td>
              <Table.Td style={{ whiteSpace: "pre-wrap" }}>{offer.description.current}</Table.Td>
            </Table.Tr>
            <Table.Tr>
              <Table.Td>
                <Text size="xs" c="dimmed">
                  {source}
                </Text>
              </Table.Td>
              <Table.Td
                style={{ whiteSpace: "pre-wrap" }}
                data-testid="catalog-offer-description-text"
              >
                {offer.description.catalog}
              </Table.Td>
            </Table.Tr>
          </Table.Tbody>
        </Table>
      </FormField>

      <FormField size="full">
        <Stack gap={4}>
          <Text size="sm" fw={600}>
            {t("catalogLink.columns")}
          </Text>
          <Table withTableBorder layout="fixed" data-testid="catalog-offer-columns">
            <Table.Thead>
              <Table.Tr>
                <Table.Th w={40} />
                <Table.Th>{t("catalogLink.column")}</Table.Th>
                <Table.Th>{t("catalogLink.inProvisa")}</Table.Th>
                <Table.Th>{t("catalogLink.catalogColumn", { catalog: offer.catalog })}</Table.Th>
                <Table.Th>{source}</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {offer.columns.map((c) => {
                const theirs = mapping[c.column]
                  ? byCatalogColumn[mapping[c.column] as string]
                  : undefined;
                return (
                  <Table.Tr key={c.column} data-testid={`catalog-offer-column-${c.column}`}>
                    <Table.Td>
                      <Checkbox
                        aria-label={c.column}
                        checked={!!takeColumn[c.column] && !!theirs}
                        disabled={!theirs}
                        onChange={(e) => {
                          const checked = e.currentTarget.checked;
                          setTakeColumn((prev) => ({ ...prev, [c.column]: checked }));
                        }}
                        data-testid={`catalog-offer-take-${c.column}`}
                      />
                    </Table.Td>
                    <Table.Td>{c.column}</Table.Td>
                    <Table.Td style={{ whiteSpace: "pre-wrap" }}>{c.current}</Table.Td>
                    <Table.Td>
                      <Select
                        size="xs"
                        data={offer.catalog_columns.map((cc) => ({
                          value: cc.name,
                          label: cc.name,
                        }))}
                        value={mapping[c.column]}
                        clearable
                        placeholder={t("catalogLink.noColumn")}
                        onChange={(v) => setMapping((prev) => ({ ...prev, [c.column]: v }))}
                        data-testid={`catalog-offer-map-${c.column}`}
                      />
                    </Table.Td>
                    <Table.Td style={{ whiteSpace: "pre-wrap" }}>
                      {theirs?.description ?? ""}
                    </Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </Stack>
      </FormField>

      {offer.owners.length > 0 && (
        <FormField size="full">
          <Stack gap={2} data-testid="catalog-offer-owners">
            <Text size="sm" fw={600}>
              {t("catalogLink.owners", { catalog: source })}
            </Text>
            <Text size="sm">{offer.owners.join(", ")}</Text>
            <Text size="xs" c="dimmed">
              {t("catalogLink.ownersShownOnly")}
            </Text>
          </Stack>
        </FormField>
      )}

      {(offeredTags.length > 0 || offer.terms.length > 0 || offeredTerms.length > 0) && (
        <FormField size="full">
          <Stack gap={4} data-testid="catalog-offer-tags">
            <Text size="sm" fw={600}>
              {t("catalogLink.tagsAndTerms", { catalog: source })}
            </Text>
            <FormChecks>
              {offeredTags.map((x) => {
                const k = key(x.column, x.tag ?? x.name);
                const label = `${x.column ?? t("catalogLink.theTable")}: ${x.name}`;
                return x.tag ? (
                  <FormField key={k} size="long">
                    <Checkbox
                      label={label}
                      checked={held.has(k) || !!takeTag[k]}
                      disabled={held.has(k)}
                      onChange={(e) => {
                        const checked = e.currentTarget.checked;
                        setTakeTag((prev) => ({ ...prev, [k]: checked }));
                      }}
                      data-testid={`catalog-offer-tag-${x.column ?? ""}-${x.tag}`}
                    />
                  </FormField>
                ) : (
                  <Group key={k} gap="xs">
                    <Text size="sm">{label}</Text>
                    <Badge size="xs" color="gray" variant="light">
                      {t("catalogLink.noSuchTag")}
                    </Badge>
                  </Group>
                );
              })}
              {offeredTerms.map((x) =>
                x.term ? (
                  <FormField key={`${x.column}|${x.name}`} size="long">
                    <Checkbox
                      label={`${x.column}: ${x.name}`}
                      checked={
                        takeTerm[x.column] === x.term || offer.current_terms[x.column] === x.term
                      }
                      disabled={offer.current_terms[x.column] === x.term}
                      onChange={(e) => {
                        const checked = e.currentTarget.checked;
                        setTakeTerm((prev) => {
                          const next = { ...prev };
                          if (checked) next[x.column] = x.term as string;
                          else delete next[x.column];
                          return next;
                        });
                      }}
                      data-testid={`catalog-offer-term-${x.column}-${x.term}`}
                    />
                  </FormField>
                ) : (
                  <Group key={`${x.column}|${x.name}`} gap="xs">
                    <Text size="sm">{`${x.column}: ${x.name}`}</Text>
                    <Badge size="xs" color="gray" variant="light">
                      {t("catalogLink.noSuchTerm")}
                    </Badge>
                  </Group>
                ),
              )}
              {offer.terms.map((x) => (
                <Group key={`table|${x.name}`} gap="xs">
                  <Text size="sm">{`${t("catalogLink.theTable")}: ${x.name}`}</Text>
                  <Badge size="xs" color="gray" variant="light">
                    {t("catalogLink.termOnTable")}
                  </Badge>
                </Group>
              ))}
            </FormChecks>
          </Stack>
        </FormField>
      )}

      <FormFooter
        onSave={apply}
        saving={busy}
        saveProps={{ "data-testid": "catalog-offer-apply" }}
        action={
          onBack ? (
            <Button variant="default" onClick={onBack} data-testid="catalog-offer-back">
              {t("catalogLink.back")}
            </Button>
          ) : undefined
        }
      />
    </FormLayout>
  );
}
