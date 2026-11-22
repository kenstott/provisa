// Copyright (c) 2026 Kenneth Stott
// Canary: 7c3e9a15-b46d-4f82-9d07-1a5c8e2b6f93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: the table of the instances of any class, on a Wikidata source. The class is found by
// name (only classes that have instances are offered) or, with the search empty, browsed from the
// most-used top-level classes with their member counts; its broader and narrower classes lead on.
// Its columns are picked from the properties its instances most use, a filter is asked for when
// it is too large to read whole, and "Use this class" closes the picker under its columns.

import { useEffect, useState } from "react";
import { Button, Group, Loader, MultiSelect, Select, Table, Text, UnstyledButton } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FieldGroup, FormField } from "../../components/form/FormLayout";
import {
  buildWikidataClassTable,
  fetchWikidataBrowse,
  fetchWikidataClass,
  searchWikidata,
  type WikidataBrowseClass,
  type WikidataClass,
  type WikidataEntity,
} from "../../api/wikidata";
import { PICKED_AT_FIRST, tableNameOf, type WikidataChoice } from "./wikidataChoice";

const SEARCH_DELAY_MS = 300;

const entityLabel = (e: { id: string; label: string | null }) =>
  e.label ? `${e.label} (${e.id})` : e.id;

/** Items found by name as a searchable Select's options, the one chosen kept among them. */
function useEntitySearch(
  language: string,
  kind: "class" | "item",
  onError: (message: string) => void,
  chosen: string | null,
) {
  const [text, setText] = useState("");
  const [found, setFound] = useState<WikidataEntity[]>([]);
  const [searching, setSearching] = useState(false);
  useEffect(() => {
    const q = text.trim();
    // A choice puts its option's label in the box: that is not a search, and the options stay.
    if (!q || q === chosen || found.some((e) => entityLabel(e) === q)) return;
    let current = true;
    const timer = setTimeout(() => {
      setSearching(true);
      searchWikidata(q, language, kind)
        .then((hits) => current && setFound(hits))
        .catch((e: Error) => current && onError(e.message))
        .finally(() => current && setSearching(false));
    }, SEARCH_DELAY_MS);
    return () => {
      current = false;
      clearTimeout(timer);
      setSearching(false);
    };
  }, [text, language, kind, onError, found, chosen]);
  return { text, setText, found, searching };
}

/** With the class search empty: the most-used top-level classes, each opened by a click. */
function BrowseClasses({
  language,
  onOpen,
  onError,
}: {
  language: string;
  onOpen: (id: string, label: string) => void;
  onError: (message: string) => void;
}) {
  const { t } = useTranslation();
  const [classes, setClasses] = useState<WikidataBrowseClass[] | null>(null);
  useEffect(() => {
    fetchWikidataBrowse(language)
      .then(setClasses)
      .catch((e: Error) => onError(e.message));
  }, [language, onError]);
  return (
    <FormField size="long" hint={classes === null ? t("wikidataTable.browseLoading") : undefined}>
      <Text size="sm" fw={500}>
        {t("wikidataTable.browseTitle")}
      </Text>
      {classes !== null && (
        <Table
          className="data-table"
          highlightOnHover
          verticalSpacing={4}
          data-testid="wikidata-class-browse"
        >
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("wikidataTable.class")}</Table.Th>
              <Table.Th ta="right">{t("wikidataTable.members")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {classes.map((c) => (
              <Table.Tr key={c.id}>
                <Table.Td>
                  <UnstyledButton
                    fz="sm"
                    c="var(--accent)"
                    onClick={() => onOpen(c.id, c.label ?? c.id)}
                    data-testid={`wikidata-browse-${c.id}`}
                  >
                    {entityLabel(c)}
                  </UnstyledButton>
                </Table.Td>
                <Table.Td ta="right">{c.instance_count.toLocaleString(language)}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </FormField>
  );
}

interface Props {
  language: string;
  /** "Use this class": the class's table, to be registered with the others. */
  onUse: (choice: WikidataChoice) => void;
  /** Another class was opened: what the last one gave is no longer the table. */
  onChange: () => void;
}

export function WikidataClassPicker({ language, onUse, onChange }: Props) {
  const { t } = useTranslation();
  const [error, setError] = useState<string | null>(null);
  const [klass, setKlass] = useState<WikidataClass | null>(null);
  const [classLabel, setClassLabel] = useState("");
  const openLabel = klass ? entityLabel({ id: klass.id, label: classLabel }) : null;
  const classSearch = useEntitySearch(language, "class", setError, openLabel);
  const valueSearch = useEntitySearch(language, "item", setError, null);
  const [picked, setPicked] = useState<string[]>([]);
  const [filterProperty, setFilterProperty] = useState<string | null>(null);
  const [filterValue, setFilterValue] = useState<string | null>(null);
  const [building, setBuilding] = useState(false);
  const [loadingClass, setLoadingClass] = useState(false);

  const openClass = (id: string | null, label: string) => {
    if (!id) return;
    setError(null);
    setKlass(null);
    setPicked([]);
    setFilterProperty(null);
    setFilterValue(null);
    setClassLabel(label);
    onChange();
    setLoadingClass(true);
    fetchWikidataClass(id, language)
      .then((found) => {
        setKlass(found);
        setPicked(found.properties.slice(0, PICKED_AT_FIRST).map((p) => p.id));
      })
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoadingClass(false));
  };

  const neighbour = (list: "broader" | "narrower", id: string | null) => {
    const found = klass?.[list].find((c) => c.id === id);
    if (found) openClass(found.id, found.label ?? found.id);
  };

  const filterReady = !klass?.filter_required || Boolean(filterProperty && filterValue);
  const applyClass = async () => {
    if (!klass) return;
    setError(null);
    setBuilding(true);
    try {
      const table = await buildWikidataClassTable({
        class_id: klass.id,
        properties: klass.properties
          .filter((p) => picked.includes(p.id))
          .map(({ id, label, data_type, item }) => ({ id, label, data_type, item })),
        filter:
          filterProperty && filterValue ? { property: filterProperty, value: filterValue } : null,
        language,
      });
      // A label with no Latin letters gives no name: the table is named by the class's id.
      onUse({ tableName: tableNameOf(classLabel) || klass.id.toLowerCase(), ...table });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBuilding(false);
    }
  };

  const neighbourData = (list: "broader" | "narrower") =>
    (klass?.[list] ?? []).map((c) => ({ value: c.id, label: entityLabel(c) }));

  return (
    <>
      <FormField size="long" help={t("wikidataTable.classHint")}>
        <Select
          label={t("wikidataTable.class")}
          searchable
          searchValue={classSearch.text}
          onSearchChange={classSearch.setText}
          value={klass?.id ?? null}
          onChange={(id) => {
            const hit = classSearch.found.find((e) => e.id === id);
            openClass(id, hit?.label ?? id ?? "");
          }}
          // The class open is an option even when it was reached by browsing, not found by name.
          data={[
            ...(klass && !classSearch.found.some((e) => e.id === klass.id)
              ? [{ value: klass.id, label: openLabel as string }]
              : []),
            ...classSearch.found.map((e) => ({ value: e.id, label: entityLabel(e) })),
          ]}
          filter={({ options }) => options}
          rightSection={
            classSearch.searching ? (
              <Loader size="xs" data-testid="wikidata-class-searching" />
            ) : undefined
          }
          nothingFoundMessage={
            classSearch.searching ? t("wikidataTable.searching") : t("wikidataTable.nothingFound")
          }
          data-testid="wikidata-class-search"
        />
      </FormField>
      {loadingClass && (
        <FormField size="long">
          <Group gap="xs" data-testid="wikidata-class-loading">
            <Loader size="xs" />
            <Text size="sm">{t("wikidataTable.loadingClass")}</Text>
          </Group>
        </FormField>
      )}
      {!klass && !loadingClass && !classSearch.text.trim() && (
        <BrowseClasses language={language} onOpen={openClass} onError={setError} />
      )}
      {klass && (
        <>
          <FormField size="full">
            <Text size="sm" data-testid="wikidata-class-count">
              {t("wikidataTable.instances", {
                value: klass.instance_count.toLocaleString(language),
              })}
            </Text>
          </FormField>
          <FieldGroup row>
            <FormField size="medium">
              <Select
                label={t("wikidataTable.broader")}
                value={null}
                onChange={(id) => neighbour("broader", id)}
                data={neighbourData("broader")}
                disabled={klass.broader.length === 0}
                data-testid="wikidata-class-broader"
              />
            </FormField>
            <FormField size="medium">
              <Select
                label={t("wikidataTable.narrower")}
                searchable
                value={null}
                onChange={(id) => neighbour("narrower", id)}
                data={neighbourData("narrower")}
                disabled={klass.narrower.length === 0}
                data-testid="wikidata-class-narrower"
              />
            </FormField>
          </FieldGroup>
          <FormField size="long" hint={t("wikidataTable.columnsHint")}>
            <MultiSelect
              label={t("wikidataTable.columns")}
              searchable
              value={picked}
              onChange={setPicked}
              data={klass.properties.map((p) => ({ value: p.id, label: entityLabel(p) }))}
              data-testid="wikidata-class-columns"
            />
          </FormField>
          {klass.filter_required && (
            <FieldGroup row>
              <FormField
                size="medium"
                hint={t("wikidataTable.filterRequired", {
                  limit: klass.limit.toLocaleString(language),
                })}
              >
                <Select
                  required
                  label={t("wikidataTable.filterProperty")}
                  value={filterProperty}
                  onChange={setFilterProperty}
                  data={klass.properties
                    .filter((p) => p.item)
                    .map((p) => ({ value: p.id, label: entityLabel(p) }))}
                  data-testid="wikidata-filter-property"
                />
              </FormField>
              <FormField size="medium">
                <Select
                  required
                  label={t("wikidataTable.filterValue")}
                  searchable
                  searchValue={valueSearch.text}
                  onSearchChange={valueSearch.setText}
                  value={filterValue}
                  onChange={setFilterValue}
                  data={valueSearch.found.map((e) => ({ value: e.id, label: entityLabel(e) }))}
                  filter={({ options }) => options}
                  nothingFoundMessage={t("wikidataTable.nothingFound")}
                  data-testid="wikidata-filter-value"
                />
              </FormField>
            </FieldGroup>
          )}
          {/* The picker's closing action, on its own row under its columns. */}
          <FormField size="full">
            <Button
              variant="default"
              onClick={() => void applyClass()}
              disabled={!filterReady || building}
              loading={building}
              data-testid="wikidata-class-use"
            >
              {t("wikidataTable.useClass")}
            </Button>
          </FormField>
        </>
      )}
      {error && (
        <FormField size="full">
          <Text c="red" size="sm" data-testid="wikidata-table-error">
            {error}
          </Text>
        </FormField>
      )}
    </>
  );
}
