// Copyright (c) 2026 Kenneth Stott
// Canary: 7c3e9a15-b46d-4f82-9d07-1a5c8e2b6f93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: a Wikidata source's table, chosen rather than written: a starter reference table, or
// the table of the instances of any class -- found by name or by browsing its broader and
// narrower classes, its columns picked from the properties its instances most use, and a filter
// asked for when the class is too large to read whole. Choosing fills the form's SPARQL query,
// name and columns; the table is registered as any SPARQL table.

import { useEffect, useState } from "react";
import { Button, MultiSelect, SegmentedControl, Select, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FieldGroup, FormField } from "../../components/form/FormLayout";
import {
  buildWikidataClassTable,
  fetchWikidataClass,
  fetchWikidataStarters,
  searchWikidata,
  type WikidataClass,
  type WikidataEntity,
  type WikidataStarter,
} from "../../api/wikidata";
import { PICKED_AT_FIRST, tableNameOf, type WikidataChoice } from "./wikidataChoice";

const SEARCH_DELAY_MS = 300;

const entityLabel = (e: { id: string; label: string | null }) =>
  e.label ? `${e.label} (${e.id})` : e.id;

/** Items found by name as a searchable Select's options, the one chosen kept among them. */
function useEntitySearch(language: string, onError: (message: string) => void) {
  const [text, setText] = useState("");
  const [found, setFound] = useState<WikidataEntity[]>([]);
  useEffect(() => {
    const q = text.trim();
    // A choice puts its option's label in the box: that is not a search, and the options stay.
    if (!q || found.some((e) => entityLabel(e) === q)) return;
    const timer = setTimeout(() => {
      searchWikidata(q, language)
        .then(setFound)
        .catch((e: Error) => onError(e.message));
    }, SEARCH_DELAY_MS);
    return () => clearTimeout(timer);
  }, [text, language, onError, found]);
  return { text, setText, found };
}

interface Props {
  language: string;
  onChoose: (choice: WikidataChoice) => void;
}

export function WikidataTableChooser({ language, onChoose }: Props) {
  const { t } = useTranslation();
  const [mode, setMode] = useState<"starter" | "class">("starter");
  const [error, setError] = useState<string | null>(null);
  const [starters, setStarters] = useState<WikidataStarter[]>([]);
  const [starter, setStarter] = useState<string | null>(null);
  const classSearch = useEntitySearch(language, setError);
  const valueSearch = useEntitySearch(language, setError);
  const [klass, setKlass] = useState<WikidataClass | null>(null);
  const [classLabel, setClassLabel] = useState("");
  const [picked, setPicked] = useState<string[]>([]);
  const [filterProperty, setFilterProperty] = useState<string | null>(null);
  const [filterValue, setFilterValue] = useState<string | null>(null);
  const [building, setBuilding] = useState(false);

  useEffect(() => {
    fetchWikidataStarters()
      .then(setStarters)
      .catch((e: Error) => setError(e.message));
  }, []);

  const chooseStarter = (name: string | null) => {
    setStarter(name);
    const found = starters.find((s) => s.name === name);
    if (found) onChoose({ tableName: found.name, query: found.query, columns: found.columns });
  };

  const openClass = (id: string | null, label: string) => {
    if (!id) return;
    setError(null);
    setKlass(null);
    setFilterProperty(null);
    setFilterValue(null);
    setClassLabel(label);
    fetchWikidataClass(id, language)
      .then((found) => {
        setKlass(found);
        setPicked(found.properties.slice(0, PICKED_AT_FIRST).map((p) => p.id));
      })
      .catch((e: Error) => setError(e.message));
  };

  const neighbour = (list: "broader" | "narrower", id: string | null) => {
    const found = klass?.[list].find((c) => c.id === id);
    if (found) openClass(found.id, found.label ?? found.id);
  };

  const filterReady = !klass?.filter_required || Boolean(filterProperty && filterValue);
  const buildTable = async () => {
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
      onChoose({ tableName: tableNameOf(classLabel) || klass.id.toLowerCase(), ...table });
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
      <FormField size="medium">
        <SegmentedControl
          aria-label={t("wikidataTable.modeLabel")}
          value={mode}
          onChange={(v) => setMode(v as "starter" | "class")}
          data={[
            { value: "starter", label: t("wikidataTable.modeStarter") },
            { value: "class", label: t("wikidataTable.modeClass") },
          ]}
          data-testid="wikidata-table-mode"
        />
      </FormField>
      {mode === "starter" && (
        <FormField size="medium" hint={t("wikidataTable.starterHint")}>
          <Select
            label={t("wikidataTable.starter")}
            value={starter}
            onChange={chooseStarter}
            data={starters.map((s) => ({
              value: s.name,
              label: t(`wikidataTable.starters.${s.name}`),
            }))}
            data-testid="wikidata-starter"
          />
        </FormField>
      )}
      {mode === "class" && (
        <>
          <FormField size="long" hint={t("wikidataTable.classHint")}>
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
              data={classSearch.found.map((e) => ({ value: e.id, label: entityLabel(e) }))}
              filter={({ options }) => options}
              nothingFoundMessage={t("wikidataTable.nothingFound")}
              data-testid="wikidata-class-search"
            />
          </FormField>
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
              <FormField size="medium">
                <Button
                  variant="default"
                  onClick={() => void buildTable()}
                  disabled={!filterReady || building}
                  data-testid="wikidata-class-use"
                >
                  {t("wikidataTable.useClass")}
                </Button>
              </FormField>
            </>
          )}
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
