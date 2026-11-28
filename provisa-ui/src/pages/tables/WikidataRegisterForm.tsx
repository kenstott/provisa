// Copyright (c) 2026 Kenneth Stott
// Canary: 2f6c8d41-9a3e-4b57-8c12-e7d4a5b9f063
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923 (amended 2026-11-20, WIKIDATA REGISTRATION): registering tables from a Wikidata source.
// The starter tables are a multi-select, each with its description and its columns (its query's
// SELECT variables, every binding text), and the custom table -- the table of a class, or a
// hand-written SPARQL query -- sits in the same step. Continue registers every chosen table at
// once with its default name and columns; descriptions, types and aliases are refined afterwards
// on each table. Nothing waits on a live query: Preview is optional, and the rows land in the
// replica load that follows registration.

import { useEffect, useState, type ReactNode } from "react";
import { PanelBody } from "../../components/list/PanelBody";
import { Button, SegmentedControl, Text, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  FormField,
  FormFooter,
  FormIdentifiers,
  FormLayout,
} from "../../components/form/FormLayout";
import { useRequiredFields } from "../../components/form/useRequiredFields";
import { MultiSelect } from "../../components/MultiSelect";
import { fetchWikidataStarters, type WikidataStarter } from "../../api/wikidata";
import { useRegisterTables, type TableRegistrationOutcome } from "../../hooks/useTableQueries";
import { useQueryPreview } from "../../hooks/useQueryPreview";
import { draftFromRegistration } from "./draftTable";
import { previewedColumns } from "./queryTables";
import { useSuggestColumnAliases } from "../../hooks/useNameSuggestions";
import { QueryTableFields } from "./QueryTableFields";
import { registeredColumns } from "./registrationColumns";
import { registrationOf } from "./registrationDefaults";
import { registrationInput } from "./useCreateTable";
import { sparqlColumnsNow, useSparqlColumns } from "./useSparqlColumns";
import { WikidataClassPicker } from "./WikidataClassPicker";
import type { WikidataChoice } from "./wikidataChoice";

/** A Wikidata table registers under its source's one schema, named after its type. */
const SPARQL_SCHEMA = "sparql";

type CustomMode = "none" | "class" | "sparql";

/** One table as it is registered: its name, alias, description, query and columns. */
interface Chosen {
  tableName: string;
  alias: string;
  description: string;
  query: string;
  columns: { name: string; data_type: string }[];
}

interface Props {
  /** The source and domain fields, at the top as in every register form. */
  identifiers: ReactNode;
  sourceId: string;
  domainId: string;
  /** Marks the required source and domain fields above left empty; true when none is. */
  validateIdentifiers: () => boolean;
  /** Only where regions exist: `{ region }`. */
  regionInput: Record<string, unknown>;
  roleIds: string[];
  /** Names already registered on this source: such a starter is not offered again. */
  registeredNames: string[];
  suggestTableAlias: (tableName: string, domainId: string, sourceId: string) => Promise<string>;
  language: string;
  onRegisteredMany: (outcomes: TableRegistrationOutcome[]) => void;
  /** Absent in source setup (REQ-2234): no Cancel there. */
  onCancel?: () => void;
  setError: (e: string | null) => void;
  problem?: string | null;
}

/** A query's preview, run on demand: its rows, or why there are none. */
function usePreview(sourceId: string) {
  const { t } = useTranslation();
  const { preview } = useQueryPreview();
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [problem, setProblem] = useState<string | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const run = async (query: string) => {
    setPreviewing(true);
    setProblem(null);
    try {
      const res = await preview({ sourceType: "sparql", sourceId, query });
      setRows(res.error ? [] : res.rows);
      if (res.error) setProblem(t("registerTableForm.previewFailed", { error: res.error }));
      else if (res.rows.length === 0) setProblem(t("registerTableForm.previewNoRows"));
    } finally {
      setPreviewing(false);
    }
  };
  const clear = () => {
    setRows([]);
    setProblem(null);
  };
  return { rows, problem, previewing, run, clear };
}

export function WikidataRegisterForm(props: Props) {
  const { t } = useTranslation();
  const { sourceId, domainId, setError } = props;
  const { registerTables } = useRegisterTables();
  // REQ-2361: each column's alias is the naming authority's suggestion.
  const suggestColumnAliases = useSuggestColumnAliases();
  const [starters, setStarters] = useState<WikidataStarter[]>([]);
  const [picked, setPicked] = useState<string[]>([]);
  const [mode, setMode] = useState<CustomMode>("none");
  // What the custom table derives from its class or its query; reset when either changes.
  const [classTable, setClassTable] = useState<WikidataChoice | null>(null);
  const [tableName, setTableName] = useState("");
  const [alias, setAlias] = useState("");
  const [query, setQuery] = useState("");
  const [registering, setRegistering] = useState(false);
  const preview = usePreview(sourceId);
  const hasQuery = mode === "sparql" || (mode === "class" && classTable !== null);
  // REQ-1968 (amended 2026-11-22): the custom table's name, when there is one, is required.
  const required = useRequiredFields<"tableName">(hasQuery ? { tableName } : {});
  // A hand-written query's columns are its SELECT variables; a class table's come with it.
  const parsed = useSparqlColumns(
    hasQuery && (classTable === null || query !== classTable.query) ? query : "",
  );

  useEffect(() => {
    fetchWikidataStarters()
      .then(setStarters)
      .catch((e: Error) => setError(e.message));
  }, [setError]);

  const resetCustom = () => {
    setClassTable(null);
    setTableName("");
    setAlias("");
    setQuery("");
    preview.clear();
  };
  const chooseMode = (next: CustomMode) => {
    resetCustom();
    setMode(next);
  };
  const takeClassTable = (choice: WikidataChoice) => {
    preview.clear();
    setClassTable(choice);
    setTableName(choice.tableName);
    setQuery(choice.query);
    setAlias("");
  };
  // The alias follows the custom table's name, as an authored table's does.
  useEffect(() => {
    if (!tableName || !domainId || !sourceId) return;
    let live = true;
    props.suggestTableAlias(tableName, domainId, sourceId).then((a) => live && setAlias(a));
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- suggestTableAlias is a stable hook callback
  }, [tableName, domainId, sourceId]);

  const offered = starters.filter((s) => !props.registeredNames.includes(s.name));
  const options = offered.map((s) => ({
    id: s.name,
    label: `${t(`wikidataTable.starters.${s.name}`)} — ${t(
      `wikidataTable.starterDescriptions.${s.name}`,
    )} (${s.columns.map((c) => c.name).join(", ")})`,
  }));

  /** The custom table as it is registered, or why it cannot be; null when there is none. A
   * class table's columns come with it until its query is edited; any other query's are its
   * SELECT variables. */
  const custom = async (): Promise<Chosen | string | null> => {
    if (mode === "none") return null;
    if (mode === "class" && classTable === null) return t("wikidataRegister.classNotUsed");
    if (!query.trim()) return t("registerTableForm.errorSparqlQuery");
    try {
      const columns =
        classTable !== null && query === classTable.query
          ? classTable.columns
          : await sparqlColumnsNow(query, parsed);
      return { tableName: tableName.trim(), alias, description: "", query: query.trim(), columns };
    } catch (e) {
      return e instanceof Error ? e.message : String(e);
    }
  };

  const chosenStarters = (): Chosen[] =>
    offered
      .filter((s) => picked.includes(s.name))
      .map((s) => ({
        tableName: s.name,
        alias: "",
        description: t(`wikidataTable.starterDescriptions.${s.name}`),
        query: s.query,
        columns: s.columns,
      }));

  const inputOf = async (chosen: Chosen): Promise<Record<string, unknown>> => {
    const alias =
      chosen.alias || (await props.suggestTableAlias(chosen.tableName, domainId, sourceId));
    const registration = registrationOf({
      sourceId,
      domainId,
      regionInput: props.regionInput,
      schemaName: SPARQL_SCHEMA,
      tableName: chosen.tableName,
      alias,
      description: chosen.description,
      watermarkColumn: "",
      columns: registeredColumns(
        await previewedColumns(
          chosen.columns.map((c) => ({ name: c.name, dataType: c.data_type })),
          props.roleIds,
          suggestColumnAliases,
        ),
        null,
      ),
      uniqueConstraints: [],
      pagingKind: null,
      pagination: null,
      pagingCeilingRows: null,
      extra: { discover: false, queryTemplate: chosen.query },
    });
    const draft = draftFromRegistration(registration);
    return registrationInput(draft, registration, draft.cacheTtl ?? null);
  };

  const register = async () => {
    setError(null);
    // Each required field left empty is marked where it is. The fields above are checked last, so
    // focus lands on the first missing one on screen: theirs when one is, else this form's.
    const ownFilled = required.validate();
    const identified = props.validateIdentifiers();
    if (!ownFilled || !identified) return;
    const own = await custom();
    if (typeof own === "string") {
      setError(own);
      return;
    }
    const tables = [...chosenStarters(), ...(own ? [own] : [])];
    if (tables.length === 0) {
      setError(t("wikidataRegister.nothingChosen"));
      return;
    }
    setRegistering(true);
    try {
      // One table after another: the alias each is suggested is one lazy query, and a second
      // call while one is in flight aborts it.
      const inputs: Record<string, unknown>[] = [];
      for (const table of tables) inputs.push(await inputOf(table));
      const outcomes = await registerTables(inputs);
      if (outcomes.some((o) => o.result.success)) {
        props.onRegisteredMany(outcomes);
        return;
      }
      setError(
        outcomes
          .map((o) =>
            t("registerManyTables.refused", { table: o.tableName, message: o.result.message }),
          )
          .join("\n"),
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRegistering(false);
    }
  };

  const shownColumns =
    classTable !== null && query === classTable.query
      ? classTable.columns
      : parsed.state === "read"
        ? parsed.columns
        : [];

  return (
    <PanelBody>
      <FormLayout data-tour="tables-form" className="form-card">
        <FormIdentifiers>
          {props.identifiers}
          <FormField
            size="long"
            help={t("wikidataRegister.startersHint")}
            data-testid="wikidata-starters-field"
          >
            <MultiSelect
              label={t("wikidataRegister.starters")}
              options={options}
              value={picked}
              onChange={setPicked}
              placeholder={t("wikidataRegister.startersPlaceholder")}
            />
          </FormField>
          <FormField size="long">
            <SegmentedControl
              aria-label={t("wikidataRegister.customLabel")}
              value={mode}
              onChange={(v) => chooseMode(v as CustomMode)}
              data={[
                { value: "none", label: t("wikidataRegister.customNone") },
                { value: "class", label: t("wikidataTable.modeClass") },
                { value: "sparql", label: t("wikidataRegister.customSparql") },
              ]}
              data-testid="wikidata-custom-mode"
            />
          </FormField>
        </FormIdentifiers>
        {mode === "class" && (
          <WikidataClassPicker
            language={props.language}
            onUse={takeClassTable}
            onChange={resetCustom}
          />
        )}
        {hasQuery && (
          <>
            <FormField size="medium">
              <TextInput
                {...required.field("tableName")}
                label={t("registerTableForm.neo4jTableNameLabel")}
                value={tableName}
                onChange={(e) => setTableName(e.currentTarget.value)}
                data-testid="register-table-sparql-table-name"
              />
            </FormField>
            <FormField size="medium">
              <TextInput
                label={t("registerTableForm.aliasLabel")}
                value={alias}
                onChange={(e) => setAlias(e.currentTarget.value)}
                placeholder={t("registerTableForm.aliasPlaceholder")}
                data-testid="register-table-alias"
              />
            </FormField>
            <QueryTableFields
              sourceType="sparql"
              pipeline={false}
              query={query}
              setQuery={(q) => {
                preview.clear();
                setQuery(q);
              }}
              args={[]}
              setArgs={() => undefined}
              samples={{}}
              setSamples={() => undefined}
              previewing={preview.previewing}
              canPreview={Boolean(query.trim())}
              onPreview={() => void preview.run(query.trim())}
              previewRows={preview.rows}
              previewProblem={preview.problem}
              columns={shownColumns}
            />
            <FormField size="full">
              <Text size="sm" c="dimmed" data-testid="wikidata-custom-columns">
                {shownColumns.length > 0
                  ? t("wikidataRegister.columns", {
                      columns: shownColumns.map((c) => c.name).join(", "),
                    })
                  : parsed.state === "refused"
                    ? parsed.problem
                    : t("wikidataRegister.columnsReading")}
              </Text>
            </FormField>
          </>
        )}
        <FormFooter
          panel
          problem={props.problem}
          onCancel={props.onCancel}
          cancelProps={{ "data-testid": "register-table-cancel" }}
          action={
            <Button
              onClick={() => void register()}
              loading={registering}
              data-testid="register-table-submit"
            >
              {t("registerTableForm.continue")}
            </Button>
          }
        />
      </FormLayout>
    </PanelBody>
  );
}
