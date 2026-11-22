// Copyright (c) 2026 Kenneth Stott
// Canary: 17bf99d0-a64e-4206-9f08-e9e4ef4649da
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The query a query table runs, on Register Table: Cypher (REQ-1670), SPARQL (REQ-1683) or a
// MongoDB native query with its arguments (REQ-1980). Preview runs it and seeds the columns.

import {
  ActionIcon,
  Button,
  Group,
  Select,
  Switch,
  Table,
  Text,
  TextInput,
  Textarea,
} from "@mantine/core";
import { CircleHelp, Plus, X } from "lucide-react";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { Tip } from "../../components/Tip";
import { QueryPreviewRows } from "./QueryPreviewRows";
import { ARGUMENT_TYPES, argumentName, pipelineArgument } from "./queryTables";
import type { ColumnForm } from "./types";

interface Props {
  sourceType: string;
  /** A MongoDB native query (REQ-1980), with arguments, rather than Cypher or SPARQL. */
  pipeline: boolean;
  query: string;
  setQuery: (q: string) => void;
  /** The pipeline's arguments, as the table's parameter columns. */
  args: ColumnForm[];
  setArgs: (args: ColumnForm[]) => void;
  /** Each argument's sample value, by name, for the preview only: never registered. */
  samples: Record<string, string>;
  setSamples: (s: Record<string, string>) => void;
  previewing: boolean;
  canPreview: boolean;
  onPreview: () => void;
  previewRows: Record<string, unknown>[];
  /** Why the last preview shows no rows: it failed, naming why, or the query answered none. */
  previewProblem?: string | null;
  columns: { name: string }[];
}

// A native query's placeholders are written {{name}}: the texts that show one keep it as written.
const LITERAL = { name: "{{name}}", query_vector: "{{query_vector}}", phrase: "{{phrase}}" };

const KEYS = {
  sparql: ["sparqlQueryLabel", "sparqlQueryHint", "sparqlQueryPlaceholder"],
  mongodb: ["pipelineQueryLabel", "pipelineQueryHint", "pipelineQueryPlaceholder"],
  cosmosdb: ["cosmosQueryLabel", "cosmosQueryHint", "cosmosQueryPlaceholder"],
  elasticsearch: ["esSearchLabel", "esSearchHint", "esSearchPlaceholder"],
  qdrant: ["qdrantSearchLabel", "qdrantSearchHint", "qdrantSearchPlaceholder"],
  neo4j: ["neo4jCypherLabel", "neo4jCypherHint", "neo4jCypherPlaceholder"],
} as const;

function PipelineArguments({
  args,
  setArgs,
  samples,
  setSamples,
}: Pick<Props, "args" | "setArgs" | "samples" | "setSamples">) {
  const { t } = useTranslation();
  const update = (i: number, patch: Partial<ColumnForm>) =>
    setArgs(args.map((a, idx) => (idx === i ? { ...a, ...patch } : a)));
  return (
    <FormField size="full">
      <Group gap={6} align="center">
        <Text size="sm" fw={600}>
          {t("registerTableForm.pipelineArguments")}
        </Text>
        <Tip text={t("registerTableForm.pipelineArgumentsHelp")} named>
          <ActionIcon
            component="span"
            variant="subtle"
            color="gray"
            size="xs"
            role="button"
            tabIndex={0}
            aria-label={t("registerTableForm.pipelineArgumentsHelp")}
          >
            <CircleHelp size={14} />
          </ActionIcon>
        </Tip>
      </Group>
      {args.length > 0 && (
        <Table className="data-table" data-testid="register-table-pipeline-args">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("parametersPanel.name")}</Table.Th>
              <Table.Th>{t("parametersPanel.type")}</Table.Th>
              <Table.Th>{t("registerTableForm.pipelineArgumentSample")}</Table.Th>
              <Table.Th>{t("parametersPanel.required")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {args.map((a, i) => {
              const name = argumentName(a);
              return (
                <Table.Tr key={i} data-testid={`register-table-pipeline-arg-${i}`}>
                  <Table.Td>
                    <TextInput
                      aria-label={t("parametersPanel.name")}
                      value={name}
                      onChange={(e) => update(i, { name: `_nf_${e.currentTarget.value}` })}
                    />
                  </Table.Td>
                  <Table.Td>
                    <Select
                      aria-label={t("parametersPanel.type")}
                      data={ARGUMENT_TYPES}
                      value={a.dataType}
                      allowDeselect={false}
                      onChange={(v) => v && update(i, { dataType: v })}
                    />
                  </Table.Td>
                  <Table.Td>
                    <TextInput
                      aria-label={t("registerTableForm.pipelineArgumentSample")}
                      value={samples[name] ?? ""}
                      onChange={(e) => setSamples({ ...samples, [name]: e.currentTarget.value })}
                    />
                  </Table.Td>
                  <Table.Td>
                    <Switch
                      aria-label={t("parametersPanel.requiredAria", { name })}
                      checked={Boolean(a.requiredBySteward)}
                      onChange={(e) => update(i, { requiredBySteward: e.currentTarget.checked })}
                    />
                  </Table.Td>
                  <Table.Td>
                    <ActionIcon
                      variant="subtle"
                      color="gray"
                      aria-label={t("registerTableForm.pipelineArgumentRemove", { name })}
                      onClick={() => setArgs(args.filter((_, idx) => idx !== i))}
                    >
                      <X size={14} />
                    </ActionIcon>
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      )}
      <Button
        variant="default"
        size="xs"
        leftSection={<Plus size={14} />}
        onClick={() => setArgs([...args, pipelineArgument(`arg${args.length + 1}`, "text")])}
        data-testid="register-table-pipeline-arg-add"
      >
        {t("registerTableForm.pipelineArgumentAdd")}
      </Button>
    </FormField>
  );
}

export function QueryTableFields(props: Props) {
  const { t } = useTranslation();
  const { sourceType, pipeline, query, setQuery, previewing } = props;
  const [label, hint, placeholder] =
    KEYS[
      pipeline
        ? sourceType === "cosmosdb" || sourceType === "qdrant" || sourceType === "elasticsearch"
          ? sourceType
          : "mongodb"
        : sourceType === "sparql"
          ? "sparql"
          : "neo4j"
    ];
  return (
    <>
      <FormField size="full">
        <Textarea
          required
          autosize
          minRows={3}
          label={
            <>
              {t(`registerTableForm.${label}`)}{" "}
              <Text span fw="normal" c="dimmed" fz="xs">
                {t(`registerTableForm.${hint}`, LITERAL)}
              </Text>
            </>
          }
          value={query}
          onChange={(e) => setQuery(e.currentTarget.value)}
          placeholder={t(`registerTableForm.${placeholder}`, LITERAL)}
          data-testid={
            pipeline
              ? `register-table-${sourceType}-pipeline`
              : sourceType === "sparql"
                ? "register-table-sparql-query"
                : "register-table-neo4j-cypher"
          }
        />
      </FormField>
      {pipeline && <PipelineArguments {...props} />}
      <FormField size="full">
        <Button
          variant="default"
          onClick={props.onPreview}
          disabled={!props.canPreview || previewing}
          data-testid={`register-table-${sourceType}-preview`}
        >
          {previewing
            ? t("registerTableForm.neo4jPreviewing")
            : t("registerTableForm.neo4jPreviewButton")}
        </Button>
      </FormField>
      {props.previewProblem && (
        <FormField size="full">
          <Text size="sm" c="red" data-testid={`register-table-${sourceType}-preview-problem`}>
            {props.previewProblem}
          </Text>
        </FormField>
      )}
      {props.previewRows.length > 0 && (
        <FormField size="full">
          <QueryPreviewRows
            rows={props.previewRows}
            columns={props.columns}
            sourceType={sourceType}
          />
        </FormField>
      )}
    </>
  );
}
