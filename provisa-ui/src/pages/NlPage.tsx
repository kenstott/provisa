// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useRef, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation, Trans } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Group,
  Loader,
  Paper,
  ScrollArea,
  SimpleGrid,
  Stack,
  Table,
  Text,
  Textarea,
  UnstyledButton,
} from "@mantine/core";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useAuth } from "../context/AuthContext";
import { submitNlQuery, streamNlResult, type NlBranchEvent } from "../api/admin";

const EXPLORER_ROUTES: Record<string, { path: string; stateKey: string }> = {
  sql: { path: "/sql", stateKey: "sql" },
  graphql: { path: "/query", stateKey: "query" },
  cypher: { path: "/graph", stateKey: "query" },
  grpc: { path: "/grpc", stateKey: "grpcMethod" },
  jsonapi: { path: "/jsonapi", stateKey: "jsonapiUrl" },
  openapi: { path: "/openapi", stateKey: "openApiUrl" },
};

const GUIDE_KEY = "provisa.nl.guide.collapsed";

function GuidanceBanner() {
  const { t } = useTranslation();
  const [collapsed, setCollapsed] = useState(
    () => localStorage.getItem(GUIDE_KEY) === "1",
  );

  const examples = [
    t("nlPage.example1"),
    t("nlPage.example2"),
    t("nlPage.example3"),
  ];

  const toggle = useCallback(() => {
    setCollapsed((prev) => {
      const next = !prev;
      localStorage.setItem(GUIDE_KEY, next ? "1" : "0");
      return next;
    });
  }, []);
  return (
    <Paper withBorder radius="md" data-testid="nl-guide">
      <UnstyledButton
        onClick={toggle}
        aria-expanded={!collapsed}
        data-testid="nl-guide-toggle"
        p="sm"
        style={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "space-between" }}
      >
        <Text fw={600} size="sm">{t("nlPage.guideTitle")}</Text>
        {collapsed ? <ChevronRight size={14} aria-hidden="true" /> : <ChevronDown size={14} aria-hidden="true" />}
      </UnstyledButton>
      {!collapsed && (
        <Stack gap="sm" p="sm" pt={0}>
          <Text size="sm" c="dimmed">
            {t("nlPage.guideDesc")}
          </Text>
          <Stack component="ul" gap={4} style={{ margin: 0, paddingLeft: 18 }}>
            <Text component="li" size="sm" c="dimmed">
              <Trans i18nKey="nlPage.guideRuleNames" t={t}>
                Use the names of your entities, not synonyms (<Text component="em" fs="normal" c="blue" span>Orders</Text>, not <Text component="em" fs="normal" c="blue" span>purchases</Text>)
              </Trans>
            </Text>
            <Text component="li" size="sm" c="dimmed">
              {t("nlPage.guideRuleFilters")}
            </Text>
            <Text component="li" size="sm" c="dimmed">
              {t("nlPage.guideRuleSchema")}
            </Text>
          </Stack>
          <Text size="xs" fw={700} tt="uppercase" c="dimmed">
            {t("nlPage.guideExamplesLabel")}
          </Text>
          <Stack component="ul" gap={4} style={{ margin: 0, paddingLeft: 18 }}>
            {examples.map((ex) => (
              <Text component="li" key={ex} size="sm" fs="italic" c="indigo">
                {ex}
              </Text>
            ))}
          </Stack>
        </Stack>
      )}
    </Paper>
  );
}

type BranchState = {
  query: string | null;
  result: unknown | null;
  error: string | null;
  loading: boolean;
};

const EMPTY_BRANCH: BranchState = { query: null, result: null, error: null, loading: false };

const TARGETS = ["sql", "graphql", "cypher", "grpc", "jsonapi", "openapi"] as const;
type Target = (typeof TARGETS)[number];

const LABELS: Record<Target, string> = {
  sql: "SQL", graphql: "GraphQL", cypher: "Cypher",
  grpc: "gRPC", jsonapi: "JSON:API", openapi: "OpenAPI",
};

export function NlPage() {
  const { t } = useTranslation();
  const { role } = useAuth();
  const navigate = useNavigate();
  const NL_QUESTION_KEY = "nl-question";
  const NL_BRANCHES_KEY = "nl-branches";
  const [question, setQuestion] = useState(() => localStorage.getItem(NL_QUESTION_KEY) ?? "");
  const [submitting, setSubmitting] = useState(false);
  const [globalError, setGlobalError] = useState<string | null>(null);
  const [branches, setBranches] = useState<Record<Target, BranchState>>(() => {
    try {
      const saved = localStorage.getItem(NL_BRANCHES_KEY);
      const parsed = saved ? JSON.parse(saved) : {};
      return {
        sql: parsed.sql ?? EMPTY_BRANCH,
        graphql: parsed.graphql ?? EMPTY_BRANCH,
        cypher: parsed.cypher ?? EMPTY_BRANCH,
        grpc: parsed.grpc ?? EMPTY_BRANCH,
        jsonapi: parsed.jsonapi ?? EMPTY_BRANCH,
        openapi: parsed.openapi ?? EMPTY_BRANCH,
      };
    } catch {
      return {
        sql: EMPTY_BRANCH, graphql: EMPTY_BRANCH, cypher: EMPTY_BRANCH,
        grpc: EMPTY_BRANCH, jsonapi: EMPTY_BRANCH, openapi: EMPTY_BRANCH,
      };
    }
  });
  const [hasResults, setHasResults] = useState(
    () => localStorage.getItem(NL_BRANCHES_KEY) !== null,
  );
  const cancelRef = useRef<(() => void) | null>(null);

  const saveBranches = useCallback((next: Record<Target, BranchState>) => {
    localStorage.setItem(NL_BRANCHES_KEY, JSON.stringify(next));
    setBranches(next);
  }, []);

  const handleSubmit = useCallback(async () => {
    const q = question.trim();
    if (!q || submitting) return;

    cancelRef.current?.();
    cancelRef.current = null;

    const roleId = role ? role.id : "default";
    setGlobalError(null);
    setHasResults(true);
    setSubmitting(true);
    saveBranches({
      sql: { ...EMPTY_BRANCH, loading: true },
      graphql: { ...EMPTY_BRANCH, loading: true },
      cypher: { ...EMPTY_BRANCH, loading: true },
      grpc: { ...EMPTY_BRANCH, loading: true },
      jsonapi: { ...EMPTY_BRANCH, loading: true },
      openapi: { ...EMPTY_BRANCH, loading: true },
    });

    let jobId: string;
    try {
      const res = await submitNlQuery(q, roleId);
      jobId = res.job_id;
    } catch (e) {
      setGlobalError(e instanceof Error ? e.message : String(e));
      setSubmitting(false);
      saveBranches({
        sql: EMPTY_BRANCH, graphql: EMPTY_BRANCH, cypher: EMPTY_BRANCH,
        grpc: EMPTY_BRANCH, jsonapi: EMPTY_BRANCH, openapi: EMPTY_BRANCH,
      });
      return;
    }

    const stop = streamNlResult(
      jobId,
      (event: NlBranchEvent) => {
        const t = event.target as Target;
        setBranches((prev) => {
          const next = { ...prev, [t]: { query: event.query, result: event.result, error: event.error, loading: false } };
          localStorage.setItem(NL_BRANCHES_KEY, JSON.stringify(next));
          return next;
        });
      },
      (_state) => {
        setSubmitting(false);
        setBranches((prev) => {
          const next = { ...prev };
          for (const t of TARGETS) {
            if (next[t].loading) next[t] = { ...EMPTY_BRANCH };
          }
          localStorage.setItem(NL_BRANCHES_KEY, JSON.stringify(next));
          return next;
        });
      },
      (msg) => {
        setGlobalError(msg);
        setSubmitting(false);
        setBranches((prev) => {
          const next = { ...prev };
          for (const t of TARGETS) {
            if (next[t].loading) next[t] = { ...EMPTY_BRANCH, error: msg };
          }
          localStorage.setItem(NL_BRANCHES_KEY, JSON.stringify(next));
          return next;
        });
      },
    );
    cancelRef.current = stop;
  }, [question, submitting, role, saveBranches]);

  const openInExplorer = useCallback((target: Target, _query: string) => {
    const route = EXPLORER_ROUTES[target];
    if (!route.stateKey) {
      navigate(route.path);
      return;
    }
    navigate(route.path, { state: { [route.stateKey]: _query, autoRun: true } });
  }, [navigate]);

  return (
    <Stack gap="md" p="md" style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
      <GuidanceBanner />
      <Group align="flex-start" gap="sm" wrap="nowrap">
        <Textarea
          aria-label={t("nlPage.questionLabel")}
          placeholder={t("nlPage.questionPlaceholder")}
          value={question}
          rows={2}
          autosize
          minRows={2}
          style={{ flex: 1 }}
          data-testid="nl-question-input"
          onChange={(e) => { setQuestion(e.currentTarget.value); localStorage.setItem(NL_QUESTION_KEY, e.currentTarget.value); }}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              void handleSubmit();
            }
          }}
        />
        <Button
          disabled={submitting || !question.trim()}
          onClick={() => void handleSubmit()}
          loading={submitting}
          data-testid="nl-submit-button"
        >
          {submitting ? t("nlPage.generating") : t("nlPage.generate")}
        </Button>
        {/* Hand the same question to the MCP chat assistant, which runs it agentically. */}
        <Button
          variant="light"
          disabled={!question.trim()}
          onClick={() =>
            navigate("/explore", { state: { mcpQuestion: question.trim() } })
          }
          data-testid="nl-mcp-chat-button"
        >
          {t("nlPage.mcpChat")}
        </Button>
      </Group>

      {globalError && (
        <Alert color="red" variant="light" data-testid="nl-global-error">
          {globalError}
        </Alert>
      )}

      {hasResults && (
        <SimpleGrid data-tour="nl-panels" cols={{ base: 1, md: 3 }} spacing="sm">
          {TARGETS.map((tk) => (
            <BranchPanel key={tk} label={LABELS[tk]} target={tk} branch={branches[tk]} onOpen={openInExplorer} />
          ))}
        </SimpleGrid>
      )}
    </Stack>
  );
}

function BranchPanel({
  label,
  target,
  branch,
  onOpen,
}: {
  label: string;
  target: Target;
  branch: BranchState;
  onOpen: (target: Target, query: string) => void;
}) {
  const { t } = useTranslation();
  const notApplicable = branch.error === "NOT_APPLICABLE";
  return (
    <Paper withBorder radius="md" style={{ display: "flex", flexDirection: "column", minHeight: 200, maxHeight: 400, overflow: "hidden" }} data-testid={`nl-branch-panel-${target}`}>
      <Group justify="space-between" px="sm" py={6} style={{ borderBottom: "1px solid var(--mantine-color-default-border)" }}>
        <Badge variant="light" size="sm">{label}</Badge>
        {!branch.loading && branch.query && (
          <Button
            size="compact-xs"
            variant="light"
            title={t("nlPage.openInExplorer", { label })}
            onClick={() => onOpen(target, branch.query!)}
            data-testid={`nl-open-button-${target}`}
          >
            {t("nlPage.openIn", { label })}
          </Button>
        )}
      </Group>
      <ScrollArea style={{ flex: 1 }} p="sm">
        <Stack gap="xs">
          {branch.loading && (
            <Group gap={6}>
              <Loader size="xs" />
              <Text size="sm" c="dimmed" fs="italic">{t("nlPage.generating")}</Text>
            </Group>
          )}
          {!branch.loading && notApplicable && (
            <Text size="sm" c="dimmed" fs="italic">{t("nlPage.notApplicable")}</Text>
          )}
          {!branch.loading && !notApplicable && branch.error && (
            <Text size="sm" c="red">{branch.error}</Text>
          )}
          {!branch.loading && branch.query && (
            <Text
              component="pre"
              size="xs"
              c="cyan"
              style={{ margin: 0, whiteSpace: "pre-wrap", wordBreak: "break-word", fontFamily: "var(--mantine-font-family-monospace, monospace)" }}
            >
              {branch.query}
            </Text>
          )}
          {!branch.loading && !branch.query && !branch.error && (
            <Text size="sm" c="dimmed" fs="italic">{t("nlPage.noQueryGenerated")}</Text>
          )}
          {!branch.loading && branch.result != null && (
            <ResultTable result={branch.result} />
          )}
        </Stack>
      </ScrollArea>
    </Paper>
  );
}

function ResultTable({ result }: { result: unknown }) {
  const { t } = useTranslation();
  if (
    typeof result !== "object" ||
    result === null ||
    !Array.isArray((result as { rows?: unknown }).rows)
  ) {
    return (
      <Text component="pre" size="xs" c="dimmed" style={{ margin: 0, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
        {JSON.stringify(result, null, 2)}
      </Text>
    );
  }

  const { columns, rows } = result as { columns: string[]; rows: Record<string, unknown>[] };
  if (!rows.length) return <Text size="sm" c="dimmed" fs="italic">{t("nlPage.noRowsReturned")}</Text>;

  return (
    <Paper withBorder radius="sm" style={{ overflow: "auto" }}>
      <Table striped fz="xs">
        <Table.Thead>
          <Table.Tr>
            {columns.map((c) => (
              <Table.Th key={c}>{c}</Table.Th>
            ))}
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {rows.slice(0, 100).map((row, i) => (
            <Table.Tr key={i}>
              {columns.map((c, j) => (
                <Table.Td key={j}>{row[c] == null ? "" : String(row[c])}</Table.Td>
              ))}
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
      {rows.length > 100 && (
        <Text size="xs" c="dimmed" px="xs" py={4}>
          {t("nlPage.showingRows", { shown: 100, total: rows.length })}
        </Text>
      )}
    </Paper>
  );
}
