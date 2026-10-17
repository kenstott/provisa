// Copyright (c) 2026 Kenneth Stott
// Canary: c0374f40-f939-4160-b6f1-fd399af87d80
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: Explore's Data Science area. The Python tab runs code in a Web Worker on the Pyodide the
// provisa-notebook package serves at /pyodide, reading Provisa as the page's user (bridge.ts).
import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Box,
  Button,
  Code,
  Group,
  Image,
  Loader,
  ScrollArea,
  Stack,
  Table,
  Tabs,
  Text,
  Title,
  useComputedColorScheme,
} from "@mantine/core";
import CodeMirror from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { bridgeAuth } from "../dataScience/bridge";
import type { FromWorker, PyodideManifest, RunValue, ToWorker } from "../dataScience/protocol";

/** The first cell: load the client and say who it reads as. The same lines run on a desktop. */
export const STARTER_CODE = "import provisa_client\nprovisa_client.sign_in()\n";

type Status = "checking" | "missing" | "loading" | "ready" | "running" | "failed";

interface Output {
  stdout: string;
  value: RunValue;
  images: string[];
  error: string | null;
}

/** The worker is a seam for tests: they hand the page a double. */
export type MakeWorker = () => Pick<Worker, "postMessage" | "terminate" | "onmessage">;

const defaultWorker: MakeWorker = () =>
  new Worker(new URL("../dataScience/pythonWorker.ts", import.meta.url), { type: "module" });

/** The files provisa-notebook serves, or null when it is not installed. */
async function readManifest(): Promise<PyodideManifest | null> {
  const res = await fetch("/pyodide/manifest.json");
  if (!res.ok || !(res.headers.get("content-type") ?? "").includes("json")) return null;
  return (await res.json()) as PyodideManifest;
}

export function DataSciencePage({ makeWorker = defaultWorker }: { makeWorker?: MakeWorker }) {
  const { t } = useTranslation();
  return (
    <Box p="md" data-testid="data-science-page">
      <Title order={3} mb="sm">
        {t("dataScience.title")}
      </Title>
      <Tabs defaultValue="python" keepMounted={false}>
        <Tabs.List>
          <Tabs.Tab value="python" data-testid="data-science-tab-python">
            {t("dataScience.tabPython")}
          </Tabs.Tab>
        </Tabs.List>
        <Tabs.Panel value="python" pt="md">
          <PythonTab makeWorker={makeWorker} />
        </Tabs.Panel>
      </Tabs>
    </Box>
  );
}

function PythonTab({ makeWorker }: { makeWorker: MakeWorker }) {
  const { t } = useTranslation();
  const theme = useComputedColorScheme("light") === "dark" ? oneDark : undefined;
  const [status, setStatus] = useState<Status>("checking");
  const [failure, setFailure] = useState<string | null>(null);
  const [code, setCode] = useState(STARTER_CODE);
  const [output, setOutput] = useState<Output | null>(null);
  const worker = useRef<ReturnType<MakeWorker> | null>(null);
  const nextRun = useRef(0);

  useEffect(() => {
    let live = true;
    void (async () => {
      const manifest = await readManifest();
      if (!live) return;
      if (manifest === null) {
        setStatus("missing");
        return;
      }
      setStatus("loading");
      const w = makeWorker();
      worker.current = w;
      w.onmessage = (event: MessageEvent<FromWorker>) => {
        const message = event.data;
        if (message.type === "ready") setStatus("ready");
        else if (message.type === "failed") {
          setFailure(message.error);
          setStatus("failed");
        } else {
          setOutput(message);
          setStatus("ready");
        }
      };
      const init: ToWorker = { type: "init", manifest, auth: await bridgeAuth() };
      w.postMessage(init);
    })();
    return () => {
      live = false;
      worker.current?.terminate();
      worker.current = null;
    };
  }, [makeWorker]);

  const run = useCallback(async () => {
    const w = worker.current;
    if (w === null || status !== "ready") return;
    setStatus("running");
    // The identity is read at the moment of the run: a rotated token or a newly picked role,
    // org or environment is what the code reads as.
    const auth: ToWorker = { type: "auth", auth: await bridgeAuth() };
    w.postMessage(auth);
    const message: ToWorker = { type: "run", id: ++nextRun.current, code };
    w.postMessage(message);
  }, [code, status]);

  if (status === "checking") return <Loader size="sm" />;
  if (status === "missing") {
    return (
      <Alert color="yellow" title={t("dataScience.missingTitle")} data-testid="data-science-missing">
        <Text size="sm">{t("dataScience.missingBody")}</Text>
      </Alert>
    );
  }
  return (
    <Stack gap="sm">
      <Box
        style={{ border: "1px solid var(--mantine-color-default-border)" }}
        onKeyDownCapture={(e) => {
          // Ctrl+Enter (Cmd+Enter) runs, before the editor takes the Enter as a new line.
          if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
            e.preventDefault();
            e.stopPropagation();
            void run();
          }
        }}
      >
        <CodeMirror
          value={code}
          onChange={setCode}
          theme={theme}
          minHeight="160px"
          maxHeight="360px"
          aria-label={t("dataScience.editorLabel")}
          data-testid="data-science-editor"
        />
      </Box>
      <Group gap="sm">
        <Button
          size="xs"
          onClick={() => void run()}
          disabled={status !== "ready"}
          loading={status === "running"}
          data-testid="data-science-run"
        >
          {t("dataScience.run")}
        </Button>
        <Text size="xs" c="dimmed" data-testid="data-science-status">
          {status === "loading"
            ? t("dataScience.loading")
            : status === "failed"
              ? t("dataScience.failed")
              : t("dataScience.runHint")}
        </Text>
      </Group>
      {failure !== null && (
        <Alert color="red" data-testid="data-science-failure">
          <Code block>{failure}</Code>
        </Alert>
      )}
      {output !== null && <RunOutput output={output} />}
    </Stack>
  );
}

function RunOutput({ output }: { output: Output }) {
  const { t } = useTranslation();
  const { value } = output;
  return (
    <Stack gap="xs" data-testid="data-science-output">
      {output.stdout !== "" && (
        <Code block data-testid="data-science-stdout">
          {output.stdout}
        </Code>
      )}
      {output.error !== null && (
        <Code block c="red" data-testid="data-science-error">
          {output.error}
        </Code>
      )}
      {value.kind === "text" && (
        <Code block data-testid="data-science-value">
          {value.text}
        </Code>
      )}
      {value.kind === "table" && (
        <Box>
          <ScrollArea.Autosize mah={420}>
            <Table striped withTableBorder fz="xs" data-testid="data-science-table">
              <Table.Thead>
                <Table.Tr>
                  {value.columns.map((c) => (
                    <Table.Th key={c}>{c}</Table.Th>
                  ))}
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {value.rows.map((row, i) => (
                  <Table.Tr key={i}>
                    {row.map((cell, j) => (
                      <Table.Td key={j}>{cell === null ? "" : String(cell)}</Table.Td>
                    ))}
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </ScrollArea.Autosize>
          <Text size="xs" c="dimmed" mt={4}>
            {t("dataScience.rowsShown", { shown: value.rows.length, total: value.total })}
          </Text>
        </Box>
      )}
      {output.images.map((png, i) => (
        <Image
          key={i}
          src={`data:image/png;base64,${png}`}
          alt={t("dataScience.figure", { n: i + 1 })}
          maw={720}
          data-testid="data-science-figure"
        />
      ))}
    </Stack>
  );
}
