// Copyright (c) 2026 Kenneth Stott
// Canary: c0374f40-f939-4160-b6f1-fd399af87d80
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: Explore's Data Science area. The Python tab runs code in a Web Worker on the Pyodide the
// provisa-notebook package serves at /pyodide, reading Provisa as the page's user (bridge.ts).
import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";
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
  useComputedColorScheme,
} from "@mantine/core";
import CodeMirror from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { bridgeAuth } from "../dataScience/bridge";
import {
  followScheme,
  seedNewNotebooks,
  startLiteBridge,
  type LiteApp,
} from "../dataScience/liteBridge";
import { reportRun, summarise } from "../dataScience/runReports";
import { useNavPayload } from "../hooks/useNavPayload";
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

/** The tabs of the area; a tab is mounted once first opened and kept, so switching keeps Python's
 * state and the notebook's kernel. */
type DataScienceTab = "python" | "notebook";

export function DataSciencePage({ makeWorker = defaultWorker }: { makeWorker?: MakeWorker }) {
  const { t } = useTranslation();
  const [tab, setTab] = useState<DataScienceTab>("python");
  const [opened, setOpened] = useState<Set<DataScienceTab>>(() => new Set(["python"]));
  const open = (next: string | null) => {
    if (next !== "python" && next !== "notebook") return;
    setTab(next);
    setOpened((was) => (was.has(next) ? was : new Set([...was, next])));
  };
  return (
    // The area fills what the shell gives it, as the other explorers do; the tab names it.
    <Box
      data-testid="data-science-page"
      style={{
        flex: 1,
        minHeight: 0,
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      <Tabs
        value={tab}
        onChange={open}
        keepMounted
        style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}
      >
        <Tabs.List px="md" pt="xs">
          <Tabs.Tab value="python" data-testid="data-science-tab-python">
            {t("dataScience.tabPython")}
          </Tabs.Tab>
          <Tabs.Tab value="notebook" data-testid="data-science-tab-notebook">
            {t("dataScience.tabNotebook")}
          </Tabs.Tab>
        </Tabs.List>
        <Tabs.Panel
          value="python"
          style={{
            flex: 1,
            minHeight: 0,
            display: tab === "python" ? "flex" : "none",
            flexDirection: "column",
          }}
        >
          {opened.has("python") && <PythonTab makeWorker={makeWorker} />}
        </Tabs.Panel>
        <Tabs.Panel
          value="notebook"
          style={{
            flex: 1,
            minHeight: 0,
            display: tab === "notebook" ? "flex" : "none",
            flexDirection: "column",
          }}
        >
          {opened.has("notebook") && <NotebookTab />}
        </Tabs.Panel>
      </Tabs>
    </Box>
  );
}

/** The JupyterLite site the notebook tab embeds, opened on the starter notebook. */
export const NOTEBOOK_URL = "/lite/notebooks/index.html?path=Provisa.ipynb";
/** How long the tab waits for the embedded app before it gives up handing it the user. */
const LITE_APP_WAIT_MS = 120_000;

function NotebookTab() {
  const { t } = useTranslation();
  const scheme = useComputedColorScheme("light");
  const [served, setServed] = useState<boolean | null>(null);
  const [app, setApp] = useState<LiteApp | null>(null);
  const frame = useRef<HTMLIFrameElement | null>(null);
  const stops = useRef<Array<() => void>>([]);

  // The notebook wears the app's colour scheme, and changes with it.
  useEffect(() => {
    if (app !== null) void followScheme(app, scheme);
  }, [app, scheme]);

  useEffect(() => {
    let live = true;
    void (async () => {
      const res = await fetch("/lite/jupyter-lite.json");
      const json = res.ok && (res.headers.get("content-type") ?? "").includes("json");
      if (live) setServed(json);
    })();
    return () => {
      live = false;
      stops.current.forEach((stop) => stop());
      stops.current = [];
    };
  }, []);

  // Once the embedded app is up, its kernels are handed the page's user and new notebooks open
  // on the starter cell.
  const connect = useCallback(() => {
    const started = Date.now();
    const appOf = () =>
      (frame.current?.contentWindow as (Window & { jupyterapp?: LiteApp }) | null)?.jupyterapp;
    const wait = setInterval(() => {
      const app = appOf();
      if (app === undefined && Date.now() - started < LITE_APP_WAIT_MS) return;
      clearInterval(wait);
      if (app === undefined) return;
      stops.current.push(startLiteBridge(appOf, bridgeAuth), seedNewNotebooks(app));
      setApp(app);
    }, 250);
    stops.current.push(() => clearInterval(wait));
  }, []);

  if (served === null) return <Loader size="sm" m="md" />;
  if (!served) {
    return (
      <Alert
        m="md"
        color="yellow"
        title={t("dataScience.missingTitle")}
        data-testid="data-science-notebook-missing"
      >
        <Text size="sm">{t("dataScience.missingBody")}</Text>
      </Alert>
    );
  }
  return (
    <iframe
      ref={frame}
      src={NOTEBOOK_URL}
      title={t("dataScience.notebookFrame")}
      onLoad={connect}
      data-testid="data-science-notebook"
      style={{ flex: 1, minHeight: 0, width: "100%", border: 0 }}
    />
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
  const splitRef = useRef<HTMLDivElement | null>(null);
  const [editorShare, setEditorShare] = useState(0.45);
  // Dragging the rule between editor and output moves it with the pointer.
  const startDrag = useCallback((down: ReactPointerEvent<HTMLDivElement>) => {
    const area = splitRef.current?.getBoundingClientRect();
    if (!area) return;
    down.preventDefault();
    const move = (e: PointerEvent) =>
      setEditorShare(clampShare((e.clientY - area.top) / area.height));
    const up = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  }, []);
  const nextRun = useRef(0);
  // REQ-2071: a run Polly asked for (navigate with state.code and autoRun) is reported back
  // under its run id once it finishes, or at once when it cannot run.
  const pending = useRef<{ code: string; runId: string | null } | null>(null);
  const [pendingTick, setPendingTick] = useState(0);
  const runIds = useRef(new Map<number, string>());
  useNavPayload<{ code?: string; autoRun?: boolean; runId?: string }>((payload) => {
    if (typeof payload.code !== "string") return;
    setCode(payload.code);
    if (payload.autoRun !== true) return;
    pending.current = { code: payload.code, runId: payload.runId ?? null };
    setPendingTick((n) => n + 1);
  });

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
          const runId = runIds.current.get(message.id);
          runIds.current.delete(message.id);
          if (runId !== undefined) reportRun(runId, summarise(message));
        }
      };
      const init: ToWorker = { type: "init", auth: await bridgeAuth() };
      w.postMessage(init);
    })();
    return () => {
      live = false;
      worker.current?.terminate();
      worker.current = null;
    };
  }, [makeWorker]);

  const run = useCallback(
    async (source: string = code, runId: string | null = null) => {
      const w = worker.current;
      if (w === null || status !== "ready") return;
      setStatus("running");
      // The identity is read at the moment of the run: a rotated token or a newly picked role,
      // org or environment is what the code reads as.
      const auth: ToWorker = { type: "auth", auth: await bridgeAuth() };
      w.postMessage(auth);
      const id = ++nextRun.current;
      if (runId !== null) runIds.current.set(id, runId);
      const message: ToWorker = { type: "run", id, code: source };
      w.postMessage(message);
    },
    [code, status],
  );

  // A requested run waits for Python to be ready; where Python cannot run, it says why at once.
  useEffect(() => {
    const request = pending.current;
    if (request === null) return;
    if (status === "ready") {
      pending.current = null;
      void run(request.code, request.runId);
    } else if (status === "missing" || status === "failed") {
      pending.current = null;
      if (request.runId !== null) {
        reportRun(request.runId, {
          stdout: "",
          value: "",
          error: status === "missing" ? t("dataScience.missingTitle") : t("dataScience.failed"),
        });
      }
    }
  }, [status, pendingTick, run, t]);

  if (status === "checking") return <Loader size="sm" m="md" />;
  if (status === "missing") {
    return (
      <Alert
        m="md"
        color="yellow"
        title={t("dataScience.missingTitle")}
        data-testid="data-science-missing"
      >
        <Text size="sm">{t("dataScience.missingBody")}</Text>
      </Alert>
    );
  }
  return (
    <Box ref={splitRef} style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
      <Box
        style={{ height: `${editorShare * 100}%`, minHeight: 0, overflow: "hidden" }}
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
          height="100%"
          style={{ height: "100%" }}
          aria-label={t("dataScience.editorLabel")}
          data-testid="data-science-editor"
        />
      </Box>
      <Group
        gap="sm"
        px="md"
        py={6}
        style={{ flexShrink: 0, borderTop: "1px solid var(--mantine-color-default-border)" }}
      >
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
      <Box
        role="separator"
        aria-orientation="horizontal"
        aria-label={t("dataScience.resizeLabel")}
        aria-valuenow={Math.round(editorShare * 100)}
        aria-valuemin={Math.round(SHARE_MIN * 100)}
        aria-valuemax={Math.round(SHARE_MAX * 100)}
        tabIndex={0}
        data-testid="data-science-split"
        onPointerDown={startDrag}
        onKeyDown={(e) => {
          if (e.key === "ArrowUp") setEditorShare((v) => clampShare(v - 0.05));
          if (e.key === "ArrowDown") setEditorShare((v) => clampShare(v + 0.05));
        }}
        style={{
          flex: "0 0 5px",
          cursor: "row-resize",
          background: "var(--mantine-color-default-border)",
          touchAction: "none",
        }}
      />
      <ScrollArea style={{ flex: 1, minHeight: 0 }} p="md" data-testid="data-science-output-pane">
        {failure !== null && (
          <Alert color="red" data-testid="data-science-failure">
            <Code block>{failure}</Code>
          </Alert>
        )}
        {output !== null && <RunOutput output={output} />}
      </ScrollArea>
    </Box>
  );
}

/** How much of the area the editor takes; the output has the rest. */
const SHARE_MIN = 0.15;
const SHARE_MAX = 0.85;
const clampShare = (share: number) => Math.min(SHARE_MAX, Math.max(SHARE_MIN, share));

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
