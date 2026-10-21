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
  ActionIcon,
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
  Text,
  useComputedColorScheme,
} from "@mantine/core";
import CodeMirror from "@uiw/react-codemirror";
import { Maximize2, Minimize2, Trash2 } from "lucide-react";
import { CopySymbolButton } from "../components/CopyButton";
import { Tip } from "../components/Tip";
import { EditorTabStrip } from "../components/EditorTabStrip";
import {
  forgetScript,
  loadOutput,
  loadScripts,
  moved,
  newScriptId,
  nextScriptTitle,
  saveOutput,
  saveScripts,
  type ScriptOutput,
  type Scripts,
} from "../dataScience/pythonTabs";
import "./DataSciencePage.css";
import { oneDark } from "@codemirror/theme-one-dark";
import { bridgeAuth } from "../dataScience/bridge";
import {
  followScheme,
  lastOutputArea,
  runCells,
  seedNewNotebooks,
  startLiteBridge,
  type LiteApp,
  type LiteBridge,
} from "../dataScience/liteBridge";
import { reportRun, summarise, type RunReport } from "../dataScience/runReports";
import { useNamedDemo } from "../context/NamedDemoContext";
import { useNavPayload } from "../hooks/useNavPayload";
import { useCapability } from "../hooks/useCapability";
import { useLocation } from "react-router-dom";
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

/** Explore's two Python surfaces, each its own route. */
type Panel = "python" | "notebook";
const PANEL_OF: Record<string, Panel> = { "/python": "python", "/notebook": "notebook" };

/** REQ-2071: what Polly hands a panel (navigate state): code for the Python tab, cells for the
 * Notebook tab, run at once with autoRun and reported back under the run id. */
interface PythonRequest {
  seq: number;
  code: string;
  autoRun: boolean;
  runId: string | null;
  /** Into a new script tab (titled ``title`` when given) rather than the active one. */
  newTab: boolean;
  title: string | null;
}
interface NotebookRequest {
  seq: number;
  cells: string[];
  autoRun: boolean;
  runId: string | null;
}

type NavState = {
  code?: unknown;
  cells?: unknown;
  autoRun?: unknown;
  runId?: unknown;
  newTab?: unknown;
  title?: unknown;
};

/** Reads the payload navigate hands /python or /notebook. Rendered only while one of them is the
 * page, so the payload of any other page is left to that page. */
function PayloadReader({ onPayload }: { onPayload: (payload: NavState) => void }) {
  useNavPayload<NavState>(onPayload);
  return null;
}

/**
 * REQ-2071: Explore's Python tab (/python) and Notebook tab (/notebook). They live outside the
 * route that shows them: each is mounted the first time it is opened and only hidden after, so
 * the Python worker's state and the notebook's kernel survive a visit to any other page. Their
 * routes render nothing of their own; this host, in the app's main area, shows the one the route
 * names.
 */
export function DataScienceHost({ makeWorker = defaultWorker }: { makeWorker?: MakeWorker }) {
  const { pathname } = useLocation();
  const allowed = useCapability("query_development");
  const active: Panel | null = allowed ? (PANEL_OF[pathname] ?? null) : null;
  const [opened, setOpened] = useState<ReadonlySet<Panel>>(() => new Set());
  if (active !== null && !opened.has(active)) setOpened(new Set([...opened, active]));
  const [pythonRequest, setPythonRequest] = useState<PythonRequest | null>(null);
  const [notebookRequest, setNotebookRequest] = useState<NotebookRequest | null>(null);
  const requests = useRef(0);
  // REQ-2084: the notebook the named demo ships, offered on the Notebook tab: its cells added to
  // the open notebook for the visitor to run.
  const { notebook: demoNotebook } = useNamedDemo();
  const offerDemoNotebook = useCallback(() => {
    setNotebookRequest({
      seq: ++requests.current,
      cells: demoNotebook,
      autoRun: false,
      runId: null,
    });
  }, [demoNotebook]);
  const { t } = useTranslation();
  // Maximized: the panel fills 95% of the window over the app; Escape restores it.
  const [maximized, setMaximized] = useState<Panel | null>(null);
  useEffect(() => {
    if (maximized === null) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setMaximized(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [maximized]);

  const take = useCallback(
    (payload: NavState) => {
      const autoRun = payload.autoRun === true;
      const runId = typeof payload.runId === "string" ? payload.runId : null;
      const cells = Array.isArray(payload.cells) ? payload.cells.map(String) : null;
      const code = typeof payload.code === "string" ? payload.code : null;
      if (cells === null && code === null) return;
      // The route says which panel: the notebook takes cells (or the code as one cell), Python
      // takes code (or the cells as one script).
      if (PANEL_OF[pathname] === "notebook") {
        const given = cells ?? [code as string];
        setNotebookRequest({ seq: ++requests.current, cells: given, autoRun, runId });
      } else {
        const given = code ?? (cells as string[]).join("\n\n");
        setPythonRequest({
          seq: ++requests.current,
          code: given,
          autoRun,
          runId,
          newTab: payload.newTab === true,
          title: typeof payload.title === "string" && payload.title.trim() ? payload.title : null,
        });
      }
    },
    [pathname],
  );

  if (opened.size === 0) return null;
  const isMax = (panel: Panel) => maximized === panel && active === panel;
  const panelBox = (panel: Panel, testId: string, body: React.ReactNode) => (
    <Box
      data-testid={testId}
      data-maximized={isMax(panel) || undefined}
      className={`ds-panel ds-pane${isMax(panel) ? " ds-max" : ""}`}
      style={{
        flex: 1,
        minHeight: 0,
        display: active === panel ? "flex" : "none",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      {body}
      <Group className="ds-controls ds-panel-controls" data-testid={`${testId}-controls`}>
        <Tip text={t(isMax(panel) ? "dataScience.restore" : "dataScience.maximize")} named>
          <ActionIcon
            type="button"
            variant="transparent"
            className="ds-control"
            aria-label={t(isMax(panel) ? "dataScience.restore" : "dataScience.maximize")}
            data-testid={`${testId}-maximize`}
            onClick={() => setMaximized(isMax(panel) ? null : panel)}
          >
            {isMax(panel) ? (
              <Minimize2 size={12} aria-hidden />
            ) : (
              <Maximize2 size={12} aria-hidden />
            )}
          </ActionIcon>
        </Tip>
      </Group>
    </Box>
  );
  return (
    <>
      {active !== null && <PayloadReader onPayload={take} />}
      {active !== null && maximized === active && (
        <div
          className="ds-backdrop"
          data-testid="data-science-backdrop"
          onClick={() => setMaximized(null)}
        />
      )}
      {opened.has("python") &&
        panelBox(
          "python",
          "python-panel",
          <PythonTab makeWorker={makeWorker} request={pythonRequest} />,
        )}
      {opened.has("notebook") &&
        panelBox(
          "notebook",
          "notebook-panel",
          <NotebookTab
            request={notebookRequest}
            onOpenDemo={demoNotebook.length > 0 ? offerDemoNotebook : null}
          />,
        )}
    </>
  );
}

/** The JupyterLite site the notebook tab embeds, opened on the starter notebook. Its JupyterLab
 * app, not its Notebook app: the Notebook app opens every document it opens or creates in a new
 * browser tab, out of the page; JupyterLab opens them as tabs of its own, inside the frame. */
export const NOTEBOOK_URL = "/lite/lab/index.html?path=Provisa.ipynb";
/** How long the tab waits for the embedded app before it gives up handing it the user. */
const LITE_APP_WAIT_MS = 120_000;

/** A box in the notebook tab's coordinates (its pane is the positioned parent). */
interface AnswerBox {
  top: number;
  left: number;
  width: number;
  height: number;
}

/** The last cell's outputs scrolled to the top of the notebook, once their pictures have loaded
 * (a figure has no height before): the whole of a figure in view, with room below it. */
async function bringAnswerIntoView(app: LiteApp) {
  const area = lastOutputArea(app);
  if (area === null) return;
  await Promise.all([...area.querySelectorAll("img")].map((img) => img.decode()));
  area.scrollIntoView({ block: "start" });
}

/** Where the last cell's outputs are drawn, in the pane's coordinates and inside the frame: null
 * while there are none, or none is in view. */
function answerBoxOf(app: LiteApp, iframe: HTMLIFrameElement | null): AnswerBox | null {
  const area = lastOutputArea(app);
  const pane = iframe?.parentElement;
  if (area === null || !iframe || !pane) return null;
  const at = iframe.getBoundingClientRect();
  const of = pane.getBoundingClientRect();
  const out = area.getBoundingClientRect();
  const top = Math.max(out.top, 0);
  const bottom = Math.min(out.bottom, at.height);
  if (bottom <= top) return null;
  return {
    top: at.top - of.top + top,
    left: at.left - of.left + out.left,
    width: out.width,
    height: bottom - top,
  };
}

function NotebookTab({
  request,
  onOpenDemo,
}: {
  request: NotebookRequest | null;
  /** Adds the named demo's notebook to the open one; null when the demo ships none. */
  onOpenDemo: (() => void) | null;
}) {
  const { t } = useTranslation();
  const scheme = useComputedColorScheme("light");
  const [served, setServed] = useState<boolean | null>(null);
  const [app, setApp] = useState<LiteApp | null>(null);
  const frame = useRef<HTMLIFrameElement | null>(null);
  const stops = useRef<Array<() => void>>([]);
  const bridge = useRef<LiteBridge | null>(null);
  const done = useRef(0);
  // The last request whose cells have all been handed over (and run, when asked): a tour step
  // waits on it being the request now in hand.
  const [answered, setAnswered] = useState<number | null>(null);
  // Where the last cell's outputs are drawn, over the frame: a tour step points at them (the
  // notebook's figure) rather than at the whole tab. Null until cells run for a tour have answered.
  const [answerBox, setAnswerBox] = useState<AnswerBox | null>(null);

  // Cells handed over are added to the open notebook, run when asked, and reported back; where
  // the notebook cannot run, it says why at once.
  useEffect(() => {
    if (request === null || request.seq === done.current) return;
    const report = (r: RunReport) => request.runId !== null && reportRun(request.runId, r);
    if (served === false) {
      done.current = request.seq;
      report({ stdout: "", value: "", error: t("dataScience.missingTitle") });
      return;
    }
    if (app === null) return;
    done.current = request.seq;
    const handOff = () => bridge.current?.handOff() ?? Promise.resolve();
    void runCells(app, request.cells, request.autoRun, handOff).then(async (r) => {
      report(r);
      if (request.autoRun) {
        await bringAnswerIntoView(app);
        setAnswerBox(answerBoxOf(app, frame.current));
      }
      setAnswered(request.seq);
    });
  }, [request, served, app, t]);

  // The last outputs' box, placed once they have answered, follows the notebook as it scrolls and
  // the window as it resizes.
  useEffect(() => {
    if (app === null || request === null || answered !== request.seq || !request.autoRun) return;
    const doc = frame.current?.contentDocument;
    const win = frame.current?.contentWindow;
    if (!doc || !win) return;
    const place = () => setAnswerBox(answerBoxOf(app, frame.current));
    doc.addEventListener("scroll", place, true);
    win.addEventListener("resize", place);
    window.addEventListener("resize", place);
    return () => {
      doc.removeEventListener("scroll", place, true);
      win.removeEventListener("resize", place);
      window.removeEventListener("resize", place);
    };
  }, [app, request, answered]);

  // The notebook wears the app's colour scheme, and changes with it.
  useEffect(() => {
    const doc = frame.current?.contentDocument;
    if (app !== null && doc) void followScheme(app, scheme, doc);
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
      const handing = startLiteBridge(appOf, bridgeAuth);
      bridge.current = handing;
      stops.current.push(handing.stop, seedNewNotebooks(app));
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
    <>
      {onOpenDemo !== null && (
        <Group className="ds-demo-notebook" gap="sm" px="md" py={6}>
          <Text size="sm">{t("dataScience.demoNotebookShipped")}</Text>
          <Tip text={t("dataScience.demoNotebookHint")} named>
            <Button
              size="compact-sm"
              variant="light"
              disabled={app === null}
              onClick={onOpenDemo}
              data-testid="notebook-open-demo"
            >
              {t("dataScience.demoNotebookOpen")}
            </Button>
          </Tip>
        </Group>
      )}
      <iframe
        ref={frame}
        src={NOTEBOOK_URL}
        title={t("dataScience.notebookFrame")}
        onLoad={connect}
        data-testid="data-science-notebook"
        style={{ flex: 1, minHeight: 0, width: "100%", border: 0 }}
      />
      {request !== null && answered === request.seq && (
        <span hidden data-testid="notebook-answered" data-seq={answered} />
      )}
      {request !== null && answered === request.seq && answerBox !== null && (
        <div
          data-testid="notebook-answer"
          aria-hidden
          style={{ position: "absolute", pointerEvents: "none", ...answerBox }}
        />
      )}
    </>
  );
}

function PythonTab({
  makeWorker,
  request,
}: {
  makeWorker: MakeWorker;
  request: PythonRequest | null;
}) {
  const { t } = useTranslation();
  const theme = useComputedColorScheme("light") === "dark" ? oneDark : undefined;
  const [status, setStatus] = useState<Status>("checking");
  const [failure, setFailure] = useState<string | null>(null);
  // The scripts, each a tab with its own code and last output, kept across reloads
  // (dataScience/pythonTabs). A run is the active script's, run fresh.
  const titleFor = useCallback((n: number) => t("dataScience.scriptTitle", { n }), [t]);
  const [scripts, setScripts] = useState<Scripts>(() =>
    loadScripts(() => {
      const id = newScriptId();
      return { tabs: [{ id, title: titleFor(1), code: STARTER_CODE }], activeId: id };
    }),
  );
  const [outputs, setOutputs] = useState<Record<string, ScriptOutput>>({});
  const activeScript = scripts.tabs.find((s) => s.id === scripts.activeId) ?? scripts.tabs[0];
  const code = activeScript.code;
  const output = outputs[activeScript.id] ?? null;
  const setCode = useCallback((next: string) => {
    setScripts((prev) => ({
      ...prev,
      tabs: prev.tabs.map((s) => (s.id === prev.activeId ? { ...s, code: next } : s)),
    }));
  }, []);
  useEffect(() => saveScripts(scripts), [scripts]);
  // Each script's last output, as the browser kept it.
  const [hydrated] = useState(() => scripts.tabs.map((s) => s.id));
  useEffect(() => {
    let live = true;
    void Promise.all(hydrated.map(async (id) => [id, await loadOutput(id)] as const)).then(
      (kept) => {
        if (!live) return;
        const found = Object.fromEntries(kept.filter(([, o]) => o !== null)) as Record<
          string,
          ScriptOutput
        >;
        setOutputs((prev) => ({ ...found, ...prev }));
      },
    );
    return () => {
      live = false;
    };
  }, [hydrated]);
  const addScript = useCallback(
    (initial = "", title: string | null = null): string => {
      const id = newScriptId();
      setScripts((prev) => ({
        tabs: [
          ...prev.tabs,
          { id, title: title ?? nextScriptTitle(prev.tabs, titleFor), code: initial },
        ],
        activeId: id,
      }));
      return id;
    },
    [titleFor],
  );
  const closeScript = useCallback(
    (id: string) => {
      forgetScript(id);
      setOutputs(({ [id]: _gone, ...rest }) => rest);
      setScripts((prev) => {
        const at = prev.tabs.findIndex((s) => s.id === id);
        const rest = prev.tabs.filter((s) => s.id !== id);
        if (rest.length === 0) {
          // The last one closed: a fresh script takes its place.
          const fresh = newScriptId();
          return { tabs: [{ id: fresh, title: titleFor(1), code: STARTER_CODE }], activeId: fresh };
        }
        const activeId =
          prev.activeId === id ? rest[Math.min(at, rest.length - 1)].id : prev.activeId;
        return { tabs: rest, activeId };
      });
    },
    [titleFor],
  );
  const worker = useRef<ReturnType<MakeWorker> | null>(null);
  const splitRef = useRef<HTMLDivElement | null>(null);
  const [editorShare, setEditorShare] = useState(0.45);
  // Dragging the rule between editor and output moves it with the pointer.
  const startDrag = useCallback((down: ReactPointerEvent<HTMLDivElement>) => {
    const area = splitRef.current?.getBoundingClientRect();
    if (!area) return;
    down.preventDefault();
    const move = (e: PointerEvent) =>
      setEditorShare(clampShare((e.clientX - area.left) / area.width));
    const up = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  }, []);
  const nextRun = useRef(0);
  // REQ-2071: code Polly hands over (navigate with state.code) lands in the editor; with autoRun it
  // runs once Python is ready, and is reported back under its run id when it finishes, or at once
  // when it cannot run.
  const runIds = useRef(new Map<number, string>());
  // Run id -> the script it ran: its output is that script's, whichever is shown when it ends.
  const runTabs = useRef(new Map<number, string>());
  const [shownRequest, setShownRequest] = useState(0);
  // The script a requested run is the run of: the active one, or the new one it was written to.
  const [requestTab, setRequestTab] = useState<string | null>(null);
  if (request !== null && request.seq !== shownRequest) {
    setShownRequest(request.seq);
    if (request.newTab) {
      const id = newScriptId();
      setScripts((prev) => ({
        tabs: [
          ...prev.tabs,
          { id, title: request.title ?? nextScriptTitle(prev.tabs, titleFor), code: request.code },
        ],
        activeId: id,
      }));
      setRequestTab(id);
    } else {
      setScripts((prev) => ({
        ...prev,
        tabs: prev.tabs.map((s) => (s.id === prev.activeId ? { ...s, code: request.code } : s)),
      }));
      setRequestTab(scripts.activeId);
    }
  }
  const ranRequest = useRef(0);

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
          const tab = runTabs.current.get(message.id);
          runTabs.current.delete(message.id);
          if (tab !== undefined) {
            setOutputs((prev) => ({ ...prev, [tab]: message }));
            saveOutput(tab, message);
          }
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
    async (source: string = code, runId: string | null = null, tab: string = activeScript.id) => {
      const w = worker.current;
      if (w === null || status !== "ready") return;
      setStatus("running");
      // The identity is read at the moment of the run: a rotated token or a newly picked role,
      // org or environment is what the code reads as.
      const auth: ToWorker = { type: "auth", auth: await bridgeAuth() };
      w.postMessage(auth);
      const id = ++nextRun.current;
      if (runId !== null) runIds.current.set(id, runId);
      runTabs.current.set(id, tab);
      const message: ToWorker = { type: "run", id, code: source };
      w.postMessage(message);
    },
    [code, status, activeScript.id],
  );

  // A requested run waits for Python to be ready; where Python cannot run, it says why at once.
  useEffect(() => {
    if (request === null || !request.autoRun || request.seq === ranRequest.current) return;
    if (status === "ready") {
      ranRequest.current = request.seq;
      void run(request.code, request.runId, requestTab ?? activeScript.id);
    } else if (status === "missing" || status === "failed") {
      ranRequest.current = request.seq;
      if (request.runId !== null) {
        reportRun(request.runId, {
          stdout: "",
          value: "",
          error: status === "missing" ? t("dataScience.missingTitle") : t("dataScience.failed"),
        });
      }
    }
  }, [status, request, run, t, requestTab, activeScript.id]);

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
    <Box style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
      <EditorTabStrip
        tabs={scripts.tabs}
        activeId={activeScript.id}
        labels={{
          renameHint: t("dataScience.scriptTabHint"),
          newTab: t("dataScience.newScript"),
          closeTab: (title) => t("dataScience.closeScript", { title }),
        }}
        testIdPrefix="python"
        onSwitch={(id) => setScripts((prev) => ({ ...prev, activeId: id }))}
        onAdd={() => addScript(STARTER_CODE)}
        onClose={closeScript}
        onRename={(id, title) =>
          setScripts((prev) => ({
            ...prev,
            tabs: prev.tabs.map((s) => (s.id === id ? { ...s, title } : s)),
          }))
        }
        onMove={(id, index) =>
          setScripts((prev) => ({ ...prev, tabs: moved(prev.tabs, id, index) }))
        }
      />
      <Box ref={splitRef} style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "row" }}>
        {/* The editor and its Run bar on the left, the output on the right. */}
        <Box
          style={{
            width: `${editorShare * 100}%`,
            minWidth: 0,
            display: "flex",
            flexDirection: "column",
          }}
        >
          <Group
            gap="sm"
            px="md"
            py={6}
            style={{ flexShrink: 0, borderBottom: "1px solid var(--mantine-color-default-border)" }}
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
            className="ds-pane"
            style={{ flex: 1, minHeight: 0, overflow: "hidden" }}
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
              // One editor per script: its own document and undo history, shown as switched to.
              key={activeScript.id}
              value={code}
              onChange={setCode}
              theme={theme}
              height="100%"
              style={{ height: "100%" }}
              aria-label={t("dataScience.editorLabel")}
              data-testid="data-science-editor"
            />
            <Group className="ds-controls" data-testid="data-science-editor-controls">
              <CopySymbolButton
                text={code}
                className="ds-control"
                title={t("dataScience.copyCode")}
              />
              <Tip text={t("dataScience.clearCode")} named>
                <ActionIcon
                  type="button"
                  variant="transparent"
                  className="ds-control"
                  aria-label={t("dataScience.clearCode")}
                  data-testid="data-science-clear"
                  onClick={() => setCode("")}
                >
                  <Trash2 size={12} aria-hidden />
                </ActionIcon>
              </Tip>
            </Group>
          </Box>
        </Box>
        <Box
          role="separator"
          aria-orientation="vertical"
          aria-label={t("dataScience.resizeLabel")}
          aria-valuenow={Math.round(editorShare * 100)}
          aria-valuemin={Math.round(SHARE_MIN * 100)}
          aria-valuemax={Math.round(SHARE_MAX * 100)}
          tabIndex={0}
          data-testid="data-science-split"
          onPointerDown={startDrag}
          onKeyDown={(e) => {
            if (e.key === "ArrowLeft") setEditorShare((v) => clampShare(v - 0.05));
            if (e.key === "ArrowRight") setEditorShare((v) => clampShare(v + 0.05));
          }}
          style={{
            flex: "0 0 5px",
            cursor: "col-resize",
            background: "var(--mantine-color-default-border)",
            touchAction: "none",
          }}
        />
        <Box className="ds-pane" style={{ flex: 1, minWidth: 0, minHeight: 0, display: "flex" }}>
          <ScrollArea
            style={{ flex: 1, minHeight: 0 }}
            p="md"
            data-testid="data-science-output-pane"
          >
            {failure !== null && (
              <Alert color="red" data-testid="data-science-failure">
                <Code block>{failure}</Code>
              </Alert>
            )}
            {output !== null && <RunOutput output={output} />}
          </ScrollArea>
          {output !== null && (
            <Group
              className="ds-controls ds-output-controls"
              data-testid="data-science-output-controls"
            >
              <CopySymbolButton
                text={outputText(output)}
                className="ds-control"
                title={t("dataScience.copyOutput")}
              />
            </Group>
          )}
        </Box>
      </Box>
    </Box>
  );
}

/** A run's output as text: what it printed, then its value (a table as tab-separated values,
 * as the results grid copies one). */
function outputText(output: Output): string {
  const parts = [output.stdout.trimEnd()];
  const { value } = output;
  if (value.kind === "text") parts.push(value.text);
  if (value.kind === "table") {
    const cell = (v: unknown) => (v === null || v === undefined ? "" : String(v));
    parts.push(
      [value.columns.join("\t"), ...value.rows.map((row) => row.map(cell).join("\t"))].join("\n"),
    );
  }
  if (output.error !== null) parts.push(output.error);
  return parts.filter((part) => part !== "").join("\n\n");
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
