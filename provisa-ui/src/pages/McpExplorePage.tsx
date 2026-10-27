// Copyright (c) 2026 Kenneth Stott
// Canary: 102d630e-f88b-4659-8425-1152b4771c3c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";
import { useNavPayload } from "../hooks/useNavPayload";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Button, Code, Group, Loader, Modal, Text, TextInput, Title } from "@mantine/core";
import { CollapsibleItem, Collapsibles } from "../components/collapsible/Collapsible";
import {
  MainContainer,
  ChatContainer,
  MessageList,
  Message,
  MessageInput,
  TypingIndicator,
} from "@chatscope/chat-ui-kit-react";
import "@chatscope/chat-ui-kit-styles/dist/default/styles.min.css";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Check, Copy, Trash2 } from "lucide-react";
import { HelpBubble } from "../components/HelpBubble";
import { useAuth } from "../context/AuthContext";
import { Tip } from "../components/Tip";
import { getStored, setStored } from "../lib/browserStorage";
import { explainSql, type ExplainResponse } from "../api/admin";

/** A copy-to-clipboard button that fades in on bubble hover. */
function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <Tip text="Copy">
      <button
        type="button"
        className="mcp-copy"
        aria-label="Copy message"
        data-testid="mcp-chat-copy"
        onClick={async () => {
          try {
            await navigator.clipboard.writeText(text);
            setCopied(true);
            setTimeout(() => setCopied(false), 1200);
          } catch {
            /* clipboard unavailable (insecure context) — no-op */
          }
        }}
      >
        {copied ? <Check size={13} /> : <Copy size={13} />}
      </button>
    </Tip>
  );
}

const API_BASE = import.meta.env.VITE_API_BASE || "";

// REQ-1103: a copy-ready "Connect Claude Desktop" panel. The browser can't write
// claude_desktop_config.json, so this is the universal path — show the config snippet + an editable
// URL and let the user paste it. Claude Desktop can only launch a local stdio child (no HTTP/URL
// transport, and its hosted connector reaches only public HTTPS), so we bridge stdio -> the running
// Streamable HTTP server with mcp-proxy. mcp-proxy is bundled INTO the native runtime (REQ-1104),
// so the command is that runtime's own python (bridge_command from the API) — no Node, no install,
// airgapped. The URL is resolved server-side (REQ-1102), proxy-aware, and shown editable.
function useCopy(): [boolean, (text: string) => void] {
  const [copied, setCopied] = useState(false);
  const copy = (text: string) => {
    void (async () => {
      try {
        await navigator.clipboard.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1200);
      } catch {
        /* clipboard unavailable (insecure context) */
      }
    })();
  };
  return [copied, copy];
}

function ConnectClaudeDesktop({ roleId, reveal }: { roleId: string; reveal: string | null }) {
  const [url, setUrl] = useState("");
  const [bridgeCommand, setBridgeCommand] = useState("");
  const [bridgeArgs, setBridgeArgs] = useState<string[]>([]);
  const [enabled, setEnabled] = useState<boolean | null>(null);
  const [copiedCfg, copyCfg] = useCopy();

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = await fetch(`${API_BASE}/admin/mcp-server`, {
          headers: { "x-provisa-role": roleId },
        });
        if (!r.ok || cancelled) return;
        const s = (await r.json()) as {
          enabled?: boolean;
          url?: string;
          bridge_command?: string;
          bridge_args?: string[];
        };
        if (cancelled) return;
        setEnabled(Boolean(s.enabled));
        if (s.url) setUrl(s.url);
        if (s.bridge_command) setBridgeCommand(s.bridge_command);
        if (s.bridge_args) setBridgeArgs(s.bridge_args);
      } catch {
        /* status unavailable — leave the panel quiet */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [roleId]);

  // Bridge config (fallback / non-TLS). Native tier hands us the bundled interpreter (ships
  // mcp-proxy) -> zero-install; elsewhere fall back to the `mcp-proxy` console script the user
  // installs themselves.
  const bundled = Boolean(bridgeCommand);
  const command = bundled ? bridgeCommand : "mcp-proxy";
  const args = bundled ? [...bridgeArgs, url] : ["--transport", "streamablehttp", url];
  const snippet = JSON.stringify({ mcpServers: { provisa: { command, args } } }, null, 2);

  if (enabled === false) {
    return (
      <Text size="xs" c="dimmed" mb="sm" data-testid="mcp-connect-off">
        MCP server is off. Set PROVISA_MCP_PORT to enable it, then reload to connect Claude Desktop.
      </Text>
    );
  }
  if (enabled === null) return null; // loading / unknown — stay quiet until we know

  return (
    <div style={{ marginBottom: "var(--mantine-spacing-sm)" }}>
      {/* The config that points Claude Desktop at this MCP server, so the same governed tools are
          available outside Provisa. */}
      <Collapsibles id="mcp-explore.claude-desktop" reveal={reveal}>
        <CollapsibleItem value="setup" defaultOpen={false} title="Connect Claude Desktop">
          <Text size="xs" c="dimmed" mb={6} data-testid="mcp-connect-config">
            In Claude Desktop, open Settings → Developer → Edit Config (this creates the file if it
            doesn&apos;t exist yet), paste the JSON below, then restart Claude.{" "}
            {bundled ? (
              <>The bridge (mcp-proxy) ships inside Provisa&apos;s runtime — no Node, no install.</>
            ) : (
              <>
                The bridge runs on the machine with Claude Desktop, so install it there:{" "}
                <Code>pip install mcp-proxy</Code>.
              </>
            )}{" "}
            Edit the URL if Provisa is remote or behind a proxy.
          </Text>
          <Tip text="The endpoint Claude Desktop connects to. Change it if Provisa is remote or behind a proxy — the JSON below updates with it.">
            <TextInput
              label="MCP URL"
              size="xs"
              value={url}
              onChange={(e) => setUrl(e.currentTarget.value)}
              mb={6}
            />
          </Tip>
          <Code block>{snippet}</Code>
          <Group gap={6} mt={6}>
            <Button
              size="xs"
              leftSection={copiedCfg ? <Check size={13} /> : <Copy size={13} />}
              onClick={() => copyCfg(snippet)}
            >
              {copiedCfg ? "Copied" : "Copy config"}
            </Button>
          </Group>
          <Text size="xs" c="dimmed" mt={6}>
            Config file — macOS: ~/Library/Application Support/Claude/claude_desktop_config.json ·
            Windows: %APPDATA%\Claude\claude_desktop_config.json · Linux:
            ~/.config/Claude/claude_desktop_config.json
          </Text>
          <Text size="xs" c="dimmed" mt={6}>
            Claude Desktop&apos;s <b>Add custom connector</b> (paste-a-URL) option can&apos;t reach
            a local server — Anthropic connects to it from their servers, so it needs a public,
            internet-reachable HTTPS endpoint. For a local install, use the config above.
          </Text>
        </CollapsibleItem>
      </Collapsibles>
    </div>
  );
}

interface ChatMsg {
  role: "user" | "assistant";
  text: string;
}

interface ToolEvent {
  name: string;
  input?: unknown;
  running?: boolean;
  error?: boolean;
}

/** The statement a run_sql call sent, or null for any other call. */
function toolSql(tool: ToolEvent): string | null {
  if (tool.name !== "run_sql") return null;
  const input = tool.input as { sql?: unknown } | undefined;
  return typeof input?.sql === "string" ? input.sql : null;
}

// REQ-2172: what a run_sql call of the agent ran -- the statement it sent, and the statement the
// one pipeline ran for it under the explorer's role, row rules and masks applied (the SQL
// explorer's explain, /data/sql/explain). The developer's view; the agent itself is never told.
function ToolExplain({ sql, roleId, onClose }: { sql: string; roleId: string; onClose: () => void }) {
  const { t } = useTranslation();
  const [plan, setPlan] = useState<ExplainResponse | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    let cancelled = false;
    explainSql(sql.trim().replace(/;+$/, ""), roleId)
      .then((p) => {
        if (!cancelled) setPlan(p);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [sql, roleId]);
  return (
    <Modal opened onClose={onClose} size="xl" title={t("mcpExplore.explainTitle")}>
      <div data-testid="mcp-explain-panel">
        <Text size="xs" fw={600} mb={4}>
          {t("mcpExplore.explainSent")}
        </Text>
        <Code block mb="sm" data-testid="mcp-explain-sent">
          {sql}
        </Code>
        <Text size="xs" fw={600} mb={4}>
          {t("mcpExplore.explainRan", { role: roleId })}
        </Text>
        {error ? (
          <Alert color="red">{error}</Alert>
        ) : plan ? (
          <Code block data-testid="mcp-explain-ran" style={{ whiteSpace: "pre-wrap" }}>
            {plan.sql}
          </Code>
        ) : (
          <Loader size="sm" />
        )}
      </div>
    </Modal>
  );
}

// REQ-1008: the MCP "explore" surface as an LLM chatbot — a Claude agent drives the governed MCP
// tools (list/describe/run/explain/search) under the active role, streamed over SSE.
export function McpExplorePage() {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  // REQ-1620: the ACTIVE roles — one, or under "Role: All" the set served as their meta-role.
  const roleId = actingRoles ?? "";
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [tools, setTools] = useState<ToolEvent[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [draft, setDraft] = useState(""); // controlled input value (for copy/clear tools)
  // Latest-value ref so the SSE reader appends to current text without stale closures.
  const assistantRef = useRef("");
  // REQ-2172: the run_sql call whose explain is open, the handed-over question's answer is on
  // screen (a demo tour step waits for it), and the Connect panel a tour step opens.
  const [explaining, setExplaining] = useState<string | null>(null);
  const [answered, setAnswered] = useState(false);
  const [revealConnect, setRevealConnect] = useState<string | null>(null);

  // Persist the conversation per role so it survives navigating away and back (and reloads within
  // the tab). Role-scoped so one role's chat never restores under another (governance isolation).
  const storageKey = `mcp.chat.${roleId || "default"}`;
  useEffect(() => {
    try {
      // The product tour seeds a canned conversation here (a live chat needs an LLM key).
      const seed = getStored("provisa_mcp_tour_chat", { session: true });
      const saved = seed ?? getStored(storageKey, { session: true });
      /* eslint-disable-next-line react-hooks/set-state-in-effect --
         restore persisted (or tour-seeded) conversation on mount / role change */
      setMessages(saved ? (JSON.parse(saved) as ChatMsg[]) : []);
    } catch {
      /* corrupt/absent → start fresh */
    }
  }, [storageKey]);
  useEffect(() => {
    try {
      // Don't persist the tour's canned chat into the role's real history.
      if (getStored("provisa_mcp_tour_chat", { session: true })) return;
      setStored(storageKey, JSON.stringify(messages), { session: true });
    } catch {
      /* storage full / unavailable — persistence is best-effort */
    }
  }, [storageKey, messages]);

  // Sent-input history (shell-style): Up/Down cycle through past entries. Persisted per role,
  // independent of the visible conversation so it survives clearing the chat.
  const histKey = `mcp.hist.${roleId || "default"}`;
  const [history, setHistory] = useState<string[]>([]);
  const histIndexRef = useRef(-1); // -1 = live draft; 0 = most recent entry
  const recalledRef = useRef<string | null>(null);
  useEffect(() => {
    try {
      const saved = getStored(histKey, { session: true });
      /* eslint-disable-next-line react-hooks/set-state-in-effect -- restore input history */
      setHistory(saved ? (JSON.parse(saved) as string[]) : []);
    } catch {
      /* absent → empty history */
    }
  }, [histKey]);

  const onDraftKey = (e: React.KeyboardEvent) => {
    const target = e.target as HTMLElement;
    if (!target.closest(".cs-message-input")) return;
    if ((e.key !== "ArrowUp" && e.key !== "ArrowDown") || history.length === 0) return;
    e.preventDefault();
    let idx = histIndexRef.current + (e.key === "ArrowUp" ? 1 : -1);
    idx = Math.min(idx, history.length - 1);
    if (idx < 0) {
      histIndexRef.current = -1;
      recalledRef.current = "";
      setDraft("");
      return;
    }
    histIndexRef.current = idx;
    const val = history[history.length - 1 - idx];
    recalledRef.current = val;
    setDraft(val);
  };

  const send = async (raw: string) => {
    const text = raw.replace(/<[^>]*>/g, "").trim(); // MessageInput yields HTML
    if (!text || busy) return;
    setDraft("");
    // Record in input history (skip a consecutive duplicate), cap at 100, and reset the cursor.
    setHistory((h) => {
      const next = h[h.length - 1] === text ? h : [...h, text].slice(-100);
      try {
        setStored(histKey, JSON.stringify(next), { session: true });
      } catch {
        /* best-effort */
      }
      return next;
    });
    histIndexRef.current = -1;
    recalledRef.current = null;
    setError("");
    setTools([]);
    const history: ChatMsg[] = [...messages, { role: "user", text }];
    setMessages([...history, { role: "assistant", text: "" }]);
    assistantRef.current = "";
    setBusy(true);

    const appendAssistant = (chunk: string) => {
      assistantRef.current += chunk;
      const current = assistantRef.current;
      setMessages((prev) => {
        const next = [...prev];
        next[next.length - 1] = { role: "assistant", text: current };
        return next;
      });
    };

    try {
      const resp = await fetch(`${API_BASE}/admin/mcp/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-provisa-role": roleId },
        body: JSON.stringify({
          messages: history.map((m) => ({ role: m.role, content: m.text })),
        }),
      });
      if (!resp.ok || !resp.body) throw new Error(`chat failed: ${resp.status}`);

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const blocks = buffer.split("\n\n");
        buffer = blocks.pop() ?? "";
        for (const block of blocks) {
          const line = block.trim();
          if (!line.startsWith("data:")) continue;
          const ev = JSON.parse(line.slice(5).trim());
          if (ev.type === "text") appendAssistant(ev.text);
          else if (ev.type === "tool_use")
            setTools((p) => [...p, { name: ev.name, input: ev.input, running: true }]);
          else if (ev.type === "tool_result")
            // Resolve the matching in-flight badge (last running of this name) rather than
            // appending a second chip — one call = one badge, running → done/error.
            setTools((p) => {
              const next = [...p];
              for (let i = next.length - 1; i >= 0; i--) {
                if (next[i].name === ev.name && next[i].running) {
                  next[i] = { ...next[i], running: false, error: ev.is_error };
                  break;
                }
              }
              return next;
            });
          else if (ev.type === "error") setError(ev.error);
        }
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  // A question handed to the page (NL "MCP Chat ›", Polly) is sent once, whether the page was just
  // opened or already open. The hand-off is removed from the history entry once handled, so a
  // refresh does not send it again.
  useNavPayload<{ mcpQuestion?: string; mcpConnect?: boolean }>((payload) => {
    if (payload.mcpConnect) setRevealConnect("setup");
    if (payload.mcpQuestion) {
      setAnswered(false);
      void send(payload.mcpQuestion).then(() => setAnswered(true));
    }
  });
  // The badge a tour's explain step opens: the turn's last run_sql call.
  const lastSqlTool = tools.reduce((last, tl, i) => (toolSql(tl) !== null ? i : last), -1);

  return (
    <div
      className="page"
      style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}
      data-testid="mcp-explore"
      data-answered={answered ? "" : undefined}
    >
      <Title order={2} mb={4}>
        {t("mcpExplore.title")}
      </Title>
      <Text c="dimmed" size="sm" mb="xs">
        {t("mcpExplore.intro")}
      </Text>
      <HelpBubble
        title={t("mcpExplore.roleNoteHelpTitle")}
        paragraphs={[t("mcpExplore.roleNoteTooltip")]}
        ariaLabel={t("mcpExplore.roleNoteHelpTitle")}
        testId="mcp-role-note-help"
        target={
          <Text size="xs" c="dimmed" mb="sm" style={{ width: "fit-content" }}>
            {t("mcpExplore.roleNote", { role: roleId || t("mcpExplore.noRole") })}
          </Text>
        }
      />

      <ConnectClaudeDesktop roleId={roleId} reveal={revealConnect} />

      {tools.length > 0 && (
        <Group gap={6} mb="xs" wrap="wrap">
          {tools.map((tl, i) => {
            const sql = toolSql(tl);
            return (
              <Tip
                text={
                  tl.error
                    ? t("mcpExplore.toolErrorTooltip", { name: tl.name })
                    : tl.running
                      ? t("mcpExplore.toolRunningTooltip", { name: tl.name })
                      : sql !== null
                        ? t("mcpExplore.toolExplainTooltip", { name: tl.name })
                        : t("mcpExplore.toolDoneTooltip", { name: tl.name })
                }
                key={i}
              >
                <Badge
                  size="sm"
                  variant={tl.running ? "outline" : "light"}
                  color={tl.error ? "red" : "grape"}
                  data-testid="mcp-chat-tool"
                  data-mcp-explain-last={i === lastSqlTool ? "" : undefined}
                  {...(sql !== null
                    ? {
                        component: "button" as const,
                        onClick: () => setExplaining(sql),
                        style: { cursor: "pointer" },
                      }
                    : {})}
                >
                  {tl.name}
                </Badge>
              </Tip>
            );
          })}
        </Group>
      )}
      {error && (
        <Alert color="red" mb="xs">
          {error}
        </Alert>
      )}
      {explaining !== null && (
        <ToolExplain sql={explaining} roleId={roleId} onClose={() => setExplaining(null)} />
      )}

      <div
        className="mcp-chat-wrap"
        style={{ flex: 1, minHeight: 0, position: "relative" }}
        onKeyDown={onDraftKey}
      >
        <MainContainer>
          <ChatContainer>
            <MessageList
              typingIndicator={
                busy ? <TypingIndicator content={t("mcpExplore.thinking")} /> : undefined
              }
            >
              {messages.length === 0 && (
                <Message
                  model={{
                    direction: "incoming",
                    position: "single",
                    sender: "assistant",
                  }}
                >
                  <Message.CustomContent>
                    <div className="mcp-md">
                      <ReactMarkdown remarkPlugins={[remarkGfm]}>
                        {t("mcpExplore.emptyState")}
                      </ReactMarkdown>
                    </div>
                  </Message.CustomContent>
                </Message>
              )}
              {messages.map((m, i) =>
                m.role === "user" ? (
                  <Message
                    key={i}
                    model={{ direction: "outgoing", position: "single", sender: "user" }}
                  >
                    <Message.CustomContent>
                      <div className="mcp-bubble">
                        <div className="mcp-user-text">{m.text}</div>
                        <CopyButton text={m.text} />
                      </div>
                    </Message.CustomContent>
                  </Message>
                ) : (
                  // Assistant bubbles render markdown (tables, code, lists).
                  <Message
                    key={i}
                    model={{ direction: "incoming", position: "single", sender: "assistant" }}
                  >
                    <Message.CustomContent>
                      <div className="mcp-bubble">
                        <div className="mcp-md">
                          <ReactMarkdown remarkPlugins={[remarkGfm]}>
                            {m.text || (busy && i === messages.length - 1 ? "…" : "")}
                          </ReactMarkdown>
                        </div>
                        {m.text && <CopyButton text={m.text} />}
                      </div>
                    </Message.CustomContent>
                  </Message>
                ),
              )}
            </MessageList>
            <MessageInput
              placeholder={t("mcpExplore.inputPlaceholder")}
              value={draft}
              onChange={(_html: string, text: string) => {
                if (text !== recalledRef.current) histIndexRef.current = -1; // manual edit
                setDraft(text);
              }}
              onSend={() => void send(draft)}
              disabled={busy}
              attachButton={false}
              data-testid="mcp-chat-input"
            />
          </ChatContainer>
        </MainContainer>
        {/* Hover copy/clear for the current draft — overlaid just left of the send button. */}
        {draft.trim() && (
          <div className="mcp-input-tools">
            <CopyButton text={draft} />
            <Tip text={t("mcpExplore.clearTooltip")}>
              <button
                type="button"
                className="mcp-copy"
                aria-label={t("mcpExplore.clear")}
                data-testid="mcp-chat-clear"
                onClick={() => setDraft("")}
              >
                <Trash2 size={13} />
              </button>
            </Tip>
          </div>
        )}
      </div>
    </div>
  );
}
