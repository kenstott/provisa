// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Group, Text, Title } from "@mantine/core";
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
import { useAuth } from "../context/AuthContext";

/** A copy-to-clipboard button that fades in on bubble hover. */
function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="mcp-copy"
      title="Copy"
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
  );
}

const API_BASE = import.meta.env.VITE_API_BASE || "";

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

// REQ-1008: the MCP "explore" surface as an LLM chatbot — a Claude agent drives the governed MCP
// tools (list/describe/run/explain/search) under the active role, streamed over SSE.
export function McpExplorePage() {
  const { t } = useTranslation();
  const { role } = useAuth();
  const roleId = role?.id ?? "";
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [tools, setTools] = useState<ToolEvent[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [draft, setDraft] = useState(""); // controlled input value (for copy/clear tools)
  // Latest-value ref so the SSE reader appends to current text without stale closures.
  const assistantRef = useRef("");

  // Persist the conversation per role so it survives navigating away and back (and reloads within
  // the tab). Role-scoped so one role's chat never restores under another (governance isolation).
  const storageKey = `mcp.chat.${roleId || "default"}`;
  useEffect(() => {
    try {
      // The product tour seeds a canned conversation here (a live chat needs an LLM key).
      const seed = sessionStorage.getItem("provisa_mcp_tour_chat");
      const saved = seed ?? sessionStorage.getItem(storageKey);
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
      if (sessionStorage.getItem("provisa_mcp_tour_chat")) return;
      sessionStorage.setItem(storageKey, JSON.stringify(messages));
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
      const saved = sessionStorage.getItem(histKey);
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
        sessionStorage.setItem(histKey, JSON.stringify(next));
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

  // Arriving from the NL page's "MCP Chat ›" button: take the carried question and run it once.
  // react-router `state` doesn't survive a reload, so there's no resend on refresh; the ref guards
  // against re-firing on re-render within this mount.
  const location = useLocation();
  const incoming = (location.state as { mcpQuestion?: string } | null)?.mcpQuestion;
  const autoSentRef = useRef(false);
  useEffect(() => {
    if (incoming && !autoSentRef.current) {
      autoSentRef.current = true;
      void send(incoming);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- run once for the carried question
  }, [incoming]);

  return (
    <div
      className="page"
      style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}
    >
      <Title order={2} mb={4}>
        {t("mcpExplore.title")}
      </Title>
      <Text c="dimmed" size="sm" mb="xs">
        {t("mcpExplore.intro")}
      </Text>
      <Text size="xs" c="dimmed" mb="sm">
        {t("mcpExplore.roleNote", { role: roleId || t("mcpExplore.noRole") })}
      </Text>

      {tools.length > 0 && (
        <Group gap={6} mb="xs" wrap="wrap">
          {tools.map((tl, i) => (
            <Badge
              key={i}
              size="sm"
              variant={tl.running ? "outline" : "light"}
              color={tl.error ? "red" : "grape"}
              data-testid="mcp-chat-tool"
            >
              {tl.name}
            </Badge>
          ))}
        </Group>
      )}
      {error && (
        <Alert color="red" mb="xs">
          {error}
        </Alert>
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
            <button
              type="button"
              className="mcp-copy"
              title={t("mcpExplore.clear")}
              aria-label={t("mcpExplore.clear")}
              data-testid="mcp-chat-clear"
              onClick={() => setDraft("")}
            >
              <Trash2 size={13} />
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
