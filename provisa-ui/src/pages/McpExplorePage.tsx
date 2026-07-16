// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useState } from "react";
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
import { useAuth } from "../context/AuthContext";

const API_BASE = import.meta.env.VITE_API_BASE || "";

interface ChatMsg {
  role: "user" | "assistant";
  text: string;
}

interface ToolEvent {
  name: string;
  input?: unknown;
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
  // Latest-value ref so the SSE reader appends to current text without stale closures.
  const assistantRef = useRef("");

  const send = async (raw: string) => {
    const text = raw.replace(/<[^>]*>/g, "").trim(); // MessageInput yields HTML
    if (!text || busy) return;
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
            setTools((p) => [...p, { name: ev.name, input: ev.input }]);
          else if (ev.type === "tool_result")
            setTools((p) => [...p, { name: ev.name, error: ev.is_error }]);
          else if (ev.type === "error") setError(ev.error);
        }
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="page">
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
              variant="light"
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

      <div style={{ height: "62vh", position: "relative" }}>
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
                    message: t("mcpExplore.emptyState"),
                    direction: "incoming",
                    position: "single",
                    sender: "assistant",
                  }}
                />
              )}
              {messages.map((m, i) => (
                <Message
                  key={i}
                  model={{
                    message: m.text || (busy && i === messages.length - 1 ? "…" : ""),
                    direction: m.role === "user" ? "outgoing" : "incoming",
                    position: "single",
                    sender: m.role,
                  }}
                />
              ))}
            </MessageList>
            <MessageInput
              placeholder={t("mcpExplore.inputPlaceholder")}
              onSend={send}
              disabled={busy}
              attachButton={false}
              data-testid="mcp-chat-input"
            />
          </ChatContainer>
        </MainContainer>
      </div>
    </div>
  );
}
