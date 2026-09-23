// Copyright (c) 2026 Kenneth Stott
// Canary: 6a1e9d34-8f27-4b6c-b0a5-3e7d2c9f1b48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import {
  ActionIcon,
  Alert,
  Badge,
  Box,
  Button,
  CloseButton,
  Group,
  Modal,
  ScrollArea,
  Text,
  Textarea,
  Title,
  Tooltip,
} from "@mantine/core";
import { useMutation } from "@apollo/client/react";
import { MessageCircle } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useAuth } from "../context/AuthContext";
import { useMcpChat } from "../hooks/useMcpChat";
import { RefreshMv } from "../hooks/admin.graphql";
import { fetchMcpChatStatus } from "../api/mcpChat";
import type { MutationResult } from "../types/admin";
import type { ClientToolResult } from "../mcp/clientTools";
import { useLocalStorage } from "./graph/graph-persistence";

const MIN_WIDTH = 320;
const MAX_WIDTH = 900;
const DEFAULT_WIDTH = 420;

/**
 * REQ-1795: the chat assistant panel — a v1 slice proving the client-tool pause/resume
 * plumbing end to end (navigate + refresh_mv), separate from the existing McpExplorePage.tsx
 * (a dedicated page using the same backend but not yet sharing this hook).
 *
 * Docked, not an overlay (REQ-1803): rendered as a flex sibling of `<main>` inside `.app-body`,
 * so opening it SHRINKS the main content's flex-basis rather than covering it — the same layout
 * relationship `.admin-rail` already has with `<main>`. `App.tsx` places `<ChatPanel />` after
 * `<main>` in that row; this component owns only its own width and contents, not the row layout.
 *
 * Kept deliberately minimal: no persisted history, no input recall, no Claude Desktop panel —
 * those are McpExplorePage.tsx concerns. This proves the mechanism; broadening the client-tool
 * set to the full admin_schema mutation surface, sharing this hook with McpExplorePage, and
 * persisting the panel's conversation are explicit follow-ups.
 */
export function ChatPanel() {
  const { role } = useAuth();
  const roleId = role?.id ?? "";
  const navigate = useNavigate();
  const location = useLocation();
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState("");

  // REQ-1804: checked on toggle click, before opening the panel — an unconfigured LLM shows an
  // explanatory modal at the button instead of an open panel that only reveals the problem after
  // the user has already typed and sent a message.
  const [checkingConfig, setCheckingConfig] = useState(false);
  const [unconfiguredReason, setUnconfiguredReason] = useState<string | null>(null);
  const openChat = async () => {
    setCheckingConfig(true);
    try {
      const status = await fetchMcpChatStatus();
      if (status.configured) {
        setOpen(true);
      } else {
        setUnconfiguredReason(status.reason || "no vendor or credential is configured");
      }
    } catch {
      // The status check itself failed (network/server error) — open anyway and let the chat's
      // own error handling surface it, rather than blocking the toggle on a second failure mode.
      setOpen(true);
    } finally {
      setCheckingConfig(false);
    }
  };

  // Resizable width (REQ-1795), persisted per browser so it survives closing/reopening the
  // panel and page reloads. Dragged from a handle on the drawer's left edge.
  const [width, setWidth] = useLocalStorage("provisa.chatPanel.width", DEFAULT_WIDTH);
  const resizingRef = useRef(false);
  const startResize = (e: React.PointerEvent) => {
    e.preventDefault();
    resizingRef.current = true;
    const onMove = (ev: PointerEvent) => {
      if (!resizingRef.current) return;
      const next = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, window.innerWidth - ev.clientX));
      setWidth(next);
    };
    const onUp = () => {
      resizingRef.current = false;
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
  };

  // A single pending confirmation at a time — refresh_mv (and future confirm-gated tools) await
  // this Promise before the client tool executor returns its result.
  const pendingResolveRef = useRef<((ok: boolean) => void) | null>(null);
  const [confirmState, setConfirmState] = useState<{ title: string; message: string } | null>(
    null,
  );
  const confirm = (opts: { title: string; message: string }): Promise<boolean> =>
    new Promise((resolve) => {
      pendingResolveRef.current = resolve;
      setConfirmState(opts);
    });
  const resolveConfirm = (ok: boolean) => {
    setConfirmState(null);
    pendingResolveRef.current?.(ok);
    pendingResolveRef.current = null;
  };

  // REQ-1795: the ONLY place clientTools.ts's `refresh_mv` dispatch actually touches GraphQL —
  // via useMutation, matching the repo's Apollo-client-usage rule (only App.tsx may import the
  // raw client). Add a case here for each new admin-mutation client tool.
  const [refreshMv] = useMutation<{ refreshMv: MutationResult }>(RefreshMv);
  const runMutation = async (
    name: string,
    variables: Record<string, unknown>,
  ): Promise<ClientToolResult> => {
    if (name === "refresh_mv") {
      const result = await refreshMv({ variables: { mvId: variables.mvId } });
      const mr = result.data?.refreshMv;
      return { success: mr?.success ?? false, message: mr?.message ?? "No result returned." };
    }
    return { success: false, message: `no mutation wired for client tool ${name}` };
  };

  const { messages, tools, busy, error, send } = useMcpChat(
    roleId,
    { navigate, confirm, runMutation },
    location.pathname,
  );

  const onSend = () => {
    const text = draft;
    setDraft("");
    void send(text);
  };

  if (!open) {
    return (
      <>
        <Tooltip label="Polly, your data assistant" position="left">
          <ActionIcon
            variant="filled"
            size="xl"
            radius="xl"
            data-testid="chat-panel-toggle"
            loading={checkingConfig}
            style={{ position: "fixed", bottom: 16, right: 16, zIndex: 200 }}
            onClick={() => void openChat()}
          >
            <MessageCircle size={20} />
          </ActionIcon>
        </Tooltip>

        <Modal
          opened={unconfiguredReason !== null}
          onClose={() => setUnconfiguredReason(null)}
          title="Polly isn't set up yet"
          data-testid="chat-panel-unconfigured-modal"
        >
          <Text size="sm" mb="md">
            Polly needs an LLM vendor and credential configured before it can chat
            {unconfiguredReason ? ` (${unconfiguredReason})` : ""}.
          </Text>
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setUnconfiguredReason(null)}>
              Not now
            </Button>
            <Button
              onClick={() => {
                setUnconfiguredReason(null);
                navigate("/admin/ai-models");
              }}
            >
              Open AI Models settings
            </Button>
          </Group>
        </Modal>
      </>
    );
  }

  return (
    <>
      {/* Docked flex sibling of `<main>` inside `.app-body` (REQ-1803) — a fixed flex-basis, not
          an overlay, so `<main>` (flex: 1; min-width: 0) shrinks to the remaining width instead
          of being covered. */}
      <Box
        data-testid="chat-panel"
        style={{
          position: "relative",
          flex: `0 0 ${width}px`,
          display: "flex",
          flexDirection: "column",
          borderLeft: "1px solid var(--mantine-color-default-border)",
          padding: "var(--mantine-spacing-sm)",
        }}
      >
        {/* Drag handle: resizes the panel by adjusting its flex-basis (REQ-1795). */}
        <div
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize chat panel"
          data-testid="chat-panel-resize-handle"
          onPointerDown={startResize}
          style={{
            position: "absolute",
            top: 0,
            bottom: 0,
            left: 0,
            width: 6,
            cursor: "ew-resize",
            zIndex: 10,
          }}
        />

        <Group justify="space-between" mb="xs">
          <Title order={5}>Polly</Title>
          <CloseButton data-testid="chat-panel-close" onClick={() => setOpen(false)} />
        </Group>

        {tools.length > 0 && (
          <Group gap={6} mb="xs" wrap="wrap">
            {tools.map((tl, i) => (
              <Badge
                key={i}
                size="sm"
                variant={tl.running ? "outline" : "light"}
                color={tl.error ? "red" : "grape"}
              >
                {tl.name}
              </Badge>
            ))}
          </Group>
        )}

        {error && (
          <Alert color="red" mb="xs">
            {error.message}
            {error.action && (
              <Button
                size="xs"
                variant="light"
                mt={6}
                onClick={() => {
                  setOpen(false);
                  navigate(error.action!.route);
                }}
              >
                {error.action.label}
              </Button>
            )}
          </Alert>
        )}

        <ScrollArea style={{ flex: 1 }} mb="sm">
          {messages.map((m, i) => (
            <Box
              key={i}
              mb="sm"
              p="xs"
              style={{
                borderRadius: 8,
                background: m.role === "user" ? "var(--mantine-color-blue-light)" : "transparent",
                fontSize: "var(--mantine-font-size-xs)",
              }}
            >
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {m.text || (busy && i === messages.length - 1 ? "…" : "")}
              </ReactMarkdown>
            </Box>
          ))}
        </ScrollArea>

        <Textarea
          placeholder="Ask the assistant, or tell it what to do…"
          value={draft}
          onChange={(e) => setDraft(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              onSend();
            }
          }}
          disabled={busy}
          autosize
          minRows={2}
          maxRows={6}
        />
        <Button mt="xs" onClick={onSend} disabled={busy || !draft.trim()} loading={busy}>
          Send
        </Button>
      </Box>

      <Modal
        opened={confirmState !== null}
        onClose={() => resolveConfirm(false)}
        title={confirmState?.title}
      >
        <Text size="sm" mb="md">
          {confirmState?.message}
        </Text>
        <Group justify="flex-end">
          <Button variant="default" onClick={() => resolveConfirm(false)}>
            Cancel
          </Button>
          <Button onClick={() => resolveConfirm(true)}>Confirm</Button>
        </Group>
      </Modal>
    </>
  );
}
