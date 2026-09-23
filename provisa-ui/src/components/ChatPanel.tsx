// Copyright (c) 2026 Kenneth Stott
// Canary: 6a1e9d34-8f27-4b6c-b0a5-3e7d2c9f1b48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import {
  ActionIcon,
  Alert,
  Badge,
  Box,
  Button,
  Checkbox,
  CloseButton,
  CopyButton,
  Group,
  Loader,
  Modal,
  Paper,
  TextInput,
  Radio,
  ScrollArea,
  Stack,
  Text,
  Textarea,
  Title,
  Tooltip,
} from "@mantine/core";
import { useApolloClient, useMutation } from "@apollo/client/react";
import { Check, Copy, Eraser, MessageCircle, Mic, MicOff, Play, Square, X } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useAuth } from "../context/AuthContext";
import { useMcpChat } from "../hooks/useMcpChat";
import { useSpeechToText } from "../hooks/useSpeechToText";
import { RefreshMv } from "../hooks/admin.graphql";
import { fetchMcpChatStatus } from "../api/mcpChat";
import type { MutationResult } from "../types/admin";
import type { ChoiceAnswer, ClientToolResult, PresentChoiceRequest } from "../mcp/clientTools";
import { useLocalStorage } from "./graph/graph-persistence";

const MIN_WIDTH = 320;
const MAX_WIDTH = 900;
const DEFAULT_WIDTH = 420;

// REQ-1806: a brief, enumerated demo prompt list shown only before the first message — picked to
// showcase distinct real capabilities (catalog search, subscription sources, page awareness),
// plus one that's just for personality.
const SUGGESTED_QUESTIONS = [
  "Why are you named Polly?",
  "What page am I on?",
  "What data sources are registered?",
  "Find me inflation data",
];

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
  const { role, selectedRoles } = useAuth();
  // REQ-1812: local-dev/demo (unsecured) auth reconstructs request.state.assignments purely from
  // this header, splitting it on commas (see provisa/auth/middleware.py) — sending only role?.id
  // (the single collapsed "acting" role) meant the "Role: All" case never actually carried the
  // rest of the user's held roles to the server, so a capability held only via a second role (e.g.
  // org_admin's source_registration) never triggered REQ-1799's confirm_required shortcut and
  // Polly always queued a pending request instead of asking permission to act directly. In the
  // real authenticated path this is harmless: the server ignores extra ids there and re-derives
  // the true assignment set from the DB/claims regardless of what the header says.
  const roleId = selectedRoles.map((r) => r.id).join(",") || (role?.id ?? "");
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

  // REQ-1810: present_choice's widget — a single pending question at a time, same await-a-Promise
  // shape as `confirm` above, generalized to carry the answer's actual value (not just ok/cancel).
  const pendingChoiceResolveRef = useRef<((answer: ChoiceAnswer) => void) | null>(null);
  const [choiceState, setChoiceState] = useState<PresentChoiceRequest | null>(null);
  const [choiceDraft, setChoiceDraft] = useState<string[]>([]);
  // REQ-1816: the listed options are never exhaustive — every mode also offers a free-text
  // "Something else" escape hatch rather than forcing the user into one of the model's options.
  const OTHER = "__other__";
  const [otherText, setOtherText] = useState("");
  const [yesNoOther, setYesNoOther] = useState(false);
  const presentChoice = (req: PresentChoiceRequest): Promise<ChoiceAnswer> =>
    new Promise((resolve) => {
      pendingChoiceResolveRef.current = resolve;
      setChoiceDraft([]);
      setOtherText("");
      setYesNoOther(false);
      setChoiceState(req);
      // REQ-1818: the question itself is tool INPUT, never part of the model's own streamed text
      // — without this the transcript shows the answer with no question it was answering. Recorded
      // as the assistant's own message so the chat window stays a complete record of the exchange.
      pushMessage({ role: "assistant", text: req.question });
    });
  // REQ-1813: record what was picked as an ordinary chat message, so the choice has a visible,
  // scrollable trace in the conversation instead of vanishing once the widget closes.
  const describeChoice = (answer: ChoiceAnswer): string => {
    if (typeof answer === "boolean") return answer ? "Yes" : "No";
    if (Array.isArray(answer)) return answer.length > 0 ? answer.join(", ") : "(none selected)";
    return answer;
  };
  const resolveChoice = (answer: ChoiceAnswer) => {
    setChoiceState(null);
    pushMessage({ role: "user", text: describeChoice(answer) });
    pendingChoiceResolveRef.current?.(answer);
    pendingChoiceResolveRef.current = null;
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

  // REQ-1820: create_source_now/register_table_now run entirely server-side — no Apollo mutation
  // is ever issued from the browser for them, so nothing tells an already-open admin page (e.g.
  // Sources) that its list is now stale. Refetch every ACTIVE query on the page whenever one of
  // these tools completes, success or failure, so the UI reconciles with whatever the server
  // actually did rather than staying frozen at pre-chat state.
  const apolloClient = useApolloClient();
  const onServerToolResult = (name: string) => {
    if (name === "create_source_now" || name === "register_table_now") {
      void apolloClient.refetchQueries({ include: "active" });
    }
  };

  const { messages, tools, busy, error, send, cancel, clear, pushMessage } = useMcpChat(
    roleId,
    { navigate, confirm, runMutation, presentChoice },
    location.pathname,
    onServerToolResult,
  );

  const onSend = () => {
    // REQ-1806: while the suggestions are showing, typing just the number of one ("1", "2", …)
    // and sending it means the same thing as clicking it — send the full question, not the digit.
    const asIndex = messages.length === 0 ? Number(draft.trim()) : NaN;
    const text =
      Number.isInteger(asIndex) && asIndex >= 1 && asIndex <= SUGGESTED_QUESTIONS.length
        ? SUGGESTED_QUESTIONS[asIndex - 1]
        : draft;
    setDraft("");
    void send(text);
  };

  // REQ-1815: Stop refocuses + briefly highlights the prompt box so the user can immediately
  // continue typing, instead of just silently killing the request. The textarea is still
  // `disabled={busy}` at the instant Stop is clicked — a disabled element can't take focus — so
  // the actual focus+flash is deferred to the effect below, which fires once `busy` (and the
  // `disabled` attribute with it) has actually flipped back to false.
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [flashPrompt, setFlashPrompt] = useState(false);
  const stoppedRef = useRef(false);
  const stopSend = () => {
    stoppedRef.current = true;
    cancel();
  };
  useEffect(() => {
    if (busy || !stoppedRef.current) return;
    stoppedRef.current = false;
    textareaRef.current?.focus();
    setFlashPrompt(true);
    const t = window.setTimeout(() => setFlashPrompt(false), 650);
    return () => window.clearTimeout(t);
  }, [busy]);

  // REQ-1821: the tool-call log has a max height (it can grow long over a multi-step turn), so it
  // needs to auto-scroll to the newest entry the same way the message list would, rather than
  // leaving new badges to appear below the visible area.
  const toolsScrollRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = toolsScrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [tools]);

  // REQ-1805: mic-to-text — appends the transcript to whatever's already typed, so a person can
  // dictate a follow-on clause instead of only replacing the draft.
  const speech = useSpeechToText((transcript) =>
    setDraft((prev) => (prev.trim() ? `${prev.trim()} ${transcript}` : transcript)),
  );

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
          // Without this, a flex item's min-height defaults to its content's natural height, so
          // a tall message list grows the panel (and the whole `.app-body` row) taller than the
          // viewport instead of clipping here — the page then scrolls as one unit, taking `<main>`
          // with it, instead of each panel's own ScrollArea scrolling independently.
          minHeight: 0,
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
          <Group gap={4}>
            <Tooltip label="Clear conversation">
              <ActionIcon
                variant="subtle"
                color="gray"
                data-testid="chat-panel-clear"
                disabled={messages.length === 0 || busy}
                onClick={clear}
              >
                <Eraser size={16} />
              </ActionIcon>
            </Tooltip>
            <CloseButton data-testid="chat-panel-close" onClick={() => setOpen(false)} />
          </Group>
        </Group>

        {tools.length > 0 && (
          <ScrollArea.Autosize
            mah={96}
            mb="xs"
            viewportRef={toolsScrollRef}
            data-testid="chat-panel-tool-log"
          >
            <Group gap={6} wrap="wrap">
              {tools.map((tl, i) => (
                <Tooltip
                  key={i}
                  label={
                    <Text
                      component="pre"
                      size="xs"
                      style={{ margin: 0, whiteSpace: "pre-wrap", maxWidth: 280 }}
                    >
                      {tl.input !== undefined ? JSON.stringify(tl.input, null, 2) : "(no params)"}
                    </Text>
                  }
                  withArrow
                  multiline
                >
                  <Badge
                    size="sm"
                    variant={tl.running ? "outline" : "light"}
                    color={tl.error ? "red" : tl.running ? "grape" : "teal"}
                    data-testid="chat-panel-tool-badge"
                    data-running={tl.running ? "true" : "false"}
                    leftSection={
                      tl.running ? (
                        <Loader size={10} color="grape" data-testid="chat-panel-tool-spinner" />
                      ) : tl.error ? (
                        <X size={10} data-testid="chat-panel-tool-error-icon" />
                      ) : (
                        <Check size={10} data-testid="chat-panel-tool-done-icon" />
                      )
                    }
                  >
                    {tl.name}
                  </Badge>
                </Tooltip>
              ))}
            </Group>
          </ScrollArea.Autosize>
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
          {messages.length === 0 && (
            <Stack gap={6} data-testid="chat-panel-suggestions">
              <Text size="xs" c="dimmed">
                Try asking:
              </Text>
              {SUGGESTED_QUESTIONS.map((q, i) => (
                <Button
                  key={q}
                  variant="light"
                  size="xs"
                  justify="flex-start"
                  onClick={() => void send(q)}
                  disabled={busy}
                >
                  {i + 1}. {q}
                </Button>
              ))}
            </Stack>
          )}
          {messages.map((m, i) => (
            <Box
              key={i}
              className="chat-message"
              pos="relative"
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
              {m.text && (
                <CopyButton value={m.text} timeout={1500}>
                  {({ copied, copy }) => (
                    <Tooltip label={copied ? "Copied" : "Copy"} withArrow>
                      <ActionIcon
                        className="chat-message-copy"
                        size="xs"
                        variant="subtle"
                        color={copied ? "teal" : "gray"}
                        onClick={copy}
                        data-testid="chat-message-copy"
                        style={{ position: "absolute", top: 2, right: 2 }}
                      >
                        {copied ? <Check size={12} /> : <Copy size={12} />}
                      </ActionIcon>
                    </Tooltip>
                  )}
                </CopyButton>
              )}
            </Box>
          ))}
        </ScrollArea>

        <Textarea
          ref={textareaRef}
          className={flashPrompt ? "chat-panel-textarea-flash" : undefined}
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
          rightSectionPointerEvents="auto"
          rightSection={
            speech.supported && (
              <Tooltip label={speech.listening ? "Stop listening" : "Speak to Polly"}>
                <ActionIcon
                  variant={speech.listening ? "filled" : "subtle"}
                  color={speech.listening ? "red" : undefined}
                  size="sm"
                  data-testid="chat-panel-mic"
                  disabled={busy}
                  onClick={() => (speech.listening ? speech.stop() : speech.start())}
                >
                  {speech.listening ? <MicOff size={14} /> : <Mic size={14} />}
                </ActionIcon>
              </Tooltip>
            )
          }
        />
        {/* REQ-1815: a media-style stop/go control, not a spinner — while busy this IS a red
            "stop" square (pulsing to show the request is still running) whose click aborts the
            in-flight fetch and refocuses + flashes the prompt box so the user can immediately
            type a follow-up; idle, it's a plain "go" triangle. */}
        <Button
          mt="xs"
          color={busy ? "red" : undefined}
          leftSection={
            busy ? (
              <Square size={14} fill="currentColor" className="chat-panel-stop-icon" />
            ) : (
              <Play size={14} fill="currentColor" />
            )
          }
          onClick={busy ? stopSend : onSend}
          disabled={!busy && !draft.trim()}
          data-testid="chat-panel-send"
        >
          {busy ? "Stop" : "Send"}
        </Button>

      {/* REQ-1812: centered WITHIN the chat panel, not the viewport. Must be a DESCENDANT of this
          Box (not a sibling rendered after it closes) — position: absolute centers against the
          nearest POSITIONED ancestor, and rendering it outside the panel meant its ancestor was
          effectively the document, so it centered over the whole app regardless of where the
          panel was docked. A plain overlay Box here, not Mantine's <Modal> (which portals to
          document.body and can't be confined to an arbitrary container at all). */}
      {choiceState && (
        <Box
          data-testid="chat-panel-choice-modal"
          style={{
            position: "absolute",
            inset: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "var(--mantine-spacing-md)",
            background: "rgba(0, 0, 0, 0.35)",
            zIndex: 20,
          }}
          onClick={(e) => {
            if (e.target === e.currentTarget) resolveChoice(choiceState.mode === "multi" ? [] : "");
          }}
        >
          <Paper shadow="md" radius="md" p="md" withBorder style={{ maxWidth: "90%", width: 340 }}>
            <Text fw={600} size="sm" mb="sm">
              {choiceState.question}
            </Text>

            {choiceState.mode === "yes_no" &&
              (yesNoOther ? (
                <Stack gap="sm">
                  <TextInput
                    placeholder="Type your answer…"
                    value={otherText}
                    onChange={(e) => setOtherText(e.currentTarget.value)}
                    autoFocus
                  />
                  <Group justify="space-between">
                    <Button variant="subtle" size="xs" onClick={() => setYesNoOther(false)}>
                      Back to Yes/No
                    </Button>
                    <Button
                      disabled={!otherText.trim()}
                      onClick={() => resolveChoice(otherText.trim())}
                    >
                      Submit
                    </Button>
                  </Group>
                </Stack>
              ) : (
                <Stack gap="sm">
                  <Group justify="flex-end">
                    <Button variant="default" onClick={() => resolveChoice(false)}>
                      No
                    </Button>
                    <Button onClick={() => resolveChoice(true)}>Yes</Button>
                  </Group>
                  <Button variant="subtle" size="xs" onClick={() => setYesNoOther(true)}>
                    Type something else
                  </Button>
                </Stack>
              ))}

            {choiceState.mode === "single" && (
              <Stack gap="sm">
                <Radio.Group value={choiceDraft[0] ?? ""} onChange={(v) => setChoiceDraft([v])}>
                  <Stack gap={6} mt="xs">
                    {choiceState.options?.map((opt) => (
                      <Radio key={opt} value={opt} label={opt} />
                    ))}
                    <Radio value={OTHER} label="Type something else" />
                  </Stack>
                </Radio.Group>
                {choiceDraft[0] === OTHER && (
                  <TextInput
                    placeholder="Type your answer…"
                    value={otherText}
                    onChange={(e) => setOtherText(e.currentTarget.value)}
                    autoFocus
                  />
                )}
                <Group justify="flex-end">
                  <Button
                    disabled={
                      !choiceDraft[0] || (choiceDraft[0] === OTHER && !otherText.trim())
                    }
                    onClick={() =>
                      resolveChoice(choiceDraft[0] === OTHER ? otherText.trim() : choiceDraft[0])
                    }
                  >
                    Submit
                  </Button>
                </Group>
              </Stack>
            )}

            {choiceState.mode === "multi" && (
              <Stack gap="sm">
                <Checkbox.Group value={choiceDraft} onChange={setChoiceDraft}>
                  <Stack gap={6} mt="xs">
                    {choiceState.options?.map((opt) => (
                      <Checkbox key={opt} value={opt} label={opt} />
                    ))}
                    <Checkbox value={OTHER} label="Type something else" />
                  </Stack>
                </Checkbox.Group>
                {choiceDraft.includes(OTHER) && (
                  <TextInput
                    placeholder="Type your answer…"
                    value={otherText}
                    onChange={(e) => setOtherText(e.currentTarget.value)}
                    autoFocus
                  />
                )}
                <Group justify="flex-end">
                  <Button
                    onClick={() => {
                      const rest = choiceDraft.filter((v) => v !== OTHER);
                      const extra = choiceDraft.includes(OTHER) && otherText.trim() ? [otherText.trim()] : [];
                      resolveChoice([...rest, ...extra]);
                    }}
                  >
                    Submit
                  </Button>
                </Group>
              </Stack>
            )}
          </Paper>
        </Box>
      )}
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
