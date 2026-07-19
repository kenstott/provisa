// Copyright (c) 2026 Kenneth Stott
// Canary: d37b633e-d4f3-4d34-82e7-c727048db209
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useCallback, useRef } from "react";
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { GrpcCodeView } from "./grpc/GrpcCodeView";
import { Badge, Button, Group, Select, Tabs, Text } from "@mantine/core";
import { useAuth } from "../context/AuthContext";
import { useDomainFilter } from "../context/DomainFilterContext";
import "./GrpcPage.css";

type OperationType = "query" | "mutation" | "command";
type LeftTab = "body" | "proto";

interface ProtoMethod {
  name: string;
  operation: OperationType;
  typeName: string;
  requestMsgName: string;
}

interface CommandArg {
  name: string;
  type: string;
}

interface CommandDef {
  name: string;
  description?: string | null;
  arguments: CommandArg[];
}

interface ProtoField {
  name: string;
  protoType: string;
  repeated: boolean;
}

interface ParsedProto {
  methods: ProtoMethod[];
  messages: Record<string, ProtoField[]>;
}

function parseProto(text: string): ParsedProto {
  const methods: ProtoMethod[] = [];
  const messages: Record<string, ProtoField[]> = {};

  const serviceMatch = text.match(/service\s+\w+\s*\{([\s\S]*?)\n\}/);
  if (serviceMatch) {
    for (const m of serviceMatch[1].matchAll(
      /rpc\s+(\w+)\s*\((\w+)\)\s*returns\s*\((?:stream\s+)?(\w+)\)/g,
    )) {
      const [, name, requestMsg] = m;
      if (name.startsWith("Query")) {
        methods.push({ name, operation: "query", typeName: name.slice(5), requestMsgName: requestMsg });
      } else if (name.startsWith("Insert")) {
        methods.push({ name, operation: "mutation", typeName: name.slice(6), requestMsgName: requestMsg });
      }
    }
  }

  for (const m of text.matchAll(/^message\s+(\w+)\s*\{([\s\S]*?)\n\}/gm)) {
    const [, msgName, body] = m;
    const fields: ProtoField[] = [];
    for (const line of body.split("\n")) {
      const fm = line.trim().match(/^(repeated\s+)?(\S+)\s+(\w+)\s*=\s*\d+;/);
      if (fm) fields.push({ name: fm[3], protoType: fm[2], repeated: !!fm[1] });
    }
    messages[msgName] = fields;
  }

  return { methods, messages };
}

function defaultForType(protoType: string): unknown {
  if (protoType === "bool") return false;
  if (protoType === "int32" || protoType === "int64") return 0;
  if (protoType === "float" || protoType === "double") return 0.0;
  return "";
}

function defaultForArgType(argType: string): unknown {
  const t = argType.toLowerCase();
  if (t === "boolean") return false;
  if (t === "int") return 0;
  if (t === "float") return 0.0;
  return "";
}

// Commands are invoked through the single generic CallCommand RPC. The Explorer edits an
// { name, args } object; args is serialized to the CommandRequest.args_json string on send.
function buildCommandTemplate(cmd: CommandDef | undefined): string {
  if (!cmd) return "";
  const args: Record<string, unknown> = {};
  for (const a of cmd.arguments) args[a.name] = defaultForArgType(a.type);
  return JSON.stringify({ name: cmd.name, args }, null, 2);
}

function buildMessageTemplate(method: ProtoMethod, messages: Record<string, ProtoField[]>): string {
  if (method.operation === "query") {
    const filterFields = messages[`${method.typeName}Filter`] ?? [];
    const filter: Record<string, unknown> = {};
    for (const f of filterFields) filter[f.name] = null;
    return JSON.stringify({ filter, limit: 20, offset: 0, read_mask: { paths: [] } }, null, 2);
  }
  const inputFields = messages[method.requestMsgName] ?? [];
  const input: Record<string, unknown> = {};
  for (const f of inputFields) input[f.name] = f.repeated ? [] : defaultForType(f.protoType);
  return JSON.stringify(input, null, 2);
}

export function GrpcPage() {
  const { t } = useTranslation();
  const location = useLocation();
  const { role } = useAuth();
  const { checkedDomains } = useDomainFilter();
  const roleId = role?.id ?? "";
  const domainsParam = checkedDomains.size > 0 ? [...checkedDomains].join(",") : "";

  const [navMethod] = useState(
    () => (location.state as { grpcMethod?: string } | null)?.grpcMethod ?? "",
  );
  const [navAutoRun] = useState(
    () => (location.state as { autoRun?: boolean } | null)?.autoRun === true,
  );

  const [protoText, setProtoText] = useState("");
  const [protoError, setProtoError] = useState("");
  const [parsed, setParsed] = useState<ParsedProto>({ methods: [], messages: {} });
  const [commands, setCommands] = useState<CommandDef[]>([]);
  const [opType, setOpType] = useState<OperationType>("query");
  const [selectedMethod, setSelectedMethod] = useState<ProtoMethod | null>(null);
  const [messageText, setMessageText] = useState("");
  const [leftTab, setLeftTab] = useState<LeftTab>("body");
  const [response, setResponse] = useState("");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");

  // Synthetic methods for registered commands (one generic CallCommand RPC, one entry per command).
  const commandsMapRef = useRef<Record<string, CommandDef>>({});
  const commandMethods: ProtoMethod[] = commands.map((c) => ({
    name: c.name,
    operation: "command",
    typeName: c.name,
    requestMsgName: "CommandRequest",
  }));
  const allMethods = [...parsed.methods, ...commandMethods];

  const selectMethod = useCallback((method: ProtoMethod, proto: ParsedProto) => {
    setSelectedMethod(method);
    setMessageText(
      method.operation === "command"
        ? buildCommandTemplate(commandsMapRef.current[method.name])
        : buildMessageTemplate(method, proto.messages),
    );
    setResponse("");
    setError("");
  }, []);

  // Auto-select first method whenever op type or parsed proto changes
  const prevOpTypeRef = useRef<OperationType | null>(null);
  useEffect(() => {
    if (!allMethods.length) return;
    if (navSelectDoneRef.current) return;
    if (prevOpTypeRef.current === opType && selectedMethod?.operation === opType) return;
    prevOpTypeRef.current = opType;
    const first = allMethods.find((m) => m.operation === opType);
    // eslint-disable-next-line react-hooks/set-state-in-effect -- auto-selects first method when opType/parsed changes; cannot be derived during render because selectedMethod also has user-driven updates via handleMethodChange
    if (first) selectMethod(first, parsed);
    else { setSelectedMethod(null); setMessageText(""); }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- allMethods is derived from parsed+commands, which are the real deps; listing the fresh array each render would loop
  }, [opType, parsed, commands, selectMethod, selectedMethod]);

  const navSelectDoneRef = useRef(false);

  const fetchProto = useCallback(async (rid: string, domains: string) => {
    setProtoError("");
    try {
      const url = domains
        ? `/data/proto/${encodeURIComponent(rid)}?domains=${encodeURIComponent(domains)}`
        : `/data/proto/${encodeURIComponent(rid)}`;
      const res = await fetch(url);
      if (!res.ok) {
        setProtoError(`No proto for role "${rid}" — schema not yet built.`);
        return;
      }
      const text = await res.text();
      setProtoText(text);
      const p = parseProto(text);
      const preferred = navMethod ? navMethod.replace(/^Query/, "") : "";
      const navM = preferred ? p.methods.find((m) => m.typeName === preferred && m.operation === "query") : null;
      const initial = navM ?? p.methods.find((m) => m.operation === "query") ?? p.methods[0] ?? null;
      if (initial) {
        setOpType(initial.operation);
        selectMethod(initial, p);
        // eslint-disable-next-line react-hooks/immutability -- one-shot guard written after async fetch resolves; read occurs in a separate effect that guards against re-auto-selection
        if (navM) navSelectDoneRef.current = true;
      }
      setParsed(p);
    } catch {
      setProtoError("Failed to fetch proto.");
    }
  }, [navMethod, selectMethod]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- triggers async proto fetch; all setState calls occur inside the async callback, not synchronously in the effect body
    if (roleId) void fetchProto(roleId, domainsParam);
  }, [roleId, domainsParam, fetchProto]);

  const fetchCommands = useCallback(async (rid: string) => {
    try {
      const res = await fetch(`/data/grpc-commands/${encodeURIComponent(rid)}`);
      if (!res.ok) {
        setCommands([]);
        return;
      }
      const list = (await res.json()) as CommandDef[];
      commandsMapRef.current = Object.fromEntries(list.map((c) => [c.name, c]));
      setCommands(list);
    } catch {
      setCommands([]);
    }
  }, []);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- triggers async command fetch; setState occurs inside the async callback
    if (roleId) void fetchCommands(roleId);
  }, [roleId, fetchCommands]);

  const handleRun = useCallback(async () => {
    if (!selectedMethod || !roleId) return;
    if (selectedMethod.operation === "mutation") {
      setError("Mutation RPCs are not yet supported via the HTTP proxy.");
      return;
    }
    setRunning(true);
    setError("");
    setResponse("");
    try {
      if (selectedMethod.operation === "command") {
        // Serialize the edited { name, args } object into a CommandRequest { name, args_json }.
        let name = selectedMethod.name;
        let args: unknown = {};
        try {
          const parsed_msg = JSON.parse(messageText) as { name?: string; args?: unknown };
          if (typeof parsed_msg.name === "string") name = parsed_msg.name;
          if (parsed_msg.args !== undefined) args = parsed_msg.args;
        } catch { /* use defaults */ }
        const res = await fetch(`/data/grpc-command/${encodeURIComponent(roleId)}`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "x-provisa-role": roleId },
          body: JSON.stringify({ name, args_json: JSON.stringify(args) }),
        });
        const json = await res.json();
        if (!res.ok) {
          setError((json as { detail?: string }).detail ?? JSON.stringify(json));
        } else {
          setResponse(JSON.stringify(json, null, 2));
        }
        return;
      }
      let body: Record<string, unknown> = { role_id: roleId };
      try {
        const parsed_msg = JSON.parse(messageText) as Record<string, unknown>;
        body = { ...parsed_msg, role_id: roleId };
      } catch { /* use default body */ }
      const res = await fetch(`/data/grpc/${encodeURIComponent(selectedMethod.typeName)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-provisa-role": roleId },
        body: JSON.stringify(body),
      });
      const json = await res.json();
      if (!res.ok) {
        setError((json as { detail?: string }).detail ?? JSON.stringify(json));
      } else {
        setResponse(JSON.stringify(json, null, 2));
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setRunning(false);
    }
  }, [selectedMethod, roleId, messageText]);

  const navAutoRunDoneRef = useRef(false);
  useEffect(() => {
    if (!navAutoRun || navAutoRunDoneRef.current || !selectedMethod) return;
    navAutoRunDoneRef.current = true;
    void handleRun();
  }, [selectedMethod, navAutoRun, handleRun]);

  const visibleMethods = allMethods.filter((m) => m.operation === opType);

  const handleMethodChange = (name: string) => {
    const m = allMethods.find((x) => x.name === name);
    if (m) selectMethod(m, parsed);
  };

  return (
    <div className="grpc-page page">
      {/* Top bar: method selector + send */}
      <Group className="grpc-topbar" justify="space-between" wrap="nowrap">
        <Group className="grpc-topbar-left" wrap="nowrap" gap="sm" style={{ flex: 1, minWidth: 0 }}>
          <Badge
            variant="outline"
            color="gray"
            radius="sm"
            style={{ fontFamily: "monospace", fontWeight: 400, textTransform: "none", flexShrink: 0 }}
          >
            {t("grpcPage.serverBadge")}
          </Badge>
          <Select
            aria-label={t("grpcPage.operationType")}
            data-testid="grpc-op-select"
            size="xs"
            w={130}
            allowDeselect={false}
            value={opType}
            onChange={(v) => v && setOpType(v as OperationType)}
            disabled={allMethods.length === 0}
            data={[
              { value: "query", label: t("grpcPage.query") },
              { value: "mutation", label: t("grpcPage.mutation") },
              { value: "command", label: t("grpcPage.command") },
            ]}
          />
          <Select
            aria-label={t("grpcPage.method")}
            data-testid="grpc-method-select"
            size="xs"
            style={{ flex: 1, minWidth: 0, maxWidth: 320, fontFamily: "monospace" }}
            value={selectedMethod?.name ?? null}
            onChange={(v) => v && handleMethodChange(v)}
            disabled={visibleMethods.length === 0}
            placeholder={visibleMethods.length === 0 ? t("grpcPage.noMethods") : undefined}
            data={visibleMethods.map((m) => ({ value: m.name, label: m.name }))}
          />
        </Group>
        <Group className="grpc-topbar-right" wrap="nowrap" gap="sm">
          {protoError && (
            <Text size="xs" c="red" data-testid="grpc-proto-error" style={{ maxWidth: 300 }} truncate="end">
              {protoError}
            </Text>
          )}
          <Button
            size="xs"
            data-testid="grpc-send-btn"
            onClick={handleRun}
            disabled={running || !selectedMethod || !roleId || !!protoError}
          >
            {running ? t("grpcPage.cancel") : t("grpcPage.send")}
          </Button>
        </Group>
      </Group>

      {/* Main panels */}
      <div className="grpc-body">
        {/* Left: Body / Proto tabs */}
        <div className="grpc-panel grpc-panel-left">
          <Tabs
            className="grpc-tabs"
            value={leftTab}
            onChange={(v) => v && setLeftTab(v as LeftTab)}
          >
            <Tabs.List>
              <Tabs.Tab value="body" data-testid="grpc-tab-body">
                {t("grpcPage.body")}
              </Tabs.Tab>
              <Tabs.Tab value="proto" data-testid="grpc-tab-proto">
                {t("grpcPage.proto")}
              </Tabs.Tab>
            </Tabs.List>
          </Tabs>
          {leftTab === "body" ? (
            <GrpcCodeView
              language="json"
              value={messageText}
              onChange={setMessageText}
              editablePlaceholder={selectedMethod ? "" : t("grpcPage.selectMethodPlaceholder")}
              data-testid="grpc-body-editor"
            />
          ) : (
            <GrpcCodeView
              language="proto"
              value={protoText}
              placeholder={protoError ? "" : t("grpcPage.loading")}
            />
          )}
        </div>

        {/* Right: Response */}
        <div className="grpc-panel grpc-panel-right">
          <Tabs className="grpc-tabs" value="response">
            <Tabs.List>
              <Tabs.Tab value="response">{t("grpcPage.response")}</Tabs.Tab>
            </Tabs.List>
          </Tabs>
          {error && (
            <Text className="grpc-error-bar" c="red" size="xs" data-testid="grpc-error-bar">
              {error}
            </Text>
          )}
          <GrpcCodeView
            language="json"
            value={response}
            placeholder={running ? t("grpcPage.waiting") : t("grpcPage.sendPrompt")}
          />
        </div>
      </div>
    </div>
  );
}
