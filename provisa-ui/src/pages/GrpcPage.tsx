// Copyright (c) 2026 Kenneth Stott
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
import { useAuth } from "../context/AuthContext";
import { useDomainFilter } from "../context/DomainFilterContext";
import "./GrpcPage.css";

type OperationType = "query" | "mutation";
type LeftTab = "body" | "proto";

interface ProtoMethod {
  name: string;
  operation: OperationType;
  typeName: string;
  requestMsgName: string;
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
  const [opType, setOpType] = useState<OperationType>("query");
  const [selectedMethod, setSelectedMethod] = useState<ProtoMethod | null>(null);
  const [messageText, setMessageText] = useState("");
  const [leftTab, setLeftTab] = useState<LeftTab>("body");
  const [response, setResponse] = useState("");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");

  const selectMethod = useCallback((method: ProtoMethod, proto: ParsedProto) => {
    setSelectedMethod(method);
    setMessageText(buildMessageTemplate(method, proto.messages));
    setResponse("");
    setError("");
  }, []);

  // Auto-select first method whenever op type or parsed proto changes
  const prevOpTypeRef = useRef<OperationType | null>(null);
  useEffect(() => {
    if (!parsed.methods.length) return;
    if (navSelectDoneRef.current) return;
    if (prevOpTypeRef.current === opType && selectedMethod?.operation === opType) return;
    prevOpTypeRef.current = opType;
    const first = parsed.methods.find((m) => m.operation === opType);
    if (first) selectMethod(first, parsed);
    else { setSelectedMethod(null); setMessageText(""); }
  }, [opType, parsed, selectMethod, selectedMethod]);

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
        if (navM) navSelectDoneRef.current = true;
      }
      setParsed(p);
    } catch {
      setProtoError("Failed to fetch proto.");
    }
  }, [navMethod, selectMethod]);

  useEffect(() => {
    if (roleId) void fetchProto(roleId, domainsParam);
  }, [roleId, domainsParam, fetchProto]);

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

  const visibleMethods = parsed.methods.filter((m) => m.operation === opType);

  const handleMethodChange = (name: string) => {
    const m = parsed.methods.find((x) => x.name === name);
    if (m) selectMethod(m, parsed);
  };

  return (
    <div className="grpc-page page">
      {/* Top bar: method selector + send */}
      <div className="grpc-topbar">
        <div className="grpc-topbar-left">
          <span className="grpc-server-badge">gRPC • :50051</span>
          <select
            className="grpc-op-select"
            value={opType}
            onChange={(e) => setOpType(e.target.value as OperationType)}
            disabled={parsed.methods.length === 0}
          >
            <option value="query">Query</option>
            <option value="mutation">Mutation</option>
          </select>
          <select
            className="grpc-method-select"
            value={selectedMethod?.name ?? ""}
            onChange={(e) => handleMethodChange(e.target.value)}
            disabled={visibleMethods.length === 0}
          >
            {visibleMethods.length === 0 && <option value="">No methods</option>}
            {visibleMethods.map((m) => (
              <option key={m.name} value={m.name}>{m.name}</option>
            ))}
          </select>
        </div>
        <div className="grpc-topbar-right">
          {protoError && <span className="grpc-topbar-error">{protoError}</span>}
          <button
            className="grpc-send-btn"
            onClick={handleRun}
            disabled={running || !selectedMethod || !roleId || !!protoError}
          >
            {running ? "◼ Cancel" : "▶ Send"}
          </button>
        </div>
      </div>

      {/* Main panels */}
      <div className="grpc-body">
        {/* Left: Body / Proto tabs */}
        <div className="grpc-panel grpc-panel-left">
          <div className="grpc-tabs">
            <button
              className={`grpc-tab${leftTab === "body" ? " active" : ""}`}
              onClick={() => setLeftTab("body")}
            >
              Body
            </button>
            <button
              className={`grpc-tab${leftTab === "proto" ? " active" : ""}`}
              onClick={() => setLeftTab("proto")}
            >
              .proto
            </button>
          </div>
          {leftTab === "body" ? (
            <textarea
              className="grpc-editor"
              value={messageText}
              onChange={(e) => setMessageText(e.target.value)}
              spellCheck={false}
              placeholder={selectedMethod ? "" : "Select a method to edit the request body."}
            />
          ) : (
            <pre className="grpc-code">
              {protoText || (protoError ? "" : "Loading…")}
            </pre>
          )}
        </div>

        {/* Right: Response */}
        <div className="grpc-panel grpc-panel-right">
          <div className="grpc-tabs">
            <button className="grpc-tab active">Response</button>
          </div>
          {error && <div className="grpc-error-bar">{error}</div>}
          <pre className="grpc-code grpc-response">
            {response || (running ? "Waiting for response…" : "Send a request to see the response.")}
          </pre>
        </div>
      </div>
    </div>
  );
}
