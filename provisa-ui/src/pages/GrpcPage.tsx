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

import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import { useNavPayload } from "../hooks/useNavPayload";
import { useScreenInputs } from "../mcp/screenState";
import { useTranslation } from "react-i18next";
import { GrpcCodeView } from "./grpc/GrpcCodeView";
import { Badge, Button, Checkbox, Group, MultiSelect, Select, Tabs, Text } from "@mantine/core";
import { useAuth } from "../context/AuthContext";
import { useDomainFilter } from "../context/DomainFilterContext";
import "./GrpcPage.css";
import { requestFailed, serverMessage, type ServerMessageShape } from "../i18n/serverMessage";
import { Tip } from "../components/Tip";

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
      /rpc\s+(\w+)\s*\(([\w.]+)\)\s*returns\s*\((?:stream\s+)?([\w.]+)\)/g,
    )) {
      const [, name, requestMsg] = m;
      if (name.startsWith("Query")) {
        methods.push({
          name,
          operation: "query",
          typeName: name.slice(5),
          requestMsgName: requestMsg,
        });
      } else if (name.startsWith("Insert")) {
        methods.push({
          name,
          operation: "mutation",
          typeName: name.slice(6),
          requestMsgName: requestMsg,
        });
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

// Mirrors provisa/grpc/query_ir.py::AGG_FUNCS — the funcs a Query{Type}Aggregate/GroupBy
// request may restrict its computation to (REQ-1361).
const AGG_FUNCS = ["count", "sum", "avg", "stddev", "variance", "min", "max"];

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

// The group-by projection controls (REQ-1401/REQ-1408), carried together because the NL page's
// call syntax states them together and the body must round-trip both.
interface NodeProjection {
  includeNodes: boolean;
  include: string[];
}

function buildMessageTemplate(
  method: ProtoMethod,
  messages: Record<string, ProtoField[]>,
  byColumns?: string[] | null,
  funcs?: string[] | null,
  projection?: NodeProjection | null,
  columns?: string[] | null,
): string {
  if (method.operation === "query") {
    // REQ-1359: Query{Type}Aggregate takes google.protobuf.Empty; Query{Type}GroupBy takes
    // a { by: [...] } request — neither is the generic {Type}Filter shape below. `funcs` must
    // be embedded here (not left to the picker-sync effect) so the auto-run race can't fire
    // with a body that's missing it (REQ-1361 bug: "Open in gRPC" executed the wrong body).
    if (method.typeName.endsWith("Aggregate")) {
      const body: Record<string, unknown> = {};
      if (funcs && funcs.length > 0) body.funcs = funcs;
      if (columns && columns.length > 0) body.columns = columns; // REQ-1882
      return JSON.stringify(body, null, 2);
    }
    if (method.typeName.endsWith("GroupBy")) {
      const body: Record<string, unknown> = { by: byColumns ?? [] };
      if (funcs && funcs.length > 0) body.funcs = funcs;
      if (columns && columns.length > 0) body.columns = columns; // REQ-1882
      if (projection?.includeNodes) {
        body.include_nodes = true;
        if (projection.include.length > 0) body.include = projection.include;
      }
      return JSON.stringify(body, null, 2);
    }
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

interface GrpcHandoff {
  method: string;
  byColumns: string[] | null;
  funcs: string[] | null;
  projection: NodeProjection | null;
  // The request body handed with the method, whole: filter, limit, offset, read_mask.
  body: Record<string, unknown> | null;
  autoRun: boolean;
  seq: number;
}

function listArg(signature: string, pattern: RegExp): string[] | null {
  const m = signature.match(pattern);
  return m
    ? m[1]
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean)
    : null;
}

/** The call syntax a hand-off carries, read once: by-columns, funcs and the nodes projection.
 *  REQ-1401/REQ-1408: the NL page hands over the whole call syntax, so the projection it chose is
 *  read out of it too, or the page runs a narrower query than the one the visitor opened. */
function parseGrpcHandoff(
  signature: string,
  body: Record<string, unknown> | null,
  autoRun: boolean,
  seq: number,
): GrpcHandoff {
  const includeNodes = body?.include_nodes === true;
  const projection: NodeProjection | null =
    /include_nodes\s*=\s*true/.test(signature) || includeNodes
      ? {
          includeNodes: true,
          include: (body?.include as string[]) ?? listArg(signature, /include=\[([^\]]*)\]/) ?? [],
        }
      : null;
  return {
    method: signature,
    byColumns: (body?.by as string[]) ?? listArg(signature, /\(by=\[([^\]]*)\]/),
    funcs: (body?.funcs as string[]) ?? listArg(signature, /funcs=\[([^\]]*)\]/),
    projection,
    body,
    autoRun,
    seq,
  };
}

/** Whether a method's request body is written by the aggregate pickers. */
function hasAggPicker(method: ProtoMethod): boolean {
  return (
    method.operation === "query" &&
    (method.typeName.endsWith("Aggregate") || method.typeName.endsWith("GroupBy"))
  );
}

const PICKER_FIELDS = ["by", "funcs", "columns", "include_nodes", "include"];

/** The body as text once the pickers have had their say. What they do not write stays: a GroupBy
 * body carries filter, order_by, limit and offset too. */
function withPickerFields(text: string, fields: Record<string, unknown>): string {
  let body: Record<string, unknown> = {};
  try {
    body = JSON.parse(text) as Record<string, unknown>;
  } catch {
    // Mid-edit text: the pickers' fields are what is known.
  }
  for (const key of PICKER_FIELDS) delete body[key];
  return JSON.stringify({ ...fields, ...body }, null, 2);
}

/** The server's own refusal of a request: its catalog message, else "<op> failed (<status>)". */
async function refusal(res: Response, op: string): Promise<string> {
  const fallback = requestFailed(op, res.status);
  const text = await res.text();
  let body: ServerMessageShape | null;
  try {
    body = JSON.parse(text) as ServerMessageShape;
  } catch {
    return fallback; // not a JSON error body (a proxy's HTML page): the status is what is known
  }
  return serverMessage(body, fallback);
}

export function GrpcPage() {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const { checkedDomains } = useDomainFilter();
  // REQ-1620: every call on this page acts as the ACTIVE roles — one role, or under "Role: All"
  // the comma-separated set the server serves as their meta-role — in the path of the routes that
  // take a role there and in X-Provisa-Role. The domain filter spans every active role, so naming
  // only the first role asked for domains that role does not reach and was refused.
  const roleId = actingRoles ?? "";
  const domainsParam = checkedDomains.size > 0 ? [...checkedDomains].join(",") : "";

  // A method handed to the page (NL "Open in gRPC", Polly) with the call syntax it was chosen with,
  // whether the page was just opened or already open (useNavPayload below). Each hand-off has its
  // own sequence number: the proto fetch selects it, and the auto-run effect runs it once.
  const [handoff, setHandoff] = useState<GrpcHandoff | null>(null);
  useNavPayload<{
    grpcMethod?: string;
    grpcBody?: Record<string, unknown>;
    autoRun?: boolean;
  }>((payload) => {
    if (!payload.grpcMethod) return;
    const signature = payload.grpcMethod;
    const body = payload.grpcBody ?? null;
    setHandoff((prev) =>
      parseGrpcHandoff(signature, body, payload.autoRun === true, (prev?.seq ?? 0) + 1),
    );
  });
  // The hand-off the page has selected, which is when it may run: a hand-off of the method already
  // selected must not run the body already on screen.
  const [selectedSeq, setSelectedSeq] = useState<number | null>(null);

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
  // REQ-1361 picker: group-by columns + restricted agg funcs for Query{Type}Aggregate/GroupBy.
  const [groupByCols, setGroupByCols] = useState<string[]>([]);
  const [selectedFuncs, setSelectedFuncs] = useState<string[]>([]);
  const [selectedAggColumns, setSelectedAggColumns] = useState<string[]>([]); // REQ-1882
  const [fetchedGroupByColumns, setFetchedGroupByColumns] = useState<string[]>([]);
  const [includeNodes, setIncludeNodes] = useState(false);
  const [includeFields, setIncludeFields] = useState<string[]>([]);

  // Synthetic methods for registered commands (one generic CallCommand RPC, one entry per command).
  const commandsMapRef = useRef<Record<string, CommandDef>>({});
  const commandMethods: ProtoMethod[] = commands.map((c) => ({
    name: c.name,
    operation: "command",
    typeName: c.name,
    requestMsgName: "CommandRequest",
  }));
  const allMethods = [...parsed.methods, ...commandMethods];

  const selectMethod = useCallback(
    (
      method: ProtoMethod,
      proto: ParsedProto,
      byColumns?: string[] | null,
      funcs?: string[] | null,
      projection?: NodeProjection | null,
      body?: Record<string, unknown> | null,
    ) => {
      setSelectedMethod(method);
      setGroupByCols(byColumns ?? []);
      setSelectedFuncs(funcs ?? []);
      // REQ-1882: not carried over nav call syntax, same as before this field existed.
      setSelectedAggColumns((body?.columns as string[]) ?? []);
      setIncludeNodes(projection?.includeNodes ?? false);
      setIncludeFields(projection?.include ?? []);
      setMessageText(
        body
          ? JSON.stringify(body, null, 2)
          : method.operation === "command"
            ? buildCommandTemplate(commandsMapRef.current[method.name])
            : buildMessageTemplate(method, proto.messages, byColumns, funcs, projection, null),
      );
      setResponse("");
      setError("");
    },
    [],
  );

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
    else {
      setSelectedMethod(null);
      setMessageText("");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- allMethods is derived from parsed+commands, which are the real deps; listing the fresh array each render would loop
  }, [opType, parsed, commands, selectMethod, selectedMethod]);

  const navSelectDoneRef = useRef(false);

  // REQ-1361: keep the group-by/agg-funcs picker in sync with messageText for Aggregate/GroupBy
  // query methods — picker selections are the source of truth for those two fields.
  useEffect(() => {
    if (!selectedMethod || !hasAggPicker(selectedMethod)) return;
    const isGroupBy = selectedMethod.typeName.endsWith("GroupBy");
    const body: Record<string, unknown> = isGroupBy ? { by: groupByCols } : {};
    if (selectedFuncs.length > 0) body.funcs = selectedFuncs;
    if (selectedAggColumns.length > 0) body.columns = selectedAggColumns; // REQ-1882
    if (isGroupBy && includeNodes) {
      body.include_nodes = true;
      if (includeFields.length > 0) body.include = includeFields;
    }
    // eslint-disable-next-line react-hooks/set-state-in-effect -- syncs picker state (source of truth) into the JSON editor; selectedMethod change is the trigger, not something read back
    setMessageText((text) => withPickerFields(text, body));
  }, [groupByCols, selectedFuncs, selectedAggColumns, includeNodes, includeFields, selectedMethod]);

  const protoFetchRef = useRef(0);
  const fetchProto = useCallback(
    async (rid: string, domains: string) => {
      setProtoError("");
      // Only the latest fetch selects: an earlier one answering late must not undo a hand-off.
      const fetchId = ++protoFetchRef.current;
      try {
        const url = domains
          ? `/data/proto/${encodeURIComponent(rid)}?domains=${encodeURIComponent(domains)}`
          : `/data/proto/${encodeURIComponent(rid)}`;
        const res = await fetch(url);
        if (!res.ok) {
          setProtoError(await refusal(res, "Proto"));
          return;
        }
        const text = await res.text();
        if (fetchId !== protoFetchRef.current) return;
        setProtoText(text);
        const p = parseProto(text);
        // Strip the "(by=[...])" call-syntax suffix the NL page's query text carries (REQ-1359)
        // before matching against the bare typeName the proto actually exposes.
        const preferred = handoff
          ? handoff.method.replace(/^Query/, "").replace(/\(.*$/, "")
          : "";
        const navM = preferred
          ? p.methods.find((m) => m.typeName === preferred && m.operation === "query")
          : null;
        const initial =
          navM ?? p.methods.find((m) => m.operation === "query") ?? p.methods[0] ?? null;
        if (initial) {
          setOpType(initial.operation);
          selectMethod(
            initial,
            p,
            navM ? handoff?.byColumns : null,
            navM ? handoff?.funcs : null,
            navM ? handoff?.projection : null,
            navM ? handoff?.body : null,
          );
          // eslint-disable-next-line react-hooks/immutability -- one-shot guard written after async fetch resolves; read occurs in a separate effect that guards against re-auto-selection
          if (navM) navSelectDoneRef.current = true;
          if (navM && handoff) setSelectedSeq(handoff.seq);
        }
        setParsed(p);
      } catch (e) {
        setProtoError(String(e));
      }
    },
    [handoff, selectMethod],
  );

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- triggers async proto fetch; all setState calls occur inside the async callback, not synchronously in the effect body
    if (roleId) void fetchProto(roleId, domainsParam);
  }, [roleId, domainsParam, fetchProto]);

  const fetchCommands = useCallback(async (rid: string) => {
    try {
      const res = await fetch(`/data/grpc-commands/${encodeURIComponent(rid)}`);
      if (!res.ok) {
        setCommands([]);
        setProtoError(await refusal(res, "Commands"));
        return;
      }
      const list = (await res.json()) as CommandDef[];
      commandsMapRef.current = Object.fromEntries(list.map((c) => [c.name, c]));
      setCommands(list);
    } catch (e) {
      setCommands([]);
      setProtoError(String(e));
    }
  }, []);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- triggers async command fetch; setState occurs inside the async callback
    if (roleId) void fetchCommands(roleId);
  }, [roleId, fetchCommands]);

  // REQ-1361: the group-by picker must offer only columns the server's {Type}DistinctOnColumn
  // enum actually accepts — the {Type}Filter message's fields (a different, broader set) yield
  // runtime 400s when selected.
  // REQ-1882: also fetched for an Aggregate method now, so the columns picker below has the
  // same aggregate-eligible column list the server's _agg_fields_selection restricts against —
  // the endpoint returns ctx.aggregate_columns either way, so one fetch serves both pickers.
  const isAggOrGroupByMethod =
    !!selectedMethod &&
    (selectedMethod.typeName.endsWith("GroupBy") || selectedMethod.typeName.endsWith("Aggregate"));
  useEffect(() => {
    if (!selectedMethod || !roleId || !isAggOrGroupByMethod) return;
    let cancelled = false;
    void (async () => {
      try {
        const res = await fetch(
          `/data/grpc-group-by-columns/${encodeURIComponent(roleId)}/${encodeURIComponent(selectedMethod.typeName)}`,
        );
        if (!res.ok) {
          const message = await refusal(res, "Columns");
          if (!cancelled) {
            setFetchedGroupByColumns([]);
            setError(message);
          }
          return;
        }
        const cols = (await res.json()) as string[];
        if (!cancelled) setFetchedGroupByColumns(cols);
      } catch (e) {
        if (!cancelled) {
          setFetchedGroupByColumns([]);
          setError(String(e));
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedMethod, roleId, isAggOrGroupByMethod]);

  // Only a GroupBy method has group-by ("by") columns to offer, and the previous method's must
  // not linger. Deriving that keeps the fetch the sole writer of the state.
  const groupByColumnOptions = useMemo(
    () => (selectedMethod?.typeName.endsWith("GroupBy") && roleId ? fetchedGroupByColumns : []),
    [selectedMethod, roleId, fetchedGroupByColumns],
  );

  // REQ-1882: the aggregate-function columns picker (sum/avg/stddev/variance over a caller-
  // chosen numeric subset, min/max over a caller-chosen comparable subset) — offered for both
  // Aggregate and GroupBy, same fetched list as the by-columns picker (see effect above).
  const aggColumnOptions = useMemo(
    () => (isAggOrGroupByMethod && roleId ? fetchedGroupByColumns : []),
    [isAggOrGroupByMethod, roleId, fetchedGroupByColumns],
  );

  // Include entries a group-by request accepts (REQ-1408), read off the proto's data message:
  // a scalar field is a base column, a singular message-typed field is a many-to-one relationship
  // whose own scalars are reachable as "rel.column" dot-paths. Mirrors query_ir.py::
  // _include_node_fields, which resolves the same three shapes server-side.
  const includeOptions = useMemo(() => {
    if (!selectedMethod?.typeName.endsWith("GroupBy")) return [];
    const typeName = selectedMethod.typeName.slice(0, -"GroupBy".length);
    const fields = parsed.messages[typeName] ?? [];
    const options: string[] = [];
    for (const f of fields) {
      const related = parsed.messages[f.protoType];
      if (!related) {
        options.push(f.name);
        continue;
      }
      if (f.repeated) continue;
      options.push(f.name);
      for (const rf of related) {
        if (!parsed.messages[rf.protoType]) options.push(`${f.name}.${rf.name}`);
      }
    }
    return options;
  }, [selectedMethod, parsed]);

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
        } catch {
          /* use defaults */
        }
        const res = await fetch(`/data/grpc-command/${encodeURIComponent(roleId)}`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "x-provisa-role": roleId },
          body: JSON.stringify({ name, args_json: JSON.stringify(args) }),
        });
        const json = await res.json();
        if (!res.ok) {
          setError(serverMessage(json as { detail?: string }, JSON.stringify(json)));
        } else {
          setResponse(JSON.stringify(json, null, 2));
        }
        return;
      }
      // The acting role travels ONLY in X-Provisa-Role: the server refuses a body role that
      // differs from the role the request runs as, and under "Role: All" that is a meta-role.
      let body: Record<string, unknown> = {};
      try {
        body = JSON.parse(messageText) as Record<string, unknown>;
      } catch {
        /* use default body */
      }
      const res = await fetch(`/data/grpc/${encodeURIComponent(selectedMethod.typeName)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-provisa-role": roleId },
        body: JSON.stringify(body),
      });
      const json = await res.json();
      if (!res.ok) {
        setError(serverMessage(json as { detail?: string }, JSON.stringify(json)));
      } else {
        setResponse(JSON.stringify(json, null, 2));
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setRunning(false);
    }
  }, [selectedMethod, roleId, messageText]);

  const navAutoRunSeqRef = useRef<number | null>(null);
  useEffect(() => {
    if (!handoff?.autoRun || navAutoRunSeqRef.current === handoff.seq || !selectedMethod) return;
    // Wait until the proto fetch has selected the handed method and its body.
    if (selectedSeq !== handoff.seq) return;
    navAutoRunSeqRef.current = handoff.seq;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- runs a handed-over call once per hand-off, guarded by navAutoRunSeqRef; the run's setState calls happen in its async body
    void handleRun();
  }, [selectedMethod, handoff, selectedSeq, handleRun]);

  const visibleMethods = allMethods.filter((m) => m.operation === opType);

  // REQ-1361: an Aggregate/GroupBy request with zero funcs selected would silently compute
  // every function in AGG_FUNCS — require an explicit choice instead.
  const isAggOrGroupBy = !!selectedMethod && hasAggPicker(selectedMethod);
  const funcsRequired = isAggOrGroupBy && selectedFuncs.length === 0;

  // REQ-2115: Polly's read_screen reads the call on screen from here, never the response's rows.
  useScreenInputs("/grpc", {
    operation: opType,
    method: selectedMethod?.name ?? null,
    body: messageText,
    status: error ? "failed" : running ? "running" : response ? "answered" : "idle",
    error: error || protoError || null,
  });

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
            style={{
              fontFamily: "monospace",
              fontWeight: 400,
              textTransform: "none",
              flexShrink: 0,
            }}
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
            <Tip text={protoError}>
              <Text
                size="xs"
                c="red"
                data-testid="grpc-proto-error"
                style={{ maxWidth: 300 }}
                truncate="end"
              >
                {protoError}
              </Text>
            </Tip>
          )}
          {!protoError && funcsRequired && (
            <Text size="xs" c="red" data-testid="grpc-funcs-required-hint">
              {t("grpcPage.selectAtLeastOneFunc")}
            </Text>
          )}
          <Button
            size="xs"
            data-testid="grpc-send-btn"
            onClick={handleRun}
            disabled={running || !selectedMethod || !roleId || !!protoError || funcsRequired}
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
          {leftTab === "body" && isAggOrGroupBy && selectedMethod && (
              <div className="grpc-agg-picker" data-testid="grpc-agg-picker">
                {selectedMethod.typeName.endsWith("GroupBy") && (
                  <MultiSelect
                    aria-label={t("grpcPage.groupByColumns")}
                    data-testid="grpc-groupby-picker"
                    size="xs"
                    placeholder={t("grpcPage.groupByColumns")}
                    data={groupByColumnOptions}
                    value={groupByCols}
                    onChange={setGroupByCols}
                    searchable
                    clearable
                  />
                )}
                {selectedMethod.typeName.endsWith("GroupBy") && (
                  <>
                    <Checkbox
                      mt="sm"
                      size="xs"
                      data-testid="grpc-include-nodes-checkbox"
                      label={t("grpcPage.includeNodes")}
                      checked={includeNodes}
                      onChange={(e) => setIncludeNodes(e.currentTarget.checked)}
                    />
                    {includeNodes && (
                      <MultiSelect
                        mt="xs"
                        aria-label={t("grpcPage.includeFields")}
                        data-testid="grpc-include-picker"
                        size="xs"
                        placeholder={t("grpcPage.includeFields")}
                        data={includeOptions}
                        value={includeFields}
                        onChange={setIncludeFields}
                        searchable
                        clearable
                      />
                    )}
                  </>
                )}
                <Checkbox.Group
                  data-testid="grpc-funcs-picker"
                  value={selectedFuncs}
                  onChange={setSelectedFuncs}
                  label={t("grpcPage.aggregateFunctions")}
                  mt="sm"
                  mb="sm"
                >
                  <Group gap="xs" mt={8} mb={4}>
                    {AGG_FUNCS.map((fn) => (
                      <Checkbox
                        key={fn}
                        value={fn}
                        label={fn}
                        size="xs"
                        data-testid={`grpc-func-${fn}`}
                      />
                    ))}
                  </Group>
                </Checkbox.Group>
                <MultiSelect
                  mt="xs"
                  aria-label={t("grpcPage.aggregateColumns")}
                  data-testid="grpc-columns-picker"
                  size="xs"
                  label={t("grpcPage.aggregateColumns")}
                  placeholder={t("grpcPage.aggregateColumns")}
                  data={aggColumnOptions}
                  value={selectedAggColumns}
                  onChange={setSelectedAggColumns}
                  searchable
                  clearable
                />
              </div>
            )}
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
        {/* REQ-2010: marked once a response is in, which a demo tour step waits for. */}
        <div className="grpc-panel grpc-panel-right" data-answered={response ? "" : undefined}>
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
