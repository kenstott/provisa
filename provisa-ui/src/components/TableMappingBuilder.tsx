import { useState } from "react";

const TRINO_TYPES = [
  "VARCHAR", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
  "DOUBLE", "REAL", "DECIMAL", "BOOLEAN", "DATE",
  "TIMESTAMP", "VARBINARY", "JSON",
];

const REDIS_VALUE_TYPES = [
  { value: "hash", label: "Hash" },
  { value: "string", label: "String" },
  { value: "zset", label: "Sorted Set" },
  { value: "list", label: "List" },
];

interface ColumnDef {
  name: string;
  type: string;
  field: string;      // Redis field mapping / ES dot-path / Mongo JSONPath
  family: string;     // Accumulo column family
  qualifier: string;  // Accumulo column qualifier
}

interface TableMappingBuilderProps {
  sourceType: string;
  onSave: (mapping: TableMapping) => void;
  onCancel: () => void;
}

export interface TableMapping {
  sourceType: string;
  tableName: string;
  // Redis-specific
  keyPattern?: string;
  keyColumn?: string;
  valueType?: string;
  // MongoDB-specific
  collection?: string;
  discover?: boolean;
  // Elasticsearch-specific
  indexPattern?: string;
  // Prometheus-specific
  metric?: string;
  labels?: string[];
  valueColumn?: string;
  defaultRange?: string;
  // Accumulo-specific
  accumuloTable?: string;
  // Common
  columns: ColumnDef[];
}

function emptyColumn(): ColumnDef {
  return { name: "", type: "VARCHAR", field: "", family: "", qualifier: "" };
}

/** Redis: key pattern, key column, value type, column rows with field mapping */
function RedisForm({ mapping, setMapping }: {
  mapping: TableMapping;
  setMapping: (m: TableMapping) => void;
}) {
  return (
    <>
      <label>
        Key Pattern
        <input
          value={mapping.keyPattern ?? ""}
          onChange={(e) => setMapping({ ...mapping, keyPattern: e.target.value })}
          placeholder="e.g. user:*"
        />
      </label>
      <label>
        Key Column Name
        <input
          value={mapping.keyColumn ?? ""}
          onChange={(e) => setMapping({ ...mapping, keyColumn: e.target.value })}
          placeholder="e.g. user_id"
        />
      </label>
      <label>
        Value Type
        <select
          value={mapping.valueType ?? "hash"}
          onChange={(e) => setMapping({ ...mapping, valueType: e.target.value })}
        >
          {REDIS_VALUE_TYPES.map((vt) => (
            <option key={vt.value} value={vt.value}>{vt.label}</option>
          ))}
        </select>
      </label>
    </>
  );
}

/** MongoDB: collection name, discover toggle, column rows with JSONPath */
function MongoForm({ mapping, setMapping }: {
  mapping: TableMapping;
  setMapping: (m: TableMapping) => void;
}) {
  return (
    <>
      <label>
        Collection
        <input
          value={mapping.collection ?? ""}
          onChange={(e) => setMapping({ ...mapping, collection: e.target.value })}
          placeholder="e.g. users"
        />
      </label>
      <label style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
        <input
          type="checkbox"
          checked={mapping.discover ?? false}
          onChange={(e) => setMapping({ ...mapping, discover: e.target.checked })}
        />
        Auto-discover schema
      </label>
    </>
  );
}

/** Elasticsearch: index pattern, column rows with dot-path */
function ElasticsearchForm({ mapping, setMapping }: {
  mapping: TableMapping;
  setMapping: (m: TableMapping) => void;
}) {
  return (
    <>
      <label>
        Index Pattern
        <input
          value={mapping.indexPattern ?? ""}
          onChange={(e) => setMapping({ ...mapping, indexPattern: e.target.value })}
          placeholder="e.g. nginx-access-*"
        />
      </label>
      <label style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
        <input
          type="checkbox"
          checked={mapping.discover ?? false}
          onChange={(e) => setMapping({ ...mapping, discover: e.target.checked })}
        />
        Auto-discover schema
      </label>
    </>
  );
}

/** Prometheus: metric name, label checkboxes, value column, time range */
function PrometheusForm({ mapping, setMapping }: {
  mapping: TableMapping;
  setMapping: (m: TableMapping) => void;
}) {
  const [labelInput, setLabelInput] = useState("");

  const addLabel = () => {
    const trimmed = labelInput.trim();
    if (trimmed && !(mapping.labels ?? []).includes(trimmed)) {
      setMapping({ ...mapping, labels: [...(mapping.labels ?? []), trimmed] });
      setLabelInput("");
    }
  };

  const removeLabel = (label: string) => {
    setMapping({ ...mapping, labels: (mapping.labels ?? []).filter((l) => l !== label) });
  };

  return (
    <>
      <label>
        Metric Name
        <input
          value={mapping.metric ?? ""}
          onChange={(e) => setMapping({ ...mapping, metric: e.target.value })}
          placeholder="e.g. http_request_duration_seconds"
        />
      </label>
      <label>
        Value Column Name
        <input
          value={mapping.valueColumn ?? "value"}
          onChange={(e) => setMapping({ ...mapping, valueColumn: e.target.value })}
        />
      </label>
      <label>
        Default Time Range
        <input
          value={mapping.defaultRange ?? "1h"}
          onChange={(e) => setMapping({ ...mapping, defaultRange: e.target.value })}
          placeholder="e.g. 1h, 24h, 7d"
        />
      </label>
      <div style={{ gridColumn: "1 / -1" }}>
        <label>Labels as Columns</label>
        <div style={{ display: "flex", gap: "0.25rem", flexWrap: "wrap", marginBottom: "0.25rem" }}>
          {(mapping.labels ?? []).map((label) => (
            <span
              key={label}
              style={{
                background: "var(--bg-secondary, #e0e0e0)",
                padding: "0.15rem 0.5rem",
                borderRadius: "4px",
                fontSize: "0.85rem",
                display: "inline-flex",
                alignItems: "center",
                gap: "0.25rem",
              }}
            >
              {label}
              <button
                onClick={() => removeLabel(label)}
                style={{ border: "none", background: "none", cursor: "pointer", padding: 0, fontSize: "0.8rem" }}
              >
                x
              </button>
            </span>
          ))}
        </div>
        <div style={{ display: "flex", gap: "0.25rem" }}>
          <input
            value={labelInput}
            onChange={(e) => setLabelInput(e.target.value)}
            placeholder="e.g. method"
            style={{ width: "10rem" }}
            onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); addLabel(); } }}
          />
          <button onClick={addLabel} type="button" style={{ padding: "0.25rem 0.5rem", fontSize: "0.8rem" }}>
            Add Label
          </button>
        </div>
      </div>
    </>
  );
}

/** Accumulo: table name, column rows with family/qualifier */
function AccumuloForm({ mapping, setMapping }: {
  mapping: TableMapping;
  setMapping: (m: TableMapping) => void;
}) {
  return (
    <label>
      Accumulo Table
      <input
        value={mapping.accumuloTable ?? ""}
        onChange={(e) => setMapping({ ...mapping, accumuloTable: e.target.value })}
        placeholder="e.g. graph_edges"
      />
    </label>
  );
}

/** Column header label for the field mapping column, varies by source type. */
function fieldLabel(sourceType: string): string {
  switch (sourceType) {
    case "redis": return "Redis Field";
    case "mongodb": return "JSONPath";
    case "elasticsearch": return "Dot-Path";
    case "accumulo": return "Family";
    default: return "Field";
  }
}

function showQualifier(sourceType: string): boolean {
  return sourceType === "accumulo";
}

export function TableMappingBuilder({ sourceType, onSave, onCancel }: TableMappingBuilderProps) {
  const [mapping, setMapping] = useState<TableMapping>({
    sourceType,
    tableName: "",
    keyPattern: "",
    keyColumn: "",
    valueType: "hash",
    collection: "",
    discover: false,
    indexPattern: "",
    metric: "",
    labels: [],
    valueColumn: "value",
    defaultRange: "1h",
    accumuloTable: "",
    columns: [emptyColumn()],
  });

  const addColumn = () => {
    setMapping({ ...mapping, columns: [...mapping.columns, emptyColumn()] });
  };

  const removeColumn = (idx: number) => {
    setMapping({ ...mapping, columns: mapping.columns.filter((_, i) => i !== idx) });
  };

  const updateColumn = (idx: number, patch: Partial<ColumnDef>) => {
    setMapping({
      ...mapping,
      columns: mapping.columns.map((c, i) => (i === idx ? { ...c, ...patch } : c)),
    });
  };

  const handleSave = () => {
    if (!mapping.tableName.trim()) return;
    onSave(mapping);
  };

  return (
    <div className="form-card" style={{ marginTop: "1rem" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h3 style={{ margin: 0 }}>Table Mapping: {sourceType}</h3>
        <button onClick={onCancel} style={{ padding: "0.25rem 0.5rem" }}>Cancel</button>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.5rem", marginTop: "0.5rem" }}>
        <label>
          Table Name
          <input
            required
            value={mapping.tableName}
            onChange={(e) => setMapping({ ...mapping, tableName: e.target.value })}
            placeholder="e.g. user_sessions"
          />
        </label>

        {sourceType === "redis" && <RedisForm mapping={mapping} setMapping={setMapping} />}
        {sourceType === "mongodb" && <MongoForm mapping={mapping} setMapping={setMapping} />}
        {sourceType === "elasticsearch" && <ElasticsearchForm mapping={mapping} setMapping={setMapping} />}
        {sourceType === "prometheus" && <PrometheusForm mapping={mapping} setMapping={setMapping} />}
        {sourceType === "accumulo" && <AccumuloForm mapping={mapping} setMapping={setMapping} />}
      </div>

      {sourceType !== "prometheus" && (
        <>
          <h4 style={{ marginTop: "1rem", marginBottom: "0.25rem" }}>
            Columns
            <button
              onClick={addColumn}
              style={{ marginLeft: "0.5rem", padding: "0.15rem 0.5rem", fontSize: "0.8rem" }}
            >
              + Add
            </button>
          </h4>
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>{fieldLabel(sourceType)}</th>
                {showQualifier(sourceType) && <th>Qualifier</th>}
                <th></th>
              </tr>
            </thead>
            <tbody>
              {mapping.columns.map((col, idx) => (
                <tr key={idx}>
                  <td>
                    <input
                      value={col.name}
                      onChange={(e) => updateColumn(idx, { name: e.target.value })}
                      placeholder="column_name"
                      style={{ width: "8rem" }}
                    />
                  </td>
                  <td>
                    <select value={col.type} onChange={(e) => updateColumn(idx, { type: e.target.value })}>
                      {TRINO_TYPES.map((t) => (
                        <option key={t} value={t}>{t}</option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <input
                      value={col.field}
                      onChange={(e) => updateColumn(idx, { field: e.target.value })}
                      placeholder={sourceType === "mongodb" ? "$.path.to.field" : "field_name"}
                      style={{ width: "10rem" }}
                    />
                  </td>
                  {showQualifier(sourceType) && (
                    <td>
                      <input
                        value={col.qualifier}
                        onChange={(e) => updateColumn(idx, { qualifier: e.target.value })}
                        placeholder="qualifier"
                        style={{ width: "8rem" }}
                      />
                    </td>
                  )}
                  <td>
                    <button
                      onClick={() => removeColumn(idx)}
                      style={{ padding: "0.15rem 0.4rem", fontSize: "0.75rem" }}
                      className="destructive"
                    >
                      X
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      <div style={{ marginTop: "0.75rem" }}>
        <button onClick={handleSave}>Save Mapping</button>
      </div>
    </div>
  );
}
