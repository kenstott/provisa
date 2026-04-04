/** Types matching provisa/api/admin/types.py */

export interface Source {
  id: string;
  type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  dialect: string | null;
  cacheEnabled: boolean;
  cacheTtl: number | null;
}

export interface Domain {
  id: string;
  description: string;
}

export interface TableColumn {
  id: number;
  columnName: string;
  visibleTo: string[];
  writableBy: string[];
  unmaskedTo: string[];
  maskType: string | null;
  maskPattern: string | null;
  maskReplace: string | null;
  maskValue: string | null;
  maskPrecision: string | null;
  alias: string | null;
  description: string | null;
}

export interface RegisteredTable {
  id: number;
  sourceId: string;
  domainId: string;
  schemaName: string;
  tableName: string;
  governance: string;
  alias: string | null;
  description: string | null;
  cacheTtl: number | null;
  columns: TableColumn[];
}

export interface Relationship {
  id: number;
  sourceTableId: number;
  targetTableId: number;
  sourceTableName: string;
  targetTableName: string;
  sourceColumn: string;
  targetColumn: string;
  cardinality: string;
  materialize: boolean;
  refreshInterval: number;
}

export interface RLSRule {
  id: number;
  tableId: number;
  roleId: string;
  filterExpr: string;
}

export interface PersistedQuery {
  id: number;
  name: string;
  queryText: string;
  status: "submitted" | "approved" | "deprecated" | "flagged";
  submittedBy: string;
  approvedBy: string | null;
  rejectionReason: string | null;
}

export interface MutationResult {
  success: boolean;
  message: string;
}
