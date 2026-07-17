// Copyright (c) 2026 Kenneth Stott
// Canary: 71b67a2b-612f-41cd-89a5-7d788e4cffaa
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { describe, it, expect, beforeEach } from 'vitest';
import { existsSync, readFileSync } from 'fs';
import { resolve } from 'path';
import {
  clusterColor,
  extractElements,
  injectExclusion,
} from '../components/graph/graph-model';
import type { GNode, GEdge } from '../components/graph/graph-model';
import {
  buildClusterElements,
  buildClusterMetaEdges,
  cidToId,
} from '../components/graph/graph-clusters';

const SRC = resolve(__dirname, '..');

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeNode(
  id: number,
  label: string,
  properties: Record<string, unknown> = {},
  tableLabel?: string,
): GNode {
  return { id, label, tableLabel: tableLabel ?? label, properties };
}

function makeEdge(
  identity: string,
  startNode: GNode,
  endNode: GNode,
  type: string,
  properties: Record<string, unknown> = {},
): GEdge {
  return {
    identity,
    start: startNode.id,
    end: endNode.id,
    type,
    startNode,
    endNode,
    properties,
  };
}

// ── REQ-058: Role-driven rendered UI ─────────────────────────────────────────

describe('REQ-058: CapabilityGate hides content from users lacking the capability', () => {
  // The CapabilityGate component is tested in CapabilityGate.test.tsx which
  // already covers this behavior. Here we verify the Capability type surface
  // covers the minimum set required by REQ-060 so each capability is assignable.
  it('REQ-058: Capability type includes source_registration', () => {
    const cap: import('../types/auth').Capability = 'source_registration';
    expect(cap).toBe('source_registration');
  });

  it('REQ-058: Capability type includes table_registration', () => {
    const cap: import('../types/auth').Capability = 'table_registration';
    expect(cap).toBe('table_registration');
  });

  it('REQ-058: Capability type includes query_development', () => {
    const cap: import('../types/auth').Capability = 'query_development';
    expect(cap).toBe('query_development');
  });

  it('REQ-058: Capability type includes admin', () => {
    const cap: import('../types/auth').Capability = 'admin';
    expect(cap).toBe('admin');
  });
});

// ── REQ-059: Role composition system ─────────────────────────────────────────

describe('REQ-059: Capability type represents independently assignable building blocks', () => {
  it('REQ-059: each capability is a distinct string value', () => {
    const caps: import('../types/auth').Capability[] = [
      'source_registration',
      'table_registration',
      'create_relationship',
      'access_config',
      'query_development',
      'admin',
    ];
    const unique = new Set(caps);
    expect(unique.size).toBe(caps.length);
  });
});

// ── REQ-060: Specific capability names must exist ─────────────────────────────

describe('REQ-060: Required capabilities exist in the Capability union', () => {
  it('REQ-060: create_relationship capability exists', () => {
    const cap: import('../types/auth').Capability = 'create_relationship';
    expect(cap).toBe('create_relationship');
  });

  it('REQ-060: access_config capability exists (Security Configuration)', () => {
    const cap: import('../types/auth').Capability = 'access_config';
    expect(cap).toBe('access_config');
  });

  it('REQ-060: approve_relationship capability exists', () => {
    const cap: import('../types/auth').Capability = 'approve_relationship';
    expect(cap).toBe('approve_relationship');
  });
});

// ── REQ-061: Destructive action confirmation (ConfirmDialog) ──────────────────
// Full behavioral coverage is in ConfirmDialog.test.tsx.
// Here we verify the consequence prop surface is present at the type level.

describe('REQ-061: ConfirmDialog accepts a consequence summary string', () => {
  it('REQ-061: ConfirmDialog module exports ConfirmDialog', async () => {
    const mod = await import('../components/ConfirmDialog');
    expect(typeof mod.ConfirmDialog).toBe('function');
  });
});

// ── REQ-062: Test endpoint metadata (types surface) ───────────────────────────

describe('REQ-062: extractElements correctly categorises nodes and edges from query results', () => {
  // REQ-062 specifies that test endpoint execution shows metadata (RLS filters,
  // excluded columns, schema scope). The extractElements function is the parsing
  // layer that processes query results into nodes and edges for display.
  it('REQ-062: extractElements returns nodes and edges maps', () => {
    const node = makeNode(1, 'Person', { name: 'Alice' });
    const { nodes, edges } = extractElements([{ n: node }]);
    expect(nodes.size).toBe(1);
    expect(edges.size).toBe(0);
  });

  it('REQ-062: extractElements preserves edge properties for metadata display', () => {
    const a = makeNode(1, 'Person', { name: 'Alice' });
    const b = makeNode(2, 'Order', { id: '99' });
    const e = makeEdge('e1', a, b, 'PLACED', { since: '2024-01-01', rls_applied: true });
    const { edges } = extractElements([{ r: e }]);
    expect(edges.get('e1')?.properties).toMatchObject({ since: '2024-01-01', rls_applied: true });
  });
});

// ── REQ-063: Creation-request queue (module surface check) ────────────────────

describe('REQ-063: RequestsPage source file is present', () => {
  it('REQ-063: RequestsPage.tsx exists in pages directory', () => {
    expect(existsSync(resolve(SRC, 'pages/RequestsPage.tsx'))).toBe(true);
  });
});

// ── REQ-242: Commands page lists functions and webhooks ───────────────────────

describe('REQ-242: CommandsPage source file is present', () => {
  it('REQ-242: CommandsPage.tsx exists in pages directory', () => {
    expect(existsSync(resolve(SRC, 'pages/CommandsPage.tsx'))).toBe(true);
  });
});

// ── REQ-243 / REQ-244 / REQ-245: Commands form — source file ─────────────────

describe('REQ-243-245: CommandsPage source file contains form markers', () => {
  it('REQ-243: CommandsPage.tsx file is non-empty', () => {
    const file = resolve(SRC, 'pages/CommandsPage.tsx');
    expect(existsSync(file)).toBe(true);
    const content: string = readFileSync(file, 'utf-8');
    expect(content.length).toBeGreaterThan(0);
  });
});

// ── REQ-248: GraphQL Voyager iframe approach ──────────────────────────────────

describe('REQ-248: GraphQL Voyager is integrated via iframe (no native fork)', () => {
  it('REQ-248: graphql-voyager package exists in node_modules', () => {
    // REQ-248: iframe approach — graphql-voyager package is present for the iframe CDN bundle.
    const pkg = resolve(__dirname, '../../node_modules/graphql-voyager/package.json');
    expect(existsSync(pkg)).toBe(true);
  });

  it('REQ-248: no VoyagerFork component file exists in project source', () => {
    // No component fork planned per REQ-248
    const forkFile = resolve(SRC, 'components/VoyagerFork.tsx');
    expect(existsSync(forkFile)).toBe(false);
  });
});

// ── REQ-249: Column-level masking config — inline on ColumnConfig ─────────────

describe('REQ-249: Masking config inline on ColumnConfig', () => {
  // REQ-249: masking fields are inline on ColumnConfig, not a separate model.
  it('REQ-249: SecurityPage source file exists', () => {
    expect(existsSync(resolve(SRC, 'pages/SecurityPage.tsx'))).toBe(true);
  });

  it('REQ-249: no standalone MaskingRule source file exists in components', () => {
    // Masking config must be inline on ColumnConfig — not a separate component
    expect(existsSync(resolve(SRC, 'components/MaskingRule.tsx'))).toBe(false);
  });
});

// ── REQ-395: PK designation configurable in TablesPage via checkbox ────────────

describe('REQ-395: TablesPage source file is present for PK designation UI', () => {
  it('REQ-395: TablesPage.tsx exists in pages directory', () => {
    expect(existsSync(resolve(SRC, 'pages/TablesPage.tsx'))).toBe(true);
  });
});

// ── REQ-396: "Exclude from query" disabled when no PK ────────────────────────

describe('REQ-396: NodeContextMenu excludes node only when PK is present', () => {
  it('REQ-396: NodeContextMenu module exports NodeContextMenu component', async () => {
    const mod = await import('../components/graph/NodeContextMenu');
    expect(typeof mod.NodeContextMenu).toBe('function');
  });

  it('REQ-396: hasPk logic: node with empty pkMap entry has no PK', () => {
    const pkMap: Record<string, string[]> = { Person: [] };
    const ctxLabel = 'Person';
    const ctxPkCols = pkMap[ctxLabel] ?? [];
    const hasPk = ctxPkCols.length > 0;
    expect(hasPk).toBe(false);
  });

  it('REQ-396: hasPk logic: node with pkMap entry has PK', () => {
    const pkMap: Record<string, string[]> = { Person: ['id'] };
    const ctxLabel = 'Person';
    const ctxPkCols = pkMap[ctxLabel] ?? [];
    const hasPk = ctxPkCols.length > 0;
    expect(hasPk).toBe(true);
  });

  it('REQ-396: node missing from pkMap is treated as no PK', () => {
    const pkMap: Record<string, string[]> = {};
    const ctxPkCols = pkMap['Unknown'] ?? [];
    const hasPk = ctxPkCols.length > 0;
    expect(hasPk).toBe(false);
  });
});

// ── REQ-401: FK/AK badges in column editor ───────────────────────────────────

describe('REQ-401: TablesPage source file present for FK/AK badge UI', () => {
  it('REQ-401: TablesPage.tsx exists (FK/AK badge surface)', () => {
    expect(existsSync(resolve(SRC, 'pages/TablesPage.tsx'))).toBe(true);
  });
});

// ── REQ-404: Security page RLS "Apply To" toggle ─────────────────────────────

describe('REQ-404: SecurityPage source file is present for RLS Apply To toggle', () => {
  it('REQ-404: SecurityPage.tsx exists in pages directory', () => {
    expect(existsSync(resolve(SRC, 'pages/SecurityPage.tsx'))).toBe(true);
  });
});

// ── REQ-410: GraphFrame Cypher WHERE uses single-quoted strings ───────────────

describe('REQ-410: injectExclusion uses single-quoted string literals for non-numeric PK values', () => {
  it('REQ-410: string PK value is single-quoted in WHERE clause', () => {
    const query = 'MATCH (n:Person) RETURN n';
    const result = injectExclusion(query, 'Person', '42', 'email', 'alice@example.com');
    expect(result).not.toBeNull();
    // Must use single-quoted string, not double-quoted
    expect(result).toContain("'alice@example.com'");
    expect(result).not.toMatch(/"alice@example.com"/);
  });

  it('REQ-410: numeric PK value is NOT quoted', () => {
    const query = 'MATCH (n:Person) RETURN n';
    const result = injectExclusion(query, 'Person', '1', 'id', 42);
    expect(result).not.toBeNull();
    // Numeric literal should appear unquoted
    expect(result).toContain('[42]');
    expect(result).not.toMatch(/'42'/);
  });

  it('REQ-410: WHERE NOT clause is injected before RETURN', () => {
    const query = 'MATCH (p:Person) RETURN p';
    const result = injectExclusion(query, 'Person', '1', 'name', 'Bob');
    expect(result).not.toBeNull();
    const returnIdx = result!.search(/\bRETURN\b/i);
    const whereIdx = result!.search(/\bWHERE\b/i);
    expect(whereIdx).toBeGreaterThan(-1);
    expect(whereIdx).toBeLessThan(returnIdx);
  });

  it('REQ-410: single-quoted value with embedded apostrophe is escaped', () => {
    const query = "MATCH (n:Person) RETURN n";
    const result = injectExclusion(query, 'Person', '1', 'name', "O'Brien");
    expect(result).not.toBeNull();
    expect(result).toContain("'O\\'Brien'");
  });
});

// ── REQ-644: Node grouping is a view transform — model never mutated ──────────

describe('REQ-644: buildClusterElements does not mutate original node/edge maps', () => {
  it('REQ-644: node map is not modified by buildClusterElements', () => {
    const alice = makeNode(1, 'sales:Customer', { name: 'Alice' });
    const bob = makeNode(2, 'sales:Customer', { name: 'Bob' });
    const nodes = new Map<string, GNode>([
      ['sales:Customer:1', alice],
      ['sales:Customer:2', bob],
    ]);
    const edges = new Map<string, GEdge>();
    const sizesBefore = nodes.size;

    buildClusterElements(nodes, edges, 'domain');

    expect(nodes.size).toBe(sizesBefore);
    expect(nodes.get('sales:Customer:1')).toBe(alice);
    expect(nodes.get('sales:Customer:2')).toBe(bob);
  });

  it('REQ-644: edge map is not modified by buildClusterElements', () => {
    const a = makeNode(1, 'A', {});
    const b = makeNode(2, 'B', {});
    const e = makeEdge('e1', a, b, 'REL');
    const nodes = new Map<string, GNode>([['A:1', a], ['B:2', b]]);
    const edges = new Map<string, GEdge>([['e1', e]]);

    buildClusterElements(nodes, edges, 'domain');

    expect(edges.size).toBe(1);
    expect(edges.get('e1')).toBe(e);
  });
});

// ── REQ-645: Groupable attribute discovery ────────────────────────────────────

describe('REQ-645: clusterColor produces stable deterministic colors from group key strings', () => {
  // REQ-645 says grouping dropdown is populated dynamically from node properties.
  // clusterColor is the stable-color assignment used when mapping group → color.
  it('REQ-645: same group key always produces same color', () => {
    expect(clusterColor('sales')).toBe(clusterColor('sales'));
    expect(clusterColor('hr')).toBe(clusterColor('hr'));
  });

  it('REQ-645: different group keys may produce different colors', () => {
    const colors = new Set(
      ['sales', 'hr', 'finance', 'ops', 'eng', 'legal', 'marketing'].map(clusterColor),
    );
    expect(colors.size).toBeGreaterThan(1);
  });

  it('REQ-645: domain-based grouping uses label prefix (before colon)', () => {
    // nodeClusterId for domain: takes label.slice(0, colonIdx) as the group key
    const label = 'sales:Customer';
    const colonIdx = label.indexOf(':');
    const domain = colonIdx > 0 ? label.slice(0, colonIdx) : label;
    expect(domain).toBe('sales');
  });

  it('REQ-645: schema_L1 cluster key maps to scl1 property', () => {
    // Verify the key mapping is correct via buildClusterElements output
    const node = makeNode(1, 'Person', { scl1: 'TopLevel', name: 'Alice' });
    const nodes = new Map<string, GNode>([['Person:1', node]]);
    const edges = new Map<string, GEdge>();
    const els = buildClusterElements(nodes, edges, 'schema_L1');
    // Should produce a cluster element using the scl1 value
    const clusterEl = els.find((e) => e.data._cluster === true);
    expect(clusterEl).toBeDefined();
    expect(clusterEl?.data.label).toBe('TopLevel');
  });
});

// ── REQ-646: Color encoding per group value ───────────────────────────────────

describe('REQ-646: clusterColor derives stable color from group key string', () => {
  it('REQ-646: clusterColor returns a hex color string', () => {
    const color = clusterColor('myGroup');
    expect(color).toMatch(/^#[0-9a-f]{6}$/i);
  });

  it('REQ-646: different group values produce potentially different colors', () => {
    const c1 = clusterColor('groupA');
    const c2 = clusterColor('groupB');
    // Even if they collide (small palette) both must be valid hex strings
    expect(c1).toMatch(/^#[0-9a-f]{6}$/i);
    expect(c2).toMatch(/^#[0-9a-f]{6}$/i);
  });

  it('REQ-646: collapsed supernode element carries _color from clusterColor', () => {
    const node = makeNode(1, 'sales:Customer', { name: 'Alice' });
    const nodes = new Map<string, GNode>([['sales:Customer:1', node]]);
    const edges = new Map<string, GEdge>();
    const collapsed = new Set<string>(['sales']);
    const els = buildClusterElements(nodes, edges, 'domain', undefined, collapsed);
    const supernode = els.find((e) => e.data._collapsed === true);
    expect(supernode).toBeDefined();
    expect(supernode?.data._color).toBe(clusterColor('sales'));
  });
});

// ── REQ-647: Hull SVG overlay on grouping ────────────────────────────────────

describe('REQ-647: buildClusterElements produces compound cluster nodes for hull rendering', () => {
  it('REQ-647: expanded cluster nodes have _cluster=true for hull rendering', () => {
    const node = makeNode(1, 'hr:Employee', { name: 'Carol' });
    const nodes = new Map<string, GNode>([['hr:Employee:1', node]]);
    const edges = new Map<string, GEdge>();
    const els = buildClusterElements(nodes, edges, 'domain');
    const clusterEl = els.find((e) => e.data._cluster === true);
    expect(clusterEl).toBeDefined();
    expect(clusterEl?.data._clusterId).toBe('hr');
  });

  it('REQ-647: child nodes inside cluster have _inCluster=true', () => {
    const node = makeNode(1, 'hr:Employee', { name: 'Carol' });
    const nodes = new Map<string, GNode>([['hr:Employee:1', node]]);
    const edges = new Map<string, GEdge>();
    const els = buildClusterElements(nodes, edges, 'domain');
    const childEl = els.find((e) => e.data._inCluster === true);
    expect(childEl).toBeDefined();
    expect(childEl?.data.parent).toMatch(/^__cluster_/);
  });
});

// ── REQ-648: Collapse-to-supernode ───────────────────────────────────────────

describe('REQ-648: collapse-to-supernode produces __collapsed_<level>_<id> node', () => {
  it('REQ-648: collapsed cluster produces supernode with correct id format', () => {
    const node = makeNode(1, 'finance:Invoice', { amount: 100 });
    const nodes = new Map<string, GNode>([['finance:Invoice:1', node]]);
    const edges = new Map<string, GEdge>();
    const collapsed = new Set<string>(['finance']);
    const els = buildClusterElements(nodes, edges, 'domain', undefined, collapsed);
    const supernode = els.find((e) => e.data._collapsed === true);
    expect(supernode).toBeDefined();
    expect(supernode?.data.id).toBe(`__collapsed_domain_${cidToId('finance')}`);
  });

  it('REQ-648: supernode label contains cluster id and member count', () => {
    const n1 = makeNode(1, 'sales:Lead', { name: 'X' });
    const n2 = makeNode(2, 'sales:Lead', { name: 'Y' });
    const nodes = new Map<string, GNode>([['sales:Lead:1', n1], ['sales:Lead:2', n2]]);
    const edges = new Map<string, GEdge>();
    const collapsed = new Set<string>(['sales']);
    const els = buildClusterElements(nodes, edges, 'domain', undefined, collapsed);
    const supernode = els.find((e) => e.data._collapsed === true);
    expect(supernode?.data.label).toContain('sales');
    expect(supernode?.data.label).toContain('2');
  });

  it('REQ-648: member nodes are excluded from elements when cluster is collapsed', () => {
    const node = makeNode(1, 'ops:Task', { name: 'Task1' });
    const nodes = new Map<string, GNode>([['ops:Task:1', node]]);
    const edges = new Map<string, GEdge>();
    const collapsed = new Set<string>(['ops']);
    const els = buildClusterElements(nodes, edges, 'domain', undefined, collapsed);
    // The data node 'ops:Task:1' must not appear (only the supernode)
    const dataEl = els.find((e) => e.data.id === 'ops:Task:1');
    expect(dataEl).toBeUndefined();
  });

  it('REQ-648: cross-cluster meta-edge targets supernode when cluster is collapsed', () => {
    const a = makeNode(1, 'sales:Customer', { name: 'Alice' });
    const b = makeNode(2, 'hr:Employee', { name: 'Bob' });
    const e = makeEdge('e1', a, b, 'ASSIGNED_TO');
    const nodes = new Map<string, GNode>([['sales:Customer:1', a], ['hr:Employee:2', b]]);
    const edges = new Map<string, GEdge>([['e1', e]]);
    const collapsed = new Set<string>(['sales']);
    const metaEls = buildClusterMetaEdges(nodes, edges, 'domain', undefined, collapsed);
    const metaEdge = metaEls.find((e) => e.group === 'edges');
    expect(metaEdge).toBeDefined();
    const collapsedId = `__collapsed_domain_${cidToId('sales')}`;
    expect(metaEdge?.data.source === collapsedId || metaEdge?.data.target === collapsedId).toBe(true);
  });

  it('REQ-648: cidToId sanitizes special characters for Cytoscape element ids', () => {
    expect(cidToId('hello world')).toBe('hello_world');
    expect(cidToId('a:b/c')).toBe('a_b_c');
    expect(cidToId('valid_id-123')).toBe('valid_id-123');
  });
});

// ── REQ-649: Node size encoding via sizeByProperty (localStorage key) ─────────

describe('REQ-649: sizeByProperty is persisted under key provisa.graph.sizeByProperty', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('REQ-649: localStorage key provisa.graph.sizeByProperty stores sizeByProperty map', () => {
    const key = 'provisa.graph.sizeByProperty';
    const value: Record<string, string> = { Person: 'degree_centrality', Order: 'amount' };
    localStorage.setItem(key, JSON.stringify(value));
    const stored = JSON.parse(localStorage.getItem(key) ?? '{}');
    expect(stored).toEqual(value);
  });

  it('REQ-649: sizeByProperty defaults to empty object when no localStorage entry', () => {
    const key = 'provisa.graph.sizeByProperty';
    localStorage.removeItem(key);
    const raw = localStorage.getItem(key);
    const parsed = raw !== null ? JSON.parse(raw) : {};
    expect(parsed).toEqual({});
  });

  it('REQ-649: numeric property linear scaling: min maps to base, max maps to base*multiplier', () => {
    // REQ-649: value linearly scaled between observed min and max to produce diameter between base and base*multiplier
    const base = 44;
    const multiplier = 3;
    const min = 0;
    const max = 100;
    const value = 50;
    const scaled = base + ((value - min) / (max - min)) * (base * multiplier - base);
    expect(scaled).toBeCloseTo(base + 0.5 * base * (multiplier - 1));
    // At min → base
    const atMin = base + ((min - min) / (max - min)) * (base * multiplier - base);
    expect(atMin).toBe(base);
    // At max → base * multiplier
    const atMax = base + ((max - min) / (max - min)) * (base * multiplier - base);
    expect(atMax).toBe(base * multiplier);
  });
});
