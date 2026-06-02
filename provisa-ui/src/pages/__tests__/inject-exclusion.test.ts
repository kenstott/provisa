// Copyright (c) 2026 Kenneth Stott
// Canary: f1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from 'vitest';
import { injectExclusion } from '../../components/graph/graph-model';

describe('injectExclusion', () => {
  describe('WHERE placement before RETURN', () => {
    it('places WHERE NOT before RETURN so all subsequent OPTIONAL MATCHes can reference the variable', () => {
      const query = [
        'MATCH (a:Pets)',
        'OPTIONAL MATCH (a:Pets)-[:HAS_TABLE]->(b:RegisteredTables)',
        'OPTIONAL MATCH (b:RegisteredTables)-[:HAS_QUERIES]->(c:Queries)',
        'RETURN a, b, c',
      ].join('\n');

      const result = injectExclusion(query, 'RegisteredTables', '38761', null, null);
      expect(result).not.toBeNull();
      const lines = result!.split('\n');
      const returnIdx = lines.findIndex((l) => /^\s*RETURN\b/i.test(l));
      const whereIdx = lines.findIndex((l) => /WHERE\s+NOT/i.test(l));

      // WHERE must appear before RETURN
      expect(whereIdx).toBeGreaterThan(-1);
      expect(whereIdx).toBeLessThan(returnIdx);
    });

    it('preserves all OPTIONAL MATCH clauses — WHERE does not split them', () => {
      const query = [
        'MATCH (a:Pets)',
        'OPTIONAL MATCH (a:Pets)-[:HAS_TABLE]->(b:RegisteredTables)',
        'OPTIONAL MATCH (b:RegisteredTables)-[:HAS_QUERIES]->(c:Queries)',
        'RETURN a, b, c',
      ].join('\n');

      const result = injectExclusion(query, 'RegisteredTables', '38761', null, null);
      expect(result).not.toBeNull();
      // Both OPTIONAL MATCHes must still be present
      expect(result).toContain('HAS_TABLE');
      expect(result).toContain('HAS_QUERIES');
    });

    it('subsequent exclusions on the same variable extend the IN list in place', () => {
      const query = [
        'MATCH (a:Pets)',
        'OPTIONAL MATCH (a:Pets)-[:HAS_TABLE]->(b:RegisteredTables)',
        'WHERE NOT id(b) IN [38761]',
        'RETURN a, b',
      ].join('\n');

      const result = injectExclusion(query, 'RegisteredTables', '99999', null, null);
      expect(result).not.toBeNull();
      expect(result).toContain('38761');
      expect(result).toContain('99999');
      // Should not duplicate the WHERE clause
      const whereCount = (result!.match(/\bWHERE\b/gi) ?? []).length;
      expect(whereCount).toBe(1);
    });
  });

  describe('fallback when no OPTIONAL MATCH contains the variable', () => {
    it('injects before RETURN when variable is not in any OPTIONAL MATCH', () => {
      const query = 'MATCH (n:Person) RETURN n.name';
      const result = injectExclusion(query, 'Person', '42', null, null);
      expect(result).not.toBeNull();
      expect(result).toContain('WHERE NOT');
      expect(result!.indexOf('WHERE')).toBeLessThan(result!.indexOf('RETURN'));
    });
  });
});
