// Copyright (c) 2026 Kenneth Stott
// Canary: 4c3cc30e-f6af-4db7-9cba-fe7c96724ddc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** Remove a source a test registered, and what the test registered against it.
 *
 * A source is deleted only when nothing refers to it (REQ-1918): `deleteSource` is refused
 * while a table is registered against it, and `deleteTable` while a relationship takes part in
 * the table. A test's cleanup therefore removes them in that order — the relationships of the
 * source's tables, the tables, then the source — each through its own mutation, as an operator
 * would.
 *
 * `gql` posts one admin GraphQL document and resolves to the parsed JSON body. A source that
 * is not there is left alone (nothing to remove). Throws when the source is still refused after
 * its tables are gone, naming what still refers to it, so a cleanup that removed nothing is not
 * mistaken for one that did.
 */
export type AdminGql = (
  query: string,
  variables?: Record<string, unknown>,
) => Promise<{ data?: Record<string, unknown> | null; errors?: unknown }>;

export async function deleteSourceAndItsTables(gql: AdminGql, sourceId: string): Promise<void> {
  const listed = await gql(
    `{ tables { id sourceId } relationships { id sourceTableId targetTableId } }`,
  );
  const tables = ((listed.data?.tables ?? []) as Array<{ id: number; sourceId: string }>).filter(
    (t) => t.sourceId === sourceId,
  );
  const tableIds = new Set(tables.map((t) => t.id));
  const relationships = (
    (listed.data?.relationships ?? []) as Array<{
      id: string;
      sourceTableId: number;
      targetTableId: number | null;
    }>
  ).filter(
    (r) => tableIds.has(r.sourceTableId) || (r.targetTableId !== null && tableIds.has(r.targetTableId)),
  );
  for (const r of relationships) {
    await gql(`mutation D($id: String!) { deleteRelationship(id: $id) { success } }`, { id: r.id });
  }
  for (const t of tables) {
    await gql(`mutation D($id: Int!) { deleteTable(id: $id) { success } }`, { id: t.id });
  }
  const res = await gql(
    `mutation D($id: String!) { deleteSource(id: $id) { success code message } }`,
    { id: sourceId },
  );
  const outcome = res.data?.deleteSource as
    | { success: boolean; code: string | null; message: string }
    | undefined;
  if (outcome && !outcome.success && outcome.code !== "schema.source_not_found") {
    throw new Error(`source ${sourceId} was not removed: ${outcome.message}`);
  }
}
