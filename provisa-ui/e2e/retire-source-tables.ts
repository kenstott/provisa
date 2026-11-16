// Copyright (c) 2026 Kenneth Stott
// Canary: 4c3cc30e-f6af-4db7-9cba-fe7c96724ddc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** A test's cleanup: retire what it registered, and leave its source in place.
 *
 * REQ-2248: nothing in the model is hard-deleted. Deleting a table or a relationship retires it,
 * and a source that still holds a table, retired or not, is kept: `deleteSource` is refused. The
 * product can restore a retired object, never remove one. So a test does not delete the source it
 * added. It retires the relationships of the source's tables and then the tables (REQ-1918: a
 * table is refused while a relationship takes part in it), each through its own mutation, as an
 * operator would, and the source stays.
 *
 * A retired table keeps its name and its (source, schema, table) key, so a test that registers the
 * same table again on the same source is refused. Every source a test adds is therefore named for
 * its attempt (`testSourceId`): a retry, or a later run against the same backend, never meets a
 * name an earlier attempt still holds.
 *
 * `gql` posts one admin GraphQL document and resolves to the parsed JSON body.
 */
export type AdminGql = (
  query: string,
  variables?: Record<string, unknown>,
) => Promise<{ data?: Record<string, unknown> | null; errors?: unknown }>;

type Outcome = { success: boolean; code: string | null; message: string } | undefined;

/** A source id of this attempt's own: `e2e_<kind>_<13-digit stamp>`, the shape
 * `retireTestSourcesTables` (source-to-query-helpers.ts) tells a test's source by. */
export function testSourceId(kind: string): string {
  return `e2e_${kind}_${Date.now()}`;
}

/** Throw unless the mutation `field` in `res` succeeded, or answered `gone` (nothing to do). */
function requireDone(
  res: Awaited<ReturnType<AdminGql>>,
  field: string,
  gone: string,
  what: string,
): void {
  if (res.errors) throw new Error(`${what}: ${JSON.stringify(res.errors)}`);
  const outcome = res.data?.[field] as Outcome;
  if (!outcome) throw new Error(`${what}: no ${field} in ${JSON.stringify(res)}`);
  if (!outcome.success && outcome.code !== gone) throw new Error(`${what}: ${outcome.message}`);
}

/** Retire every table in service on `sourceId`, and the relationships they take part in. The
 * source itself is left in place. A source with no table in service is left as it is. Throws on
 * any refusal, naming it, so a cleanup that retired nothing is not mistaken for one that did. */
export async function retireSourceTables(gql: AdminGql, sourceId: string): Promise<void> {
  const listed = await gql(
    `{ tables { id sourceId } relationships { id sourceTableId targetTableId } }`,
  );
  if (listed.errors)
    throw new Error(`tables of ${sourceId} could not be listed: ${JSON.stringify(listed.errors)}`);
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
    (r) =>
      tableIds.has(r.sourceTableId) || (r.targetTableId !== null && tableIds.has(r.targetTableId)),
  );
  for (const r of relationships) {
    const res = await gql(
      `mutation D($id: String!) { deleteRelationship(id: $id) { success code message } }`,
      { id: r.id },
    );
    requireDone(
      res,
      "deleteRelationship",
      "schema.relationship_not_found",
      `relationship ${r.id} of ${sourceId} was not retired`,
    );
  }
  for (const t of tables) {
    const res = await gql(
      `mutation D($id: Int!) { deleteTable(id: $id) { success code message } }`,
      { id: t.id },
    );
    requireDone(
      res,
      "deleteTable",
      "schema.table_not_found",
      `table ${t.id} of ${sourceId} was not retired`,
    );
  }
}

/** Delete a source that never held a table. Only for a source that must not stay: AskAmerica,
 * of which one source is served (REQ-540), so a second is refused while the first is there.
 * A source that holds a table, retired or not, is kept (REQ-2248) and this throws, naming what
 * holds it. A source that is not there is left alone. */
export async function deleteSourceHoldingNoTable(gql: AdminGql, sourceId: string): Promise<void> {
  const res = await gql(
    `mutation D($id: String!) { deleteSource(id: $id) { success code message } }`,
    { id: sourceId },
  );
  requireDone(res, "deleteSource", "schema.source_not_found", `source ${sourceId} was not removed`);
}
