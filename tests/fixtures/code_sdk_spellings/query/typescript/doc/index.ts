export interface Row {
  id: number;
  name: string;
}

/** Rows of one name.
 * @query
 */
export async function lookup(name: string): Promise<Row[]> {
  return [];
}
