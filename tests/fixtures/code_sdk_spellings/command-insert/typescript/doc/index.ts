export interface Row {
  id: number;
  name: string;
}

/** The insert of rows.
 * @command
 * @insert rows
 */
export async function insertRows(rows: Row[]): Promise<number> {
  return 0;
}
