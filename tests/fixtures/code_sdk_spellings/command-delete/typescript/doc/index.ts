export interface Row {
  id: number;
  name: string;
}

/** The delete of rows.
 * @command
 * @delete rows
 */
export async function deleteRows(keys: number[]): Promise<number> {
  return 0;
}
