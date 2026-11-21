export interface Row {
  id: number;
  name: string;
}

export interface Changes {
  name?: string;
}

/** The update of rows.
 * @command
 * @update rows
 */
export async function updateRows(keys: number[], changes: Changes): Promise<number> {
  return 0;
}
