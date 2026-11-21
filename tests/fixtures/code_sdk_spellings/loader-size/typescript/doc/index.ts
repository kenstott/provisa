import type { ReadContext } from "@provisa/services";

export interface Row {
  id: number;
  name: string;
}

/** Rows of the keys, 100 at a time.
 * @table
 * @loader 100
 */
export async function rows(context: ReadContext): Promise<Row[]> {
  return [];
}
