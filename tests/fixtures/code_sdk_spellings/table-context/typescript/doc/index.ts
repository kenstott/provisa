import type { ReadContext } from "@provisa/services";

export interface Row {
  id: number;
  name: string;
}

/** Rows the read asks for.
 * @table
 */
export async function rows(context: ReadContext): Promise<Row[]> {
  return [];
}
