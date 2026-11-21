import type { ReadContext } from "@provisa/services";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of every key in one batch.
 * @table
 * @loader
 */
export async function rows(inputs: Inputs, context: ReadContext): Promise<Row[]> {
  return [];
}
