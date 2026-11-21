import type { ReadContext } from "@provisa/services";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of a region, as the read asks.
 * @table
 */
export async function* rows(inputs: Inputs, context: ReadContext): AsyncIterable<Row> {}
