import { table, type AsyncRows, type ReadContext } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of a region, as the read asks. */
export const rows = table(async function* rows(inputs: Inputs, context: ReadContext): AsyncRows<Row> {});
