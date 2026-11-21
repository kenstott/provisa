import { table, type ReadContext } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Rows of every key in one batch. */
export const rows = table(
  async function rows(inputs: Inputs, context: ReadContext): Promise<Row[]> {
    return [];
  },
  { loader: true },
);
