import { table, type ReadContext } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Rows the read asks for. */
export const rows = table(async function rows(context: ReadContext): Promise<Row[]> {
  return [];
});
