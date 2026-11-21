import { table, type ReadContext } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

export const rows = table(function rows(context: ReadContext, inputs: Inputs): Row[] {
  return [];
});
