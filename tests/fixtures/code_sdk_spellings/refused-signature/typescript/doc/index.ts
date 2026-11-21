import type { ReadContext } from "@provisa/services";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** @table */
export function rows(context: ReadContext, inputs: Inputs): Row[] {
  return [];
}
