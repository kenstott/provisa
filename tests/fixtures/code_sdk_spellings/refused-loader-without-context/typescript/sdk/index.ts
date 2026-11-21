import { table } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

export const rows = table(
  function rows(inputs: Inputs): Row[] {
    return [];
  },
  { loader: true },
);
