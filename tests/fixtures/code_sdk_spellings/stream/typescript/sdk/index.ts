import { stream } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}
export interface Inputs {
  region?: string;
}

/** Each row as it arrives. */
export const ticks = stream(async function* ticks(inputs: Inputs): AsyncGenerator<Row> {});
