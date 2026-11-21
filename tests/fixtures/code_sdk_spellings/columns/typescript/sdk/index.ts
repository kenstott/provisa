import { columns, type Batch } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export interface Upper {
  upper: string;
}

/** Each name in capitals. */
export const shout = columns(function shout(names: string[]): Batch<Upper> {
  return names.map((n) => ({ upper: n.toUpperCase() }));
});
