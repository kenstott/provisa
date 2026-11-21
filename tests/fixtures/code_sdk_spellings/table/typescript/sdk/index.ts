import { table } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

/** Every row. */
export const rows = table(function rows(): Row[] {
  return [];
});
