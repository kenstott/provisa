import { table } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export const rows = table(function rows(): Record<string, unknown>[] {
  return [];
});
