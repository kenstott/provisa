import { tabel } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export const rows = tabel(function rows(): Row[] {
  return [];
});
