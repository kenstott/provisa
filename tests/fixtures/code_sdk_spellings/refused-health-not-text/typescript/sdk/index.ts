import { health } from "@provisa/sdk";

export interface Row {
  id: number;
  name: string;
}

export const status = health(function status(): number {
  return 1;
});
