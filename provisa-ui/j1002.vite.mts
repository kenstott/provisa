import base from "./vite.config";
import { mergeConfig } from "vite";
const b = typeof base === "function" ? (base as never as (e: object) => object)({ command: "serve", mode: "development" }) : base;
export default mergeConfig(b as object, { cacheDir: process.env.HOME + "/provisa-agent-logs/J1002-vite-cache", server: { port: 5972, strictPort: true, host: "127.0.0.1", proxy: {} } });
