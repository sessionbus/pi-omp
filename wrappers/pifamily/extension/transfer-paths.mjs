// SPDX-License-Identifier: MIT
import path from "node:path";

// Match Codex's local path binding without normalizing symlinks or "..".
// This runs at the native tool boundary for both interactive and lane calls.
export function bindTransferPaths(action, args, cwd) {
  const keys = action === "transfer.offer" ? ["source"]
    : action === "transfer.request" ? ["destination"]
    : action === "transfer.accept" ? ["source", "destination"] : [];
  if (keys.length === 0 || args === null || typeof args !== "object" || Array.isArray(args)) return args;
  for (const key of Object.keys(args)) {
    // The private Go caller also folds long-s when decoding path fields.
    const folded = key.replaceAll("ſ", "s").toLowerCase();
    if (keys.some((name) => name !== key && name === folded)) {
      throw new Error(`json: unknown field ${JSON.stringify(key)}`);
    }
  }
  let bound = args;
  for (const key of keys) {
    const value = args[key];
    if (typeof value !== "string" || value === "" || path.isAbsolute(value)) continue;
    if (typeof cwd !== "string" || !path.isAbsolute(cwd)) {
      throw new Error(`transfer ${key} must be an absolute path: the thread's working directory is unknown`);
    }
    if (bound === args) bound = { ...args };
    bound[key] = (cwd.endsWith(path.sep) ? cwd.slice(0, -1) : cwd) + path.sep + value;
  }
  return bound;
}
