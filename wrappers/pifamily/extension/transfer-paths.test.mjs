// SPDX-License-Identifier: MIT
import assert from "node:assert/strict";
import test from "node:test";
import { bindTransferPaths } from "./transfer-paths.mjs";

// Inputs/expected outputs ported unchanged from Codex transfer_paths_test.go.
// The native boundary uses objects; the invalid JSON row passes a non-object
// unchanged, as the Go binder leaves invalid JSON for the SDK to reject.
const cases = [
  {
    "name": "relative source",
    "action": "transfer.offer",
    "args": "{\"target\":\"t\",\"source\":\"docs/a.txt\"}",
    "cwd": "/work",
    "want": "{\"source\":\"/work/docs/a.txt\",\"target\":\"t\"}"
  },
  {
    "name": "absolute source kept",
    "action": "transfer.offer",
    "args": "{\"target\":\"t\",\"source\":\"/etc/a.txt\"}",
    "cwd": "/work",
    "want": "{\"target\":\"t\",\"source\":\"/etc/a.txt\"}"
  },
  {
    "name": "relative destination",
    "action": "transfer.request",
    "args": "{\"target\":\"t\",\"what\":\"w\",\"destination\":\"out.bin\",\"overwrite\":true}",
    "cwd": "/work/",
    "want": "{\"destination\":\"/work/out.bin\",\"overwrite\":true,\"target\":\"t\",\"what\":\"w\"}"
  },
  {
    "name": "dot-dot kept for the filesystem",
    "action": "transfer.accept",
    "args": "{\"id\":\"x\",\"destination\":\"../up.txt\"}",
    "cwd": "/work/sub",
    "want": "{\"destination\":\"/work/sub/../up.txt\",\"id\":\"x\"}"
  },
  {
    "name": "accept source",
    "action": "transfer.accept",
    "args": "{\"id\":\"x\",\"source\":\"a.txt\"}",
    "cwd": "/w",
    "want": "{\"id\":\"x\",\"source\":\"/w/a.txt\"}"
  },
  {
    "name": "root cwd",
    "action": "transfer.offer",
    "args": "{\"target\":\"t\",\"source\":\"a\"}",
    "cwd": "/",
    "want": "{\"source\":\"/a\",\"target\":\"t\"}"
  },
  {
    "name": "other action untouched",
    "action": "send",
    "args": "{\"target\":\"t\",\"message\":\"source\"}",
    "cwd": "",
    "want": "{\"target\":\"t\",\"message\":\"source\"}"
  },
  {
    "name": "status untouched",
    "action": "transfer.status",
    "args": "{\"id\":\"x\"}",
    "cwd": "",
    "want": "{\"id\":\"x\"}"
  },
  {
    "name": "empty path left to the SDK",
    "action": "transfer.offer",
    "args": "{\"target\":\"t\",\"source\":\"\"}",
    "cwd": "",
    "want": "{\"target\":\"t\",\"source\":\"\"}"
  },
  {
    "name": "non-string left to the SDK",
    "action": "transfer.offer",
    "args": "{\"target\":\"t\",\"source\":7}",
    "cwd": "",
    "want": "{\"target\":\"t\",\"source\":7}"
  },
  {
    "name": "invalid JSON left to the SDK",
    "action": "transfer.offer",
    "args": "{",
    "cwd": "",
    "want": "{"
  }
];
test("shared path binder preserves the shipped Codex cases and inputs", () => {
  for (const row of cases) {
    let args, want;
    try { args = JSON.parse(row.args); want = JSON.parse(row.want); }
    catch { args = row.args; want = row.want; }
    const before = structuredClone(args);
    assert.deepEqual(bindTransferPaths(row.action, args, row.cwd), want, row.name);
    assert.deepEqual(args, before, row.name + " mutated its input");
  }
});

test("relative local paths require a known absolute native base", () => {
  for (const cwd of ["", "relative/dir"]) {
    assert.throws(() => bindTransferPaths("transfer.offer", { target: "t", source: "a.txt" }, cwd), /absolute path/);
  }
  assert.deepEqual(bindTransferPaths("transfer.offer", { target: "t", source: "/a.txt" }, ""), { target: "t", source: "/a.txt" });
});

test("path aliases cannot bypass binding through the Go caller", () => {
  for (const [action, args] of [
    ["transfer.offer", { target: "t", Source: "a.txt" }],
    ["transfer.offer", { target: "t", ſource: "a.txt" }],
    ["transfer.offer", { target: "t", source: "/abs/a.txt", SOURCE: "a.txt" }],
    ["transfer.request", { target: "t", what: "w", Destination: "a.txt", overwrite: true }],
    ["transfer.accept", { id: "x", Source: "a.txt" }],
    ["transfer.accept", { id: "x", destinatioN: "a.txt" }],
  ]) {
    for (const cwd of ["/thread/work", ""]) assert.throws(() => bindTransferPaths(action, args, cwd), /unknown field/);
  }
});
