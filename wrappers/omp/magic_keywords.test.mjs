// SPDX-License-Identifier: MIT

// OMP's own magic-keyword matcher, vendored byte-exact from @oh-my-pi/pi-tui
// 18.18.5 (testdata/pi-tui-18.18.5/UPSTREAM.json), run on the peer envelopes
// the Go wrapper renders (testdata/magic-keyword-envelopes.json, kept current
// by TestOMPMagicKeywordGoldenIsCurrent). Rerun at every OMP update after
// re-vendoring; a matcher drift fails here.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { registerHooks } from "node:module";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const vendor = path.join(here, "testdata", "pi-tui-18.18.5");
const upstream = JSON.parse(readFileSync(path.join(vendor, "UPSTREAM.json"), "utf8"));
const cases = JSON.parse(readFileSync(path.join(here, "testdata", "magic-keyword-envelopes.json"), "utf8"));

// The vendored sources use extensionless relative imports, as Bun allows.
registerHooks({
  resolve(specifier, context, next) {
    if (context.parentURL?.startsWith(pathToFileURL(vendor).href) && specifier.startsWith(".") && !path.extname(specifier)) {
      const candidate = path.resolve(path.dirname(fileURLToPath(context.parentURL)), `${specifier}.ts`);
      if (existsSync(candidate)) return next(pathToFileURL(candidate).href, context);
    }
    return next(specifier, context);
  },
});
const { containsMagicKeyword } = await import(pathToFileURL(path.join(vendor, "prompt", "magic-keywords.ts")).href);

test("vendored matcher files are byte-exact upstream", () => {
  for (const [file, sha256] of Object.entries(upstream.verbatim)) {
    assert.equal(createHash("sha256").update(readFileSync(path.join(vendor, file))).digest("hex"), sha256, file);
  }
});

test("the golden covers every upstream keyword", () => {
  assert.deepEqual([...new Set(cases.map((entry) => entry.word))], upstream.words);
});

test("no magic keyword in a rendered peer body reaches OMP's matcher", () => {
  for (const entry of cases) {
    assert.equal(containsMagicKeyword(entry.ours, entry.word), false, `${entry.word}: ${entry.case}`);
  }
});

test("the same bodies through the plain renderer show the bypasses this closes", () => {
  for (const entry of cases) {
    assert.equal(containsMagicKeyword(entry.base, entry.word), entry.bypass, `${entry.word}: ${entry.case}`);
  }
  for (const word of upstream.words) {
    assert.equal(containsMagicKeyword(`please ${word} now`, word), true, `${word} outside an envelope`);
  }
});
