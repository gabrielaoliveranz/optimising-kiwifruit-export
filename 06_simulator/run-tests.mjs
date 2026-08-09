// =============================================================================
// APOPHENIA — headless test runner for assets/js/app.test.js
// Script: run-tests.mjs
// Stage:  Testing
// Author: Gabriela Olivera | Data Analytics Portfolio
// =============================================================================
//
// app.test.js was written against test.html's inline describe/test/expect
// shims and only ever exercises pure functions (CONFIG structure, the risk
// model, the payment engine, fetch() with a relative URL that's expected to
// fail either way) — no DOM/Canvas/WebGL API is touched anywhere in it, so
// it runs correctly under plain Node (which has already had global fetch()
// since Node 18) without needing a browser at all. Reproducing the same
// three shims here means CI can run the exact same 18 tests headlessly with
// zero new dependencies — no Playwright/Puppeteer download, since Node
// itself is already what GitHub's ubuntu-latest runners ship with.

globalThis.__suites = [];
let current = null;

globalThis.describe = (name, fn) => {
  current = { name, tests: [] };
  globalThis.__suites.push(current);
  fn();
  current = null;
};

globalThis.test = (name, fn) => {
  current.tests.push({ name, fn });
};

globalThis.expect = (actual) => ({
  toBe(expected) {
    if (actual !== expected) {
      throw new Error(
        `Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
      );
    }
  },
  toHaveProperty(key) {
    if (actual == null || !(key in Object(actual))) {
      throw new Error(
        `Expected property "${key}" — keys: [${Object.keys(actual || {}).join(", ")}]`
      );
    }
  },
  toHaveLength(n) {
    if (!actual || actual.length !== n) {
      throw new Error(`Expected length ${n} — got ${actual?.length}`);
    }
  },
});

await import("./assets/js/app.test.js");

let passed = 0;
let failed = 0;

for (const suite of globalThis.__suites) {
  console.log(`\n${suite.name}`);
  for (const t of suite.tests) {
    try {
      const result = t.fn();
      if (result && typeof result.then === "function") await result;
      console.log(`  ✓ ${t.name}`);
      passed++;
    } catch (e) {
      console.log(`  ✗ ${t.name}`);
      console.log(`    ${e.message}`);
      failed++;
    }
  }
}

const total = passed + failed;
console.log(`\n${passed}/${total} passed${failed ? ` — ${failed} failed` : ""}`);

process.exit(failed === 0 ? 0 : 1);
