import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";

/**
 * One-shot verification entrypoint (the `verify` compose service).
 *
 * Runs after the API is healthy and aggregates all results into a single exit
 * code (0 = every stage passed, 1 = one or more stages failed):
 *   1. code tests (node:test unit + HTTP integration suites)
 *   2. TypeScript build (strict tsc type-check)
 *   3. submit/query smoke against the live API, including:
 *      - first submission -> 201, identical retry -> 200 with the same result
 *      - GET returns the stored document
 *      - cross-batch alias stability and per-category isolation
 *      - 409 on conflicting content, 422 on an invalid whole batch
 *      - no raw identifier value ever appears in a response
 */

const API_BASE_URL = process.env.API_BASE_URL ?? "http://api:8080";
const APP_DIR = process.env.APP_DIR ?? "/app";

function runCommand(name: string, command: string, args: string[]): boolean {
  process.stdout.write(`\n=== verify: ${name} ===\n`);
  const result = spawnSync(command, args, { cwd: APP_DIR, stdio: "inherit" });
  if (result.error) {
    process.stderr.write(`--- verify: ${name} FAILED: ${String(result.error.message)}\n`);
    return false;
  }
  if (result.status !== 0) {
    process.stderr.write(`--- verify: ${name} FAILED: ${command} exited with ${result.status}\n`);
    return false;
  }
  process.stdout.write(`--- verify: ${name} OK\n`);
  return true;
}

async function waitForHealthy(timeoutMs = 60_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError = "not started";
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${API_BASE_URL}/healthz`);
      if (res.ok) return;
      lastError = `status ${res.status}`;
    } catch (err) {
      lastError = (err as Error).message;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`API did not become healthy in time (${lastError})`);
}

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

async function postJson(path: string, body: unknown): Promise<{ status: number; json: any }> {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: res.status, json: await res.json() };
}

async function getJson(path: string): Promise<{ status: number; json: any }> {
  const res = await fetch(`${API_BASE_URL}${path}`);
  return { status: res.status, json: await res.json() };
}

const RAW_VALUES = ["SMOKE-R1", "SMOKE-R2", "SMOKE-R3", "SMOKE-R9", "SMOKE-P1", "SMOKE-P2", "SMOKE-P9", "SMOKE-A1", "SMOKE-A2", "SMOKE-A9", "DUPVAL", "SMOKE-X", "SMOKE-PX", "SMOKE-PY", "SMOKE-AX", "SMOKE-AY", "GHOST"];

function assertNoRawLeak(label: string, value: unknown): void {
  const text = JSON.stringify(value);
  for (const raw of RAW_VALUES) {
    assert(!text.includes(raw), `${label} leaks raw identifier ${raw}`);
  }
}

const batchOne = {
  batchId: "smoke-batch-1",
  records: [
    {
      recordId: "SMOKE-R1",
      patientId: "SMOKE-P1",
      accessionId: "SMOKE-A1",
      relatedIds: ["SMOKE-R2"],
      measurements: { tumorSizeMm: 11.5, marker: "her2", positive: true, residual: null },
    },
    {
      recordId: "SMOKE-R2",
      patientId: "SMOKE-P2",
      accessionId: "SMOKE-A2",
      relatedIds: [],
      measurements: { count: 3 },
    },
  ],
};

async function runSmoke(): Promise<boolean> {
  process.stdout.write("\n=== verify: submit/query smoke ===\n");
  try {
    await waitForHealthy();
    process.stdout.write("verify: API healthy\n");

    // 1. First submission -> 201 alias-only closed copy.
    const created = await postJson("/api/manifests", batchOne);
    assert(created.status === 201, `expected 201, got ${created.status}`);
    assertNoRawLeak("create response", created.json);
    const createdText = JSON.stringify(created.json);
    for (const prefix of ["rec-", "pat-", "acc-"]) {
      assert(createdText.includes(prefix), `create response missing ${prefix} aliases`);
    }
    assert(
      JSON.stringify(created.json.records[0].measurements) ===
        JSON.stringify({ tumorSizeMm: 11.5, marker: "her2", positive: true, residual: null }),
      "measurements must be preserved verbatim",
    );
    const aliases = new Set(created.json.records.map((r: any) => r.recordAlias));
    assert(
      created.json.records[0].relatedAliases.length === 1 &&
        aliases.has(created.json.records[0].relatedAliases[0]),
      "cross references must resolve to record aliases",
    );

    // 2. Identical retry -> 200 with the ORIGINAL result.
    const retry = await postJson("/api/manifests", batchOne);
    assert(retry.status === 200, `expected 200 on retry, got ${retry.status}`);
    assert(
      JSON.stringify(retry.json) === JSON.stringify(created.json),
      "retry must return the original stored result",
    );

    // 3. GET returns the same document.
    const fetched = await getJson("/api/manifests/smoke-batch-1");
    assert(fetched.status === 200, `expected 200 on GET, got ${fetched.status}`);
    assert(
      JSON.stringify(fetched.json) === JSON.stringify(created.json),
      "GET must return the stored manifest",
    );

    // 4. Cross-batch alias stability + category isolation.
    const batchTwo = {
      batchId: "smoke-batch-2",
      records: [
        { recordId: "SMOKE-R3", patientId: "SMOKE-P1", accessionId: "SMOKE-A1", relatedIds: [], measurements: {} },
        { recordId: "DUPVAL", patientId: "DUPVAL", accessionId: "DUPVAL", relatedIds: [], measurements: {} },
      ],
    };
    const second = await postJson("/api/manifests", batchTwo);
    assert(second.status === 201, `expected 201 for second batch, got ${second.status}`);
    assertNoRawLeak("second batch response", second.json);
    assert(
      second.json.records[0].patientAlias === created.json.records[0].patientAlias,
      "same patient id across batches must map to the same alias",
    );
    assert(
      second.json.records[0].accessionAlias === created.json.records[0].accessionAlias,
      "same accession id across batches must map to the same alias",
    );
    const dup = second.json.records[1];
    assert(
      dup.recordAlias !== dup.patientAlias &&
        dup.recordAlias !== dup.accessionAlias &&
        dup.patientAlias !== dup.accessionAlias,
      "identifier classes must be isolated even for identical raw values",
    );

    // 5. Conflict: same batchId, different business content -> 409.
    const conflicting = JSON.parse(JSON.stringify(batchOne));
    conflicting.records[1].measurements.count = 4;
    const conflict = await postJson("/api/manifests", conflicting);
    assert(conflict.status === 409, `expected 409, got ${conflict.status}`);
    assertNoRawLeak("conflict response", conflict.json);

    // 6. Invalid structure (duplicate recordId) -> whole batch rejected 422.
    const invalid = {
      batchId: "smoke-batch-invalid",
      records: [
        { recordId: "SMOKE-X", patientId: "SMOKE-PX", accessionId: "SMOKE-AX", relatedIds: [], measurements: {} },
        { recordId: "SMOKE-X", patientId: "SMOKE-PY", accessionId: "SMOKE-AY", relatedIds: [], measurements: {} },
      ],
    };
    const rejected = await postJson("/api/manifests", invalid);
    assert(rejected.status === 422, `expected 422, got ${rejected.status}`);
    assertNoRawLeak("validation response", rejected.json);
    const missing = await getJson("/api/manifests/smoke-batch-invalid");
    assert(missing.status === 404, "rejected batch must not be stored");

    // 7. Dangling reference -> 422.
    const dangling = {
      batchId: "smoke-batch-dangling",
      records: [
        { recordId: "SMOKE-R9", patientId: "SMOKE-P9", accessionId: "SMOKE-A9", relatedIds: ["GHOST"], measurements: {} },
      ],
    };
    const danglingRes = await postJson("/api/manifests", dangling);
    assert(danglingRes.status === 422, `expected 422 for dangling reference, got ${danglingRes.status}`);
    assertNoRawLeak("dangling response", danglingRes.json);

    process.stdout.write("--- verify: submit/query smoke OK\n");
    return true;
  } catch (err) {
    process.stderr.write(`--- verify: submit/query smoke FAILED: ${(err as Error)?.message ?? String(err)}\n`);
    return false;
  }
}

async function main(): Promise<void> {
  // Isolated data dir for the in-process servers spawned by the test suites.
  if (!process.env.DATA_DIR) {
    const dir = "/tmp/manifest-verify-data";
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    process.env.DATA_DIR = dir;
  }

  const results: boolean[] = [
    runCommand("code tests", process.execPath, [
      "--experimental-strip-types",
      "--test",
      "test/manifest.test.ts",
      "test/api.test.ts",
    ]),
    runCommand("typescript build", process.execPath, [
      "node_modules/typescript/bin/tsc",
      "-p",
      "tsconfig.json",
    ]),
    await runSmoke(),
  ];

  const failed = results.filter((ok) => !ok).length;
  process.stdout.write(`\nverify summary: ${results.length - failed}/${results.length} stages passed\n`);
  process.exit(failed === 0 ? 0 : 1);
}

void main();
