import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Aliaser } from "../src/alias.ts";
import { contentHash } from "../src/canonical.ts";
import { createAppServer } from "../src/http.ts";
import { ManifestStore } from "../src/store.ts";
import { transformBatch } from "../src/transform.ts";
import { validateBatch } from "../src/validation.ts";
import type { SharedManifest } from "../src/types.ts";

/**
 * Restore-time integrity regression suite.
 *
 * Covers data volumes that were restored from a backup, migrated from an old
 * version or logically corrupted: entries that do not satisfy the current
 * persisted contract must be isolated (quarantined), never served by GET,
 * never silently overwritten by POST, and never leak raw identifiers into
 * responses or logs — while legitimate manifests keep loading, replaying and
 * conflicting exactly as before.
 */

const SECRET = Buffer.from("0123456789abcdef0123456789abcdef", "utf8");

/** The confirmed corrupt sample: raw identifiers in alias fields, illegal
 *  contentHash and createdAt, stored under the correct batch hash file name. */
const CORRUPT_SAMPLE = {
  batchId: "restore-batch",
  createdAt: "not-a-valid-timestamp",
  contentHash: "sha256-hex",
  records: [
    {
      recordAlias: "HOSPITAL-REC-900",
      patientAlias: "PATIENT-900",
      accessionAlias: "ACCESSION-900",
      relatedAliases: ["HOSPITAL-REC-900"],
      measurements: { tumorSizeMm: 12.5 },
    },
  ],
};
const SAMPLE_RAW_IDS = ["PATIENT-900", "ACCESSION-900", "HOSPITAL-REC-900"];

function fileNameFor(batchId: string): string {
  return createHash("sha256").update(batchId, "utf8").digest("hex") + ".json";
}

/** Produce a manifest exactly the way the current pipeline persists it. */
function makeValidPersisted(batchId: string): SharedManifest {
  const batch = validateBatch({
    batchId,
    records: [
      {
        recordId: "R-1",
        patientId: "P-1",
        accessionId: "A-1",
        relatedIds: ["R-2"],
        measurements: { tumorSizeMm: 9.9, reviewed: true },
      },
      {
        recordId: "R-2",
        patientId: "P-2",
        accessionId: "A-2",
        relatedIds: [],
        measurements: { count: 1 },
      },
    ],
  });
  return transformBatch(batch, new Aliaser(SECRET), contentHash(batch));
}

async function startServer(store: ManifestStore): Promise<{ server: any; baseUrl: string }> {
  const server = createAppServer({ store, aliasSecret: SECRET, maxBodyBytes: 1_000_000 });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { server, baseUrl: `http://127.0.0.1:${server.address().port}` };
}

function stopServer(server: any): Promise<void> {
  return new Promise<void>((resolve) => server.close(() => resolve()));
}

async function postJson(baseUrl: string, body: unknown): Promise<{ status: number; json: any }> {
  const res = await fetch(`${baseUrl}/api/manifests`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: res.status, json: await res.json() };
}

/** Capture everything the process logs while fn runs (privacy assertions). */
async function captureLogs<T>(fn: () => Promise<T>): Promise<{ result: T; lines: string }> {
  const chunks: string[] = [];
  const originalOut = process.stdout.write;
  const originalErr = process.stderr.write;
  process.stdout.write = (chunk: unknown) => {
    chunks.push(String(chunk));
    return true;
  };
  process.stderr.write = (chunk: unknown) => {
    chunks.push(String(chunk));
    return true;
  };
  try {
    const result = await fn();
    return { result, lines: chunks.join("") };
  } finally {
    process.stdout.write = originalOut;
    process.stderr.write = originalErr;
  }
}

test("legitimate manifests reload after restart with replay and conflict semantics intact", async () => {
  const dir = mkdtempSync(join(tmpdir(), "manifest-restore-ok-"));
  const batchOne = {
    batchId: "restore-ok",
    records: [
      {
        recordId: "RR-1",
        patientId: "RP-1",
        accessionId: "RA-1",
        relatedIds: ["RR-2"],
        measurements: { tumorSizeMm: 3.1, reviewed: true },
      },
      {
        recordId: "RR-2",
        patientId: "RP-2",
        accessionId: "RA-2",
        relatedIds: [],
        measurements: { count: 2 },
      },
    ],
  };

  const store1 = new ManifestStore(dir);
  await store1.load();
  const first = await startServer(store1);
  const created = await postJson(first.baseUrl, batchOne);
  assert.equal(created.status, 201);
  await stopServer(first.server);

  // Restart over the same data volume.
  const store2 = new ManifestStore(dir);
  await store2.load();
  assert.ok(!store2.isQuarantined("restore-ok"));
  const second = await startServer(store2);
  try {
    const fetched = await fetch(`${second.baseUrl}/api/manifests/restore-ok`);
    assert.equal(fetched.status, 200);
    assert.deepEqual(await fetched.json(), created.json);

    // Identical retry after restart replays the original stored document.
    const replay = await postJson(second.baseUrl, batchOne);
    assert.equal(replay.status, 200);
    assert.deepEqual(replay.json, created.json);

    // Different content for the same batchId still conflicts after restart.
    const conflicting = JSON.parse(JSON.stringify(batchOne));
    conflicting.records[1].measurements.count = 99;
    const conflict = await postJson(second.baseUrl, conflicting);
    assert.equal(conflict.status, 409);
    assert.equal(conflict.json.error, "batch_conflict");
  } finally {
    await stopServer(second.server);
  }
});

test("the corrupt restored sample is quarantined: never served, never overwritten, no leaks", async () => {
  const dir = mkdtempSync(join(tmpdir(), "manifest-restore-corrupt-"));
  await writeFile(join(dir, fileNameFor("restore-batch")), JSON.stringify(CORRUPT_SAMPLE));

  const store = new ManifestStore(dir);
  const { lines } = await captureLogs(() => store.load());

  // Isolated: not retrievable, but its batchId is reserved rather than forgotten.
  assert.equal(store.get("restore-batch"), undefined);
  assert.ok(store.isQuarantined("restore-batch"));

  // The quarantine is observable in logs without ever logging the corrupt content.
  assert.ok(lines.includes("store_entry_quarantined"));
  for (const raw of SAMPLE_RAW_IDS) {
    assert.ok(!lines.includes(raw), `quarantine log must not contain ${raw}`);
  }

  // The corrupt file is moved aside for diagnosis, not deleted or left in place.
  const rootEntries = await readdir(dir);
  assert.ok(!rootEntries.includes(fileNameFor("restore-batch")));
  const quarantined = await readdir(join(dir, "quarantine"));
  assert.equal(quarantined.length, 1);

  const { server, baseUrl } = await startServer(store);
  try {
    // GET must not return the corrupt entry and must not leak raw identifiers.
    const res = await fetch(`${baseUrl}/api/manifests/restore-batch`);
    assert.notEqual(res.status, 200);
    assert.equal(res.status, 409);
    const body = await res.json();
    assert.equal(body.error, "manifest_quarantined");
    for (const raw of SAMPLE_RAW_IDS) {
      assert.ok(!JSON.stringify(body).includes(raw), `GET response must not contain ${raw}`);
    }

    // POST with the same batchId must not silently overwrite the quarantined entry.
    const overwrite = await postJson(baseUrl, {
      batchId: "restore-batch",
      records: [
        {
          recordId: "NEW-R1",
          patientId: "NEW-P1",
          accessionId: "NEW-A1",
          relatedIds: [],
          measurements: {},
        },
      ],
    });
    assert.equal(overwrite.status, 409);
    assert.equal(overwrite.json.error, "manifest_quarantined");
    for (const raw of [...SAMPLE_RAW_IDS, "NEW-R1", "NEW-P1", "NEW-A1"]) {
      assert.ok(!JSON.stringify(overwrite.json).includes(raw));
    }

    // The entry is still not served afterwards.
    assert.equal((await fetch(`${baseUrl}/api/manifests/restore-batch`)).status, 409);
  } finally {
    await stopServer(server);
  }
});

test("quarantine state survives further restarts", async () => {
  const dir = mkdtempSync(join(tmpdir(), "manifest-restore-taint-"));
  await writeFile(join(dir, fileNameFor("restore-batch")), JSON.stringify(CORRUPT_SAMPLE));

  const first = new ManifestStore(dir);
  await first.load();
  assert.ok(first.isQuarantined("restore-batch"));

  // A fresh store over the same directory rebuilds the quarantine from disk.
  const second = new ManifestStore(dir);
  await second.load();
  assert.ok(second.isQuarantined("restore-batch"));
  assert.equal(second.get("restore-batch"), undefined);

  const manifest = makeValidPersisted("restore-batch");
  const outcome = await second.create("restore-batch", manifest.contentHash, manifest);
  assert.equal(outcome.status, "quarantined");
});

const CONTRACT_VIOLATIONS: Array<{ name: string; batchId: string; mutate: (doc: any) => void }> = [
  {
    name: "duplicate record alias",
    batchId: "corrupt-dup-alias",
    mutate: (doc) => {
      doc.records[1] = JSON.parse(JSON.stringify(doc.records[0]));
    },
  },
  {
    name: "dangling related alias",
    batchId: "corrupt-dangling",
    mutate: (doc) => {
      doc.records[0].relatedAliases = [`rec-${"0".repeat(32)}`];
    },
  },
  {
    name: "duplicate related alias",
    batchId: "corrupt-dup-related",
    mutate: (doc) => {
      const target = doc.records[1].recordAlias;
      doc.records[0].relatedAliases = [target, target];
    },
  },
  {
    name: "invalid createdAt",
    batchId: "corrupt-created-at",
    mutate: (doc) => {
      doc.createdAt = "not-a-timestamp";
    },
  },
  {
    name: "invalid contentHash",
    batchId: "corrupt-hash",
    mutate: (doc) => {
      doc.contentHash = "not-a-sha256";
    },
  },
  {
    name: "unknown top-level field",
    batchId: "corrupt-top-field",
    mutate: (doc) => {
      doc.legacy = true;
    },
  },
  {
    name: "raw identifier field left in record",
    batchId: "corrupt-raw-field",
    mutate: (doc) => {
      doc.records[0].patientId = "P-1";
    },
  },
  {
    name: "non-scalar measurement",
    batchId: "corrupt-measurement",
    mutate: (doc) => {
      doc.records[0].measurements = { nested: { leak: 1 } };
    },
  },
  {
    name: "empty records",
    batchId: "corrupt-empty",
    mutate: (doc) => {
      doc.records = [];
    },
  },
  {
    name: "alias with wrong class prefix",
    batchId: "corrupt-prefix",
    mutate: (doc) => {
      doc.records[0].patientAlias = doc.records[0].recordAlias;
    },
  },
];

test("persisted entries violating the contract are quarantined while valid ones load", async () => {
  const dir = mkdtempSync(join(tmpdir(), "manifest-restore-contract-"));

  // One legitimate entry must survive untouched.
  const valid = makeValidPersisted("restore-valid");
  await writeFile(join(dir, fileNameFor("restore-valid")), JSON.stringify(valid));

  for (const c of CONTRACT_VIOLATIONS) {
    const doc: any = makeValidPersisted(c.batchId);
    c.mutate(doc);
    await writeFile(join(dir, fileNameFor(c.batchId)), JSON.stringify(doc));
  }

  const store = new ManifestStore(dir);
  await store.load();

  assert.deepEqual(store.get("restore-valid"), valid);
  assert.ok(!store.isQuarantined("restore-valid"));
  for (const c of CONTRACT_VIOLATIONS) {
    assert.equal(store.get(c.batchId), undefined, c.name);
    assert.ok(store.isQuarantined(c.batchId), c.name);
  }

  // Over HTTP the valid entry is served and every corrupt one is hidden.
  const { server, baseUrl } = await startServer(store);
  try {
    const okRes = await fetch(`${baseUrl}/api/manifests/restore-valid`);
    assert.equal(okRes.status, 200);
    assert.deepEqual(await okRes.json(), JSON.parse(JSON.stringify(valid)));
    for (const c of CONTRACT_VIOLATIONS) {
      const res = await fetch(`${baseUrl}/api/manifests/${c.batchId}`);
      assert.equal(res.status, 409, c.name);
      const body = await res.json();
      assert.equal(body.error, "manifest_quarantined", c.name);
      assert.ok(!JSON.stringify(body).includes("P-1"), c.name);
    }
  } finally {
    await stopServer(server);
  }
});

test("an entry stored under a file name not matching its batchId is quarantined", async () => {
  const dir = mkdtempSync(join(tmpdir(), "manifest-restore-binding-"));
  const doc = makeValidPersisted("binding-batch");
  // Valid content, but persisted under the hash of a DIFFERENT batchId.
  await writeFile(join(dir, fileNameFor("other-batch")), JSON.stringify(doc));

  const store = new ManifestStore(dir);
  await store.load();

  assert.equal(store.get("binding-batch"), undefined);
  assert.ok(store.isQuarantined("binding-batch"));
  // The unrelated batchId whose file name was abused stays usable.
  assert.ok(!store.isQuarantined("other-batch"));
  assert.equal(store.get("other-batch"), undefined);
});

test("unparseable entries are quarantined without affecting valid manifests", async () => {
  const dir = mkdtempSync(join(tmpdir(), "manifest-restore-unparseable-"));
  const valid = makeValidPersisted("restore-valid");
  await writeFile(join(dir, fileNameFor("restore-valid")), JSON.stringify(valid));
  await writeFile(join(dir, fileNameFor("garbage-batch")), "{not json");

  const store = new ManifestStore(dir);
  await store.load();

  assert.deepEqual(store.get("restore-valid"), valid);
  const quarantined = await readdir(join(dir, "quarantine"));
  assert.equal(quarantined.length, 1);
  assert.ok(quarantined[0].startsWith(fileNameFor("garbage-batch")));
});
