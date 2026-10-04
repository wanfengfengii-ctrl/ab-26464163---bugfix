import { createHash } from "node:crypto";
import { existsSync, mkdirSync } from "node:fs";
import { readdir, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { SharedManifest } from "./types.ts";
import { log } from "./log.ts";

/**
 * Persistent manifest store.
 *
 * Only alias-only {@link SharedManifest} documents are ever written to disk;
 * raw identifiers exist solely in the short-lived request handling scope.
 * Files are named by SHA-256(batchId) so storage paths contain no client
 * supplied identifier text, and writes are atomic (temp file + rename).
 */

export type CreateOutcome =
  | { status: "created"; manifest: SharedManifest }
  | { status: "replayed"; manifest: SharedManifest }
  | { status: "conflict" };

export class ManifestStore {
  private readonly manifests = new Map<string, SharedManifest>();
  /** Serializes concurrent creates targeting the same batchId. */
  private readonly locks = new Map<string, Promise<unknown>>();

  private readonly dataDir: string;

  constructor(dataDir: string) {
    this.dataDir = dataDir;
    mkdirSync(dataDir, { recursive: true });
  }

  private fileName(batchId: string): string {
    return createHash("sha256").update(batchId, "utf8").digest("hex") + ".json";
  }

  async load(): Promise<void> {
    const entries = await readdir(this.dataDir);
    let count = 0;
    for (const entry of entries) {
      if (!entry.endsWith(".json")) continue;
      try {
        const raw = await readFile(join(this.dataDir, entry), "utf8");
        const parsed = JSON.parse(raw) as SharedManifest;
        if (typeof parsed.batchId === "string" && Array.isArray(parsed.records)) {
          this.manifests.set(parsed.batchId, parsed);
          count++;
        }
      } catch {
        log.error("store_load_entry_failed", { file: entry });
      }
    }
    log.info("store_loaded", { manifests: count });
  }

  get(batchId: string): SharedManifest | undefined {
    return this.manifests.get(batchId);
  }

  /**
   * Idempotent create.
   *  - first submission for the batchId: persist and return "created"
   *  - identical business content (same content hash): return "replayed"
   *  - different content for the same batchId: return "conflict" (HTTP 409)
   */
  create(batchId: string, contentDigest: string, manifest: SharedManifest): Promise<CreateOutcome> {
    const prior = this.locks.get(batchId) ?? Promise.resolve();
    const result = prior.then(() => this.createInner(batchId, contentDigest, manifest));
    this.locks.set(
      batchId,
      result.catch(() => undefined),
    );
    return result;
  }

  private async createInner(
    batchId: string,
    contentDigest: string,
    manifest: SharedManifest,
  ): Promise<CreateOutcome> {
    const existing = this.manifests.get(batchId);
    if (existing !== undefined) {
      return existing.contentHash === contentDigest
        ? { status: "replayed", manifest: existing }
        : { status: "conflict" };
    }

    const target = join(this.dataDir, this.fileName(batchId));
    const tmp = `${target}.${process.pid}.${Date.now()}.tmp`;
    await writeFile(tmp, JSON.stringify(manifest), { mode: 0o600 });
    await rename(tmp, target);
    this.manifests.set(batchId, manifest);
    return { status: "created", manifest };
  }
}
