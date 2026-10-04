import { createHash } from "node:crypto";
import { existsSync, mkdirSync } from "node:fs";
import { mkdir, readdir, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { SharedManifest } from "./types.ts";
import { extractBatchId, validatePersistedManifest } from "./integrity.ts";
import { log } from "./log.ts";

/**
 * Persistent manifest store.
 *
 * Only alias-only {@link SharedManifest} documents are ever written to disk;
 * raw identifiers exist solely in the short-lived request handling scope.
 * Files are named by SHA-256(batchId) so storage paths contain no client
 * supplied identifier text, and writes are atomic (temp file + rename).
 *
 * Restore integrity: a data volume may come from a backup, an older version
 * or logical corruption, so every persisted entry is re-validated against the
 * current contract at load time (see integrity.ts) and its file name must
 * match SHA-256 of the batchId it claims. Entries that fail are NOT silently
 * dropped (which would mistake them for "never existed" and allow a silent
 * overwrite) and never served: they are moved to `quarantine/` for diagnosis
 * and their batchId is durably tainted. Quarantined batchIds reject both GET
 * and POST with a conflict until an operator clears the quarantine; the taint
 * is rebuilt from the quarantine directory on every restart.
 */

export type CreateOutcome =
  | { status: "created"; manifest: SharedManifest }
  | { status: "replayed"; manifest: SharedManifest }
  | { status: "conflict" }
  | { status: "quarantined" };

const QUARANTINE_DIR = "quarantine";

export class ManifestStore {
  private readonly manifests = new Map<string, SharedManifest>();
  /** Batch ids bound to persisted entries that failed restore-time validation. */
  private readonly quarantinedBatchIds = new Set<string>();
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
      if (await this.loadEntry(entry)) count++;
    }
    await this.rebuildQuarantineIndex();
    log.info("store_loaded", {
      manifests: count,
      quarantined: this.quarantinedBatchIds.size,
    });
  }

  /** Load one persisted entry; returns true when a valid manifest was restored. */
  private async loadEntry(entry: string): Promise<boolean> {
    let rawText: string;
    try {
      rawText = await readFile(join(this.dataDir, entry), "utf8");
    } catch {
      log.error("store_load_entry_failed", { file: entry });
      return false;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(rawText);
    } catch {
      // Not even parseable: no batchId can be trusted, but the file must not
      // stay servable-looking in the data directory.
      await this.quarantine(entry, null, "unparseable_json", 0);
      return false;
    }

    const check = validatePersistedManifest(parsed);
    if (check.manifest === null) {
      await this.quarantine(entry, extractBatchId(parsed), "contract_violation", check.issues.length);
      return false;
    }

    if (entry !== this.fileName(check.manifest.batchId)) {
      // The entry is not stored under its own batchId hash: the binding
      // between batchId and persisted file is broken.
      await this.quarantine(entry, check.manifest.batchId, "batch_id_binding_mismatch", 0);
      return false;
    }

    this.manifests.set(check.manifest.batchId, check.manifest);
    return true;
  }

  /**
   * Isolate a corrupt entry: taint its batchId (when one can be trusted) and
   * move the file aside for operator diagnosis. The in-memory taint fails
   * closed even if the move itself fails.
   */
  private async quarantine(
    entry: string,
    batchId: string | null,
    reason: string,
    issueCount: number,
  ): Promise<void> {
    if (batchId !== null) this.quarantinedBatchIds.add(batchId);
    try {
      const dir = join(this.dataDir, QUARANTINE_DIR);
      await mkdir(dir, { recursive: true });
      let target = join(dir, entry);
      if (existsSync(target)) target = `${target}.${Date.now()}`;
      await rename(join(this.dataDir, entry), target);
      // Only the hashed file name, the (format-validated) batchId, a reason
      // code and a count are logged — never the corrupt entry's content.
      log.warn("store_entry_quarantined", { file: entry, batchId, reason, issues: issueCount });
    } catch {
      log.error("store_quarantine_failed", { file: entry, reason });
    }
  }

  /**
   * Rebuild batchId taints from previously quarantined files so a restart
   * cannot silently unblock a quarantined batchId.
   */
  private async rebuildQuarantineIndex(): Promise<void> {
    let entries: string[];
    try {
      entries = await readdir(join(this.dataDir, QUARANTINE_DIR));
    } catch {
      return; // No quarantine directory yet.
    }
    for (const entry of entries) {
      try {
        const parsed: unknown = JSON.parse(
          await readFile(join(this.dataDir, QUARANTINE_DIR, entry), "utf8"),
        );
        const batchId = extractBatchId(parsed);
        if (batchId !== null) this.quarantinedBatchIds.add(batchId);
      } catch {
        // Best effort: an unreadable quarantined file yields no batch id.
      }
    }
  }

  get(batchId: string): SharedManifest | undefined {
    return this.manifests.get(batchId);
  }

  /**
   * True when a persisted entry for this batchId failed restore-time
   * validation and has been isolated. Quarantine takes precedence over any
   * loadable entry for the same batchId (ambiguous volume state fails closed).
   */
  isQuarantined(batchId: string): boolean {
    return this.quarantinedBatchIds.has(batchId);
  }

  /**
   * Idempotent create.
   *  - first submission for the batchId: persist and return "created"
   *  - identical business content (same content hash): return "replayed"
   *  - different content for the same batchId: return "conflict" (HTTP 409)
   *  - batchId tainted by a quarantined entry: return "quarantined" (HTTP 409)
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
    if (this.quarantinedBatchIds.has(batchId)) {
      // A corrupt persisted entry already owns this batchId: refuse to
      // silently overwrite it. An operator must clear the quarantine first.
      return { status: "quarantined" };
    }

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
