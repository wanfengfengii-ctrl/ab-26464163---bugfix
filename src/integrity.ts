import type { SharedManifest, ValidationIssue } from "./types.ts";
import {
  BATCH_ID_PATTERN,
  MAX_RECORDS,
  MAX_RELATED,
  isPlainObject,
  validateMeasurements,
} from "./validation.ts";
import type { IssueSink } from "./validation.ts";

/**
 * Restore-time integrity validation for persisted shared manifests.
 *
 * A data volume may have been restored from a backup, migrated from an older
 * version or logically corrupted, so every persisted entry must re-satisfy the
 * exact contract the current pipeline produces before it may be served:
 *
 *  - fixed top-level and record fields with correct types;
 *  - `createdAt` is a canonical ISO-8601 UTC timestamp (millisecond precision)
 *    and `contentHash` a lowercase SHA-256 hex digest;
 *  - aliases carry their class prefix and a 128-bit hex digest
 *    (`pat-` / `acc-` / `rec-` + 32 lowercase hex chars);
 *  - measurements are plain objects of allowed scalar values (shared with the
 *    inbound contract via {@link validateMeasurements});
 *  - every `recordAlias` is unique within the manifest and `relatedAliases`
 *    are unique per record and reference only aliases present in the manifest.
 *
 * Like the inbound validator, issues report JSON paths and rule codes only:
 * a corrupt entry may contain raw identifiers, and those must never be echoed
 * into logs or responses.
 */

const RECORD_ALIAS_PATTERN = /^rec-[0-9a-f]{32}$/;
const PATIENT_ALIAS_PATTERN = /^pat-[0-9a-f]{32}$/;
const ACCESSION_ALIAS_PATTERN = /^acc-[0-9a-f]{32}$/;
const CONTENT_HASH_PATTERN = /^[0-9a-f]{64}$/;
const CREATED_AT_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const MANIFEST_FIELDS = new Set(["batchId", "createdAt", "contentHash", "records"]);
const RECORD_FIELDS = new Set([
  "recordAlias",
  "patientAlias",
  "accessionAlias",
  "relatedAliases",
  "measurements",
]);

/** Bounds memory use when a large corrupt file violates many rules at once. */
const MAX_ISSUES = 1_000;

class IssueCollector implements IssueSink {
  readonly issues: ValidationIssue[] = [];

  add(code: string, path: string, message: string): void {
    if (this.issues.length >= MAX_ISSUES) return;
    this.issues.push({ code, path, message });
  }
}

export interface PersistedManifestCheck {
  /** Non-null only when the document fully satisfies the persisted contract. */
  manifest: SharedManifest | null;
  /** Privacy-safe issues: JSON paths and rule codes, never offending values. */
  issues: ValidationIssue[];
}

function isValidCreatedAt(value: unknown): value is string {
  if (typeof value !== "string" || !CREATED_AT_PATTERN.test(value)) return false;
  const parsed = Date.parse(value);
  // Round-trip to reject impossible dates the pattern alone would accept.
  return Number.isFinite(parsed) && new Date(parsed).toISOString() === value;
}

function validateRecord(
  raw: unknown,
  index: number,
  issues: IssueCollector,
  recordAliases: Set<string>,
): void {
  const path = `records[${index}]`;
  if (!isPlainObject(raw)) {
    issues.add("record_not_object", path, "each record must be a JSON object");
    return;
  }

  for (const key of Object.keys(raw)) {
    if (!RECORD_FIELDS.has(key)) {
      issues.add("record_unknown_field", `${path}.${key}`, "unexpected field in record");
    }
  }

  if (typeof raw.recordAlias !== "string" || !RECORD_ALIAS_PATTERN.test(raw.recordAlias)) {
    issues.add(
      "invalid_record_alias",
      `${path}.recordAlias`,
      "recordAlias must be a rec- prefixed 128-bit hex alias",
    );
  } else {
    if (recordAliases.has(raw.recordAlias)) {
      issues.add(
        "duplicate_record_alias",
        `${path}.recordAlias`,
        "recordAlias must be unique within the manifest",
      );
    }
    recordAliases.add(raw.recordAlias);
  }
  if (typeof raw.patientAlias !== "string" || !PATIENT_ALIAS_PATTERN.test(raw.patientAlias)) {
    issues.add(
      "invalid_patient_alias",
      `${path}.patientAlias`,
      "patientAlias must be a pat- prefixed 128-bit hex alias",
    );
  }
  if (typeof raw.accessionAlias !== "string" || !ACCESSION_ALIAS_PATTERN.test(raw.accessionAlias)) {
    issues.add(
      "invalid_accession_alias",
      `${path}.accessionAlias`,
      "accessionAlias must be an acc- prefixed 128-bit hex alias",
    );
  }

  if (!Array.isArray(raw.relatedAliases)) {
    issues.add(
      "related_aliases_not_array",
      `${path}.relatedAliases`,
      "relatedAliases must be an array of record aliases",
    );
  } else {
    if (raw.relatedAliases.length > MAX_RELATED) {
      issues.add("too_many_related", `${path}.relatedAliases`, "too many related aliases");
    }
    const seen = new Set<string>();
    raw.relatedAliases.forEach((alias: unknown, j: number) => {
      if (typeof alias !== "string" || !RECORD_ALIAS_PATTERN.test(alias)) {
        issues.add(
          "invalid_related_alias",
          `${path}.relatedAliases[${j}]`,
          "relatedAliases entries must be rec- prefixed 128-bit hex aliases",
        );
        return;
      }
      if (seen.has(alias)) {
        issues.add(
          "duplicate_related_alias",
          `${path}.relatedAliases[${j}]`,
          "relatedAliases entries must be unique within a record",
        );
      }
      seen.add(alias);
    });
  }

  validateMeasurements(raw.measurements, `${path}.measurements`, issues);
}

/**
 * Validate a parsed persisted manifest against the current contract. Returns
 * the manifest only when every rule holds; otherwise returns the collected
 * privacy-safe issues.
 */
export function validatePersistedManifest(raw: unknown): PersistedManifestCheck {
  const issues = new IssueCollector();

  if (!isPlainObject(raw)) {
    return {
      manifest: null,
      issues: [
        {
          code: "manifest_not_object",
          path: "$",
          message: "persisted manifest must be a JSON object",
        },
      ],
    };
  }

  for (const key of Object.keys(raw)) {
    if (!MANIFEST_FIELDS.has(key)) {
      issues.add(
        "manifest_unknown_field",
        `$.${key}`,
        "unexpected top-level field in persisted manifest",
      );
    }
  }

  if (typeof raw.batchId !== "string" || !BATCH_ID_PATTERN.test(raw.batchId)) {
    issues.add(
      "invalid_batch_id",
      "$.batchId",
      "batchId must be 1-128 chars from letters, digits, dot, underscore or dash",
    );
  }
  if (!isValidCreatedAt(raw.createdAt)) {
    issues.add(
      "invalid_created_at",
      "$.createdAt",
      "createdAt must be a canonical ISO-8601 UTC timestamp with millisecond precision",
    );
  }
  if (typeof raw.contentHash !== "string" || !CONTENT_HASH_PATTERN.test(raw.contentHash)) {
    issues.add(
      "invalid_content_hash",
      "$.contentHash",
      "contentHash must be a lowercase SHA-256 hex digest",
    );
  }

  const recordAliases = new Set<string>();
  if (!Array.isArray(raw.records)) {
    issues.add("records_not_array", "$.records", "records must be an array");
  } else {
    if (raw.records.length === 0) {
      issues.add("records_empty", "$.records", "at least one record is required");
    }
    if (raw.records.length > MAX_RECORDS) {
      issues.add("too_many_records", "$.records", "record count exceeds the maximum");
    }
    raw.records.forEach((record: unknown, index: number) => {
      validateRecord(record, index, issues, recordAliases);
    });
    // Reference closure: every well-formed relatedAlias must point at a
    // recordAlias present in this manifest (malformed ones were already
    // reported above).
    raw.records.forEach((record: unknown, index: number) => {
      if (!isPlainObject(record) || !Array.isArray(record.relatedAliases)) return;
      record.relatedAliases.forEach((alias: unknown, j: number) => {
        if (
          typeof alias === "string" &&
          RECORD_ALIAS_PATTERN.test(alias) &&
          !recordAliases.has(alias)
        ) {
          issues.add(
            "dangling_related_alias",
            `records[${index}].relatedAliases[${j}]`,
            "every relatedAlias must reference a recordAlias present in the same manifest",
          );
        }
      });
    });
  }

  if (issues.issues.length > 0) {
    return { manifest: null, issues: issues.issues };
  }
  return { manifest: raw as unknown as SharedManifest, issues: [] };
}

/**
 * Best-effort extraction of a pattern-valid batchId from an untrusted parsed
 * document. Used to taint and log the batchId of a quarantined entry; anything
 * that does not match the strict batchId format yields null (and is never
 * logged, since it could itself be a raw identifier).
 */
export function extractBatchId(raw: unknown): string | null {
  if (!isPlainObject(raw)) return null;
  const batchId = raw.batchId;
  return typeof batchId === "string" && BATCH_ID_PATTERN.test(batchId) ? batchId : null;
}
