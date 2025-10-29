#!/usr/bin/env node

/**
 * plcbundle.ts - A reference implementation for fetching PLC directory
 * operations and creating verifiable, chained bundles according to the plcbundle V1 spec.
 *
 * This script fetches operations, validates their order, de-duplicates them,
 * and groups them into 10,000-operation bundles. Each bundle is compressed,
 * hashed, and cryptographically linked to the previous one, creating a verifiable
 * chain of data.
 */

import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { init, compress, decompress } from '@bokuweb/zstd-wasm';
import axios from 'axios';

// --- Configuration ---
const BUNDLE_SIZE = 10000;
const INDEX_FILE = 'plc_bundles.json';
const DEFAULT_DIR = './plc_bundles';
const PLC_URL = 'https://plc.directory';

// --- Types (as per spec) ---
interface PLCOperation {
  cid: string;
  createdAt: string;
  _raw: string; // Holds the original raw JSON string for reproducibility.
}

interface BundleMetadata {
  bundle_number: number;
  start_time: string;
  end_time: string;
  operation_count: number;
  did_count: number;
  hash: string; // The chain hash.
  content_hash: string;
  parent: string;
  compressed_hash: string;
  compressed_size: number;
  uncompressed_size: number;
  cursor: string;
  created_at: string;
}

interface Index {
  version: string;
  last_bundle: number;
  updated_at: string;
  total_size_bytes: number;
  bundles: BundleMetadata[];
}

// --- ZSTD Initialization ---
await init();

/**
 * Manages the state and process of fetching, validating, and creating PLC bundles.
 */
class PlcBundleManager {
  private bundleDir: string;
  private index!: Index;
  private mempool: PLCOperation[] = [];
  // This set is used to de-duplicate operations. It tracks CIDs from the
  // previous bundle's boundary and all CIDs from the current mempool.
  private seenCIDs = new Set<string>();

  constructor(bundleDir: string) {
    this.bundleDir = bundleDir;
  }

  /**
   * Initializes the manager by loading the index and seeding the `seenCIDs`
   * set with the CIDs from the last saved bundle's boundary.
   */
  async init() {
    await fs.mkdir(this.bundleDir, { recursive: true });
    this.index = await this._loadIndex();
    console.log(`plcbundle Reference Implementation\nDirectory: ${this.bundleDir}\n`);

    const lastBundle = this.index.bundles.at(-1);
    if (lastBundle) {
      console.log(`Resuming from bundle ${lastBundle.bundle_number + 1}. Last op time: ${lastBundle.end_time}`);
      try {
        // Pre-seed the de-duplication set with CIDs from the previous bundle's boundary.
        // This is crucial for preventing duplicates between two adjacent bundles.
        const prevOps = await this._loadBundleOps(lastBundle.bundle_number);
        this.seenCIDs = this._getBoundaryCIDs(prevOps);
        console.log(`  Seeded de-duplication set with ${this.seenCIDs.size} boundary CIDs.`);
      } catch (e) {
        console.warn(`  Warning: Could not load previous bundle file. Boundary deduplication may be incomplete.`);
      }
    } else {
      console.log('Starting from the beginning (genesis bundle).');
    }
  }

  /**
   * The main execution loop. It continuously fetches operations, validates and
   * de-duplicates them, fills the mempool, and creates bundles when ready.
   */
  async run() {
    let cursor = this.index.bundles.at(-1)?.end_time || null;

    while (true) {
      try {
        console.log(`\nFetching operations from cursor: ${cursor || 'start'}...`);
        const fetchedOps = await this._fetchOperations(cursor);

        if (fetchedOps.length === 0) {
          console.log('No more operations available from PLC directory.');
          break;
        }
        
        // The core ingestion logic: de-duplicate and validate operations before adding to the mempool.
        this._processAndValidateOps(fetchedOps);
        
        // The cursor for the next fetch is always the timestamp of the last operation received in the current batch.
        cursor = fetchedOps[fetchedOps.length - 1].createdAt;

        // If the mempool is full enough, create bundles. This can run multiple times per fetch.
        while (this.mempool.length >= BUNDLE_SIZE) {
          await this._createAndSaveBundle();
        }

        await new Promise(resolve => setTimeout(resolve, 200)); // Be nice to the server.
      } catch (err: any) {
        console.error(`\nError: ${err.message}`);
        if (err.response) console.error(`HTTP Status: ${err.response.status}`);
        if (['ECONNRESET', 'ECONNABORTED'].includes(err.code)) {
          console.log('Connection error, retrying in 5 seconds...');
          await new Promise(resolve => setTimeout(resolve, 5000));
          continue;
        }
        break;
      }
    }

    await this._saveIndex();
    console.log(`\n---`);
    console.log('Process complete.');
    console.log(`Total bundles in index: ${this.index.bundles.length}`);
    console.log(`Operations in mempool: ${this.mempool.length}`);
    console.log(`Total size: ${(this.index.total_size_bytes / 1024 / 1024).toFixed(2)} MB`);
  }
  
  // ==========================================================================
  // Private Helper Methods
  // ==========================================================================

  private async _fetchOperations(after: string | null): Promise<PLCOperation[]> {
    const params = { count: 1000, ...(after && { after }) };
    const response = await axios.get<string>(`${PLC_URL}/export`, { params, responseType: 'text' });
    const lines = response.data.trimEnd().split('\n');
    if (lines.length === 1 && lines[0] === '') return [];
    // Important: The `_raw` property is added here to preserve the original JSON string,
    // ensuring byte-for-byte reproducibility as required by Spec 4.2.
    return lines.map(line => ({ ...JSON.parse(line), _raw: line }));
  }

  /**
   * Processes a batch of fetched operations. It ensures each operation is unique
   * (both within the batch and across bundle boundaries) and that it maintains
   * chronological order before adding it to the mempool.
   */
  private _processAndValidateOps(ops: PLCOperation[]) {
    // The timestamp to validate against is the last operation in the mempool, or if empty,
    // the end time of the last bundle. This prevents chronological gaps.
    let lastTimestamp = this.mempool.at(-1)?.createdAt || this.index.bundles.at(-1)?.end_time || '';
    let newOpsCount = 0;

    for (const op of ops) {
      // The `seenCIDs` set efficiently handles duplicates from the previous bundle's
      // boundary as well as any duplicates within the current fetched batch.
      if (this.seenCIDs.has(op.cid)) {
        continue;
      }

      // Spec 3: Validate that the stream is chronological. This is a critical sanity check.
      if (op.createdAt < lastTimestamp) {
        throw new Error(`Chronological validation failed: op ${op.cid} at ${op.createdAt} is older than last op at ${lastTimestamp}`);
      }
      
      this.mempool.push(op);
      this.seenCIDs.add(op.cid); // Add the CID to the set only after it's confirmed valid.
      lastTimestamp = op.createdAt;
      newOpsCount++;
    }
    console.log(`  Added ${newOpsCount} new operations to mempool.`);
  }

  /**
   * Takes 10,000 operations from the mempool, creates a bundle file, generates
   * its metadata according to the spec, and updates the index.
   */
  private async _createAndSaveBundle() {
    const currentBundleNumber = this.index.last_bundle + 1;
    const bundleOps = this.mempool.splice(0, BUNDLE_SIZE);

    const parentHash = this.index.bundles.at(-1)?.hash || '';
    const previousCursor = this.index.bundles.at(-1)?.end_time || '';

    // The hashing and serialization process follows the spec exactly to ensure compatibility.
    const jsonl = PlcBundleManager._serializeJSONL(bundleOps);
    const uncompressedBuffer = Buffer.from(jsonl, 'utf8');
    const contentHash = PlcBundleManager._sha256(uncompressedBuffer);
    const chainHash = PlcBundleManager._calculateChainHash(parentHash, contentHash);
    const compressedBuffer = Buffer.from(compress(uncompressedBuffer, 3));
    
    const filename = `${String(currentBundleNumber).padStart(6, '0')}.jsonl.zst`;
    await fs.writeFile(path.join(this.bundleDir, filename), compressedBuffer);

    const dids = new Set(bundleOps.map(op => JSON.parse(op._raw).did));
    const metadata: BundleMetadata = {
      bundle_number: currentBundleNumber,
      start_time: bundleOps[0].createdAt,
      end_time: bundleOps[bundleOps.length - 1].createdAt,
      operation_count: bundleOps.length,
      did_count: dids.size,
      hash: chainHash,
      content_hash: contentHash,
      parent: parentHash,
      compressed_hash: PlcBundleManager._sha256(compressedBuffer),
      compressed_size: compressedBuffer.length,
      uncompressed_size: uncompressedBuffer.length,
      cursor: previousCursor,
      created_at: new Date().toISOString()
    };

    this.index.bundles.push(metadata);
    this.index.last_bundle = currentBundleNumber;
    this.index.total_size_bytes += metadata.compressed_size;
    
    // Prune the `seenCIDs` set to keep it memory-efficient. It only needs to hold CIDs
    // from the new boundary and the remaining mempool, not all CIDs ever seen.
    const newBoundaryCIDs = this._getBoundaryCIDs(bundleOps);
    const mempoolCIDs = new Set(this.mempool.map(op => op.cid));
    this.seenCIDs = new Set([...newBoundaryCIDs, ...mempoolCIDs]);

    await this._saveIndex();
    console.log(`\nCreating bundle ${filename}...`);
    console.log(`  ✓ Saved. Hash: ${metadata.hash.substring(0, 16)}...`);
    console.log(`    Set new boundary with ${newBoundaryCIDs.size} CIDs. Pruned de-duplication set to ${this.seenCIDs.size} CIDs.`);
  }
  
  private async _loadIndex(): Promise<Index> {
    try {
      const data = await fs.readFile(path.join(this.bundleDir, INDEX_FILE), 'utf8');
      return JSON.parse(data);
    } catch (err) {
      return { version: '1.0', last_bundle: 0, updated_at: '', total_size_bytes: 0, bundles: [] };
    }
  }

  private async _saveIndex(): Promise<void> {
    this.index.updated_at = new Date().toISOString();
    const tempPath = path.join(this.bundleDir, INDEX_FILE + '.tmp');
    await fs.writeFile(tempPath, JSON.stringify(this.index, null, 2));
    await fs.rename(tempPath, path.join(this.bundleDir, INDEX_FILE));
  }
  
  private async _loadBundleOps(bundleNumber: number): Promise<PLCOperation[]> {
    const filename = `${String(bundleNumber).padStart(6, '0')}.jsonl.zst`;
    const filepath = path.join(this.bundleDir, filename);
    const compressed = await fs.readFile(filepath);
    const decompressed = Buffer.from(decompress(compressed)).toString('utf8');
    return decompressed.trimEnd().split('\n').map(line => ({...JSON.parse(line), _raw: line}));
  }

  /** Returns CIDs from the last timestamp of a bundle, used for boundary de-duplication. */
  private _getBoundaryCIDs(ops: PLCOperation[]): Set<string> {
    if (!ops.length) return new Set();
    const lastTime = ops.at(-1)!.createdAt;
    const cids = new Set<string>();
    for (let i = ops.length - 1; i >= 0 && ops[i].createdAt === lastTime; i--) {
      cids.add(ops[i].cid);
    }
    return cids;
  }
  
  // --- Static Utilities ---
  private static _sha256 = (data: string | Buffer): string => crypto.createHash('sha256').update(data).digest('hex');
  private static _serializeJSONL = (ops: PLCOperation[]): string => ops.map(op => op._raw + '\n').join('');
  private static _calculateChainHash = (parent: string, contentHash: string): string => {
    return PlcBundleManager._sha256(parent ? `${parent}:${contentHash}` : `plcbundle:genesis:${contentHash}`);
  };
}

// --- Entry Point ---
(async () => {
  const dir = process.argv[2] || DEFAULT_DIR;
  const manager = new PlcBundleManager(dir);
  await manager.init();
  await manager.run();
})().catch(err => {
  console.error('\nFATAL ERROR:', err.message);
  console.error(err.stack);
  process.exit(1);
});
