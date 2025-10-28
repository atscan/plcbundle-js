#!/usr/bin/env node

/**
 * plcbundle.ts - Fetch from PLC Directory and create verifiable bundles
 */

import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { init, compress } from '@bokuweb/zstd-wasm';
import axios from 'axios';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const BUNDLE_SIZE = 10000;
const INDEX_FILE = 'plc_bundles.json';
const PLC_URL = 'https://plc.directory';

// ============================================================================
// Types
// ============================================================================

interface PLCOperation {
  did: string;
  cid: string;
  createdAt: string;
  operation: Record<string, any>;
  nullified?: boolean | string;
  _raw?: string;
}

interface BundleMetadata {
  bundle_number: number;
  start_time: string;
  end_time: string;
  operation_count: number;
  did_count: number;
  hash: string;
  content_hash: string;
  parent: string;
  compressed_hash: string;
  compressed_size: number;
  uncompressed_size: number;
  created_at: string;
}

interface Index {
  version: string;
  last_bundle: number;
  updated_at: string;
  total_size_bytes: number;
  bundles: BundleMetadata[];
}

// Initialize zstd
await init();

// ============================================================================
// Index Management
// ============================================================================

const loadIndex = async (dir: string): Promise<Index> => {
  try {
    const data = await fs.readFile(path.join(dir, INDEX_FILE), 'utf8');
    return JSON.parse(data);
  } catch (err) {
    return {
      version: '1.0',
      last_bundle: 0,
      updated_at: new Date().toISOString(),
      total_size_bytes: 0,
      bundles: []
    };
  }
};

const saveIndex = async (dir: string, index: Index): Promise<void> => {
  index.updated_at = new Date().toISOString();
  const indexPath = path.join(dir, INDEX_FILE);
  const tempPath = indexPath + '.tmp';
  await fs.writeFile(tempPath, JSON.stringify(index, null, 2));
  await fs.rename(tempPath, indexPath);
};

// ============================================================================
// PLC Directory Client
// ============================================================================

const fetchOperations = async (after: string | null, count: number = 1000): Promise<PLCOperation[]> => {
  const params: Record<string, any> = { count };
  if (after) {
    params.after = after;
  }
  
  const response = await axios.get<string>(`${PLC_URL}/export`, {
    params,
    responseType: 'text'
  });
  
  const lines = response.data.trim().split('\n').filter(l => l);
  
  return lines.map(line => {
    const op = JSON.parse(line) as PLCOperation;
    op._raw = line; // Preserve exact JSON
    return op;
  });
};

// ============================================================================
// Bundle Operations
// ============================================================================

const serializeJSONL = (operations: PLCOperation[]): string => {
  // Each operation followed by \n, but NO trailing newline at the end
  // This matches the Go implementation exactly
  const lines = operations.map(op => {
    const json = op._raw || JSON.stringify(op);
    return json + '\n';
  });
  return lines.join('');
};

const sha256 = (data: Buffer | string): string => {
  return crypto.createHash('sha256').update(data).digest('hex');
};

const calculateChainHash = (parent: string, contentHash: string): string => {
  let data: string;
  if (!parent || parent === '') {
    // Genesis bundle (first bundle)
    data = `plcbundle:genesis:${contentHash}`;
  } else {
    // Subsequent bundles - chain parent hash with current content
    data = `${parent}:${contentHash}`;
  }
  return sha256(data);
};

const extractUniqueDIDs = (operations: PLCOperation[]): number => {
  const dids = new Set<string>();
  operations.forEach(op => dids.add(op.did));
  return dids.size;
};

const saveBundle = async (
  dir: string,
  bundleNumber: number,
  operations: PLCOperation[],
  parentHash: string
): Promise<BundleMetadata> => {
  const filename = `${String(bundleNumber).padStart(6, '0')}.jsonl.zst`;
  const filepath = path.join(dir, filename);
  
  // Serialize to JSONL (exact format: each line ends with \n, no trailing newline)
  const jsonl = serializeJSONL(operations);
  const uncompressedBuffer = Buffer.from(jsonl, 'utf8');
  
  console.log(`    JSONL size: ${uncompressedBuffer.length} bytes`);
  console.log(`    First 100 chars: ${jsonl.substring(0, 100)}`);
  console.log(`    Last 100 chars: ${jsonl.substring(jsonl.length - 100)}`);
  
  // Calculate content hash
  const contentHash = sha256(uncompressedBuffer);
  const uncompressedSize = uncompressedBuffer.length;
  
  // Calculate chain hash
  const chainHash = calculateChainHash(parentHash, contentHash);
  
  // Compress with zstd level 3 (same as Go SpeedDefault)
  const compressed = compress(uncompressedBuffer, 3);
  const compressedBuffer = Buffer.from(compressed);
  const compressedHash = sha256(compressedBuffer);
  const compressedSize = compressedBuffer.length;
  
  // Write file
  await fs.writeFile(filepath, compressedBuffer);
  
  // Extract metadata
  const startTime = operations[0].createdAt;
  const endTime = operations[operations.length - 1].createdAt;
  const didCount = extractUniqueDIDs(operations);
  
  return {
    bundle_number: bundleNumber,
    start_time: startTime,
    end_time: endTime,
    operation_count: operations.length,
    did_count: didCount,
    hash: chainHash,              // Chain hash (primary)
    content_hash: contentHash,    // Content hash
    parent: parentHash || '',     // Parent chain hash
    compressed_hash: compressedHash,
    compressed_size: compressedSize,
    uncompressed_size: uncompressedSize,
    created_at: new Date().toISOString()
  };
};

// ============================================================================
// Main Logic
// ============================================================================

const run = async (): Promise<void> => {
  const dir = process.argv[2] || './bundles';
  
  console.log('PLC Bundle Fetcher');
  console.log('==================');
  console.log();
  console.log(`Directory: ${dir}`);
  console.log(`Source:    ${PLC_URL}`);
  console.log();
  
  await fs.mkdir(dir, { recursive: true });
  
  // Load existing index
  const index = await loadIndex(dir);
  
  let currentBundle = index.last_bundle + 1;
  let cursor: string | null = null;
  let parentHash = '';
  
  // If resuming, get cursor and parent from last bundle
  if (index.bundles.length > 0) {
    const lastBundle = index.bundles[index.bundles.length - 1];
    cursor = lastBundle.end_time;
    parentHash = lastBundle.hash; // Chain hash from previous bundle
    console.log(`Resuming from bundle ${currentBundle}`);
    console.log(`Last operation: ${cursor}`);
    console.log(`Parent hash: ${parentHash}`);
  } else {
    console.log('Starting from the beginning (genesis)');
  }
  
  console.log();
  
  let mempool: PLCOperation[] = [];
  let totalFetched = 0;
  let totalBundles = 0;
  
  while (true) {
    try {
      // Fetch operations
      console.log(`Fetching operations (cursor: ${cursor || 'start'})...`);
      const operations = await fetchOperations(cursor, 1000);
      
      if (operations.length === 0) {
        console.log('No more operations available');
        break;
      }
      
      console.log(`  Fetched ${operations.length} operations`);
      totalFetched += operations.length;
      
      // Add to mempool
      mempool.push(...operations);
      
      // Update cursor
      cursor = operations[operations.length - 1].createdAt;
      
      // Create bundles while we have enough operations
      while (mempool.length >= BUNDLE_SIZE) {
        const bundleOps = mempool.splice(0, BUNDLE_SIZE);
        
        console.log(`Creating bundle ${String(currentBundle).padStart(6, '0')}...`);
        
        const metadata = await saveBundle(dir, currentBundle, bundleOps, parentHash);
        
        // Add to index
        index.bundles.push(metadata);
        index.last_bundle = currentBundle;
        index.total_size_bytes += metadata.compressed_size;
        
        await saveIndex(dir, index);
        
        console.log(`  ✓ Bundle ${String(currentBundle).padStart(6, '0')}: ${metadata.operation_count} ops, ${metadata.did_count} DIDs`);
        console.log(`    Chain Hash:   ${metadata.hash}`);
        console.log(`    Content Hash: ${metadata.content_hash}`);
        if (metadata.parent) {
          console.log(`    Parent Hash:  ${metadata.parent}`);
        } else {
          console.log(`    Parent Hash:  (genesis)`);
        }
        console.log(`    Compressed:   ${metadata.compressed_hash}`);
        console.log(`    Size: ${(metadata.compressed_size / 1024).toFixed(1)} KB (${(metadata.compressed_size / metadata.uncompressed_size * 100).toFixed(1)}% of original)`);
        console.log();
        
        // Update parent hash for next bundle
        parentHash = metadata.hash;
        
        currentBundle++;
        totalBundles++;
      }
      
      // Small delay to be nice to the server
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (err: any) {
      console.error(`Error: ${err.message}`);
      
      if (err.response) {
        console.error(`HTTP Status: ${err.response.status}`);
      }
      
      if (err.code === 'ECONNRESET' || err.code === 'ECONNABORTED') {
        console.log('Connection error, retrying in 5 seconds...');
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }
      
      break;
    }
  }
  
  // Save final index
  await saveIndex(dir, index);
  
  console.log();
  console.log('================');
  console.log('Complete!');
  console.log('================');
  console.log(`Total operations fetched: ${totalFetched}`);
  console.log(`Bundles created: ${totalBundles}`);
  console.log(`Total bundles: ${index.bundles.length}`);
  console.log(`Mempool: ${mempool.length} operations`);
  console.log(`Total size: ${(index.total_size_bytes / 1024 / 1024).toFixed(1)} MB`);
  
  if (mempool.length > 0) {
    console.log();
    console.log(`Note: ${mempool.length} operations in mempool (need ${BUNDLE_SIZE - mempool.length} more for next bundle)`);
  }
};

// ============================================================================
// Entry Point
// ============================================================================

run().catch(err => {
  console.error('Fatal error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
