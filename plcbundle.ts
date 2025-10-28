#!/usr/bin/env node

/**
 * plcbundle.ts - Fetch from PLC Directory and create verifiable bundles
 */

import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { init, compress, decompress } from '@bokuweb/zstd-wasm';
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
// Bundle Loading
// ============================================================================

const loadBundle = async (dir: string, bundleNumber: number): Promise<PLCOperation[]> => {
  const filename = `${String(bundleNumber).padStart(6, '0')}.jsonl.zst`;
  const filepath = path.join(dir, filename);
  
  const compressed = await fs.readFile(filepath);
  const decompressed = decompress(compressed);
  const jsonl = Buffer.from(decompressed).toString('utf8');
  
  const lines = jsonl.trim().split('\n').filter(l => l);
  return lines.map(line => {
    const op = JSON.parse(line) as PLCOperation;
    op._raw = line;
    return op;
  });
};

// ============================================================================
// Boundary Handling
// ============================================================================

const getBoundaryCIDs = (operations: PLCOperation[]): Set<string> => {
  if (operations.length === 0) return new Set();
  
  const lastOp = operations[operations.length - 1];
  const boundaryTime = lastOp.createdAt;
  const cidSet = new Set<string>();
  
  // Walk backwards from the end to find all operations with the same timestamp
  for (let i = operations.length - 1; i >= 0; i--) {
    if (operations[i].createdAt === boundaryTime) {
      cidSet.add(operations[i].cid);
    } else {
      break;
    }
  }
  
  return cidSet;
};

const stripBoundaryDuplicates = (
  operations: PLCOperation[],
  prevBoundaryCIDs: Set<string>
): PLCOperation[] => {
  if (prevBoundaryCIDs.size === 0) return operations;
  if (operations.length === 0) return operations;
  
  const boundaryTime = operations[0].createdAt;
  let startIdx = 0;
  
  // Skip operations that are in the previous bundle's boundary
  for (let i = 0; i < operations.length; i++) {
    const op = operations[i];
    
    // Stop if we've moved past the boundary timestamp
    if (op.createdAt > boundaryTime) {
      break;
    }
    
    // Skip if this CID was in the previous boundary
    if (op.createdAt === boundaryTime && prevBoundaryCIDs.has(op.cid)) {
      startIdx = i + 1;
      continue;
    }
    
    break;
  }
  
  const stripped = operations.slice(startIdx);
  if (startIdx > 0) {
    console.log(`    Stripped ${startIdx} boundary duplicates`);
  }
  return stripped;
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
    data = `plcbundle:genesis:${contentHash}`;
  } else {
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
  
  const jsonl = serializeJSONL(operations);
  const uncompressedBuffer = Buffer.from(jsonl, 'utf8');
  
  const contentHash = sha256(uncompressedBuffer);
  const uncompressedSize = uncompressedBuffer.length;
  
  const chainHash = calculateChainHash(parentHash, contentHash);
  
  const compressed = compress(uncompressedBuffer, 3);
  const compressedBuffer = Buffer.from(compressed);
  const compressedHash = sha256(compressedBuffer);
  const compressedSize = compressedBuffer.length;
  
  await fs.writeFile(filepath, compressedBuffer);
  
  const startTime = operations[0].createdAt;
  const endTime = operations[operations.length - 1].createdAt;
  const didCount = extractUniqueDIDs(operations);
  
  return {
    bundle_number: bundleNumber,
    start_time: startTime,
    end_time: endTime,
    operation_count: operations.length,
    did_count: didCount,
    hash: chainHash,
    content_hash: contentHash,
    parent: parentHash || '',
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
  
  const index = await loadIndex(dir);
  
  let currentBundle = index.last_bundle + 1;
  let cursor: string | null = null;
  let parentHash = '';
  let prevBoundaryCIDs = new Set<string>();
  
  if (index.bundles.length > 0) {
    const lastBundle = index.bundles[index.bundles.length - 1];
    cursor = lastBundle.end_time;
    parentHash = lastBundle.hash;
    
    try {
      const prevOps = await loadBundle(dir, lastBundle.bundle_number);
      prevBoundaryCIDs = getBoundaryCIDs(prevOps);
      console.log(`Loaded previous bundle boundary: ${prevBoundaryCIDs.size} CIDs`);
    } catch (err) {
      console.log(`Could not load previous bundle for boundary detection`);
    }
    
    console.log(`Resuming from bundle ${currentBundle}`);
    console.log(`Last operation: ${cursor}`);
  } else {
    console.log('Starting from the beginning (genesis)');
  }
  
  console.log();
  
  let mempool: PLCOperation[] = [];
  const seenCIDs = new Set<string>(prevBoundaryCIDs);
  let totalFetched = 0;
  let totalBundles = 0;
  
  while (true) {
    try {
      console.log(`Fetching operations (cursor: ${cursor || 'start'})...`);
      const operations = await fetchOperations(cursor, 1000);
      
      if (operations.length === 0) {
        console.log('No more operations available');
        break;
      }
      
      // Deduplicate
      const uniqueOps = operations.filter(op => {
        if (seenCIDs.has(op.cid)) {
          return false;
        }
        seenCIDs.add(op.cid);
        return true;
      });
      
      console.log(`  Fetched ${operations.length} operations (${uniqueOps.length} unique)`);
      totalFetched += uniqueOps.length;
      
      mempool.push(...uniqueOps);
      cursor = operations[operations.length - 1].createdAt;
      
      while (mempool.length >= BUNDLE_SIZE) {
        const bundleOps = mempool.splice(0, BUNDLE_SIZE);
        
        console.log(`\nCreating bundle ${String(currentBundle).padStart(6, '0')}...`);
        
        const metadata = await saveBundle(dir, currentBundle, bundleOps, parentHash);
        
        index.bundles.push(metadata);
        index.last_bundle = currentBundle;
        index.total_size_bytes += metadata.compressed_size;
        
        await saveIndex(dir, index);
        
        console.log(`  ✓ Bundle ${String(currentBundle).padStart(6, '0')}: ${metadata.operation_count} ops, ${metadata.did_count} DIDs`);
        console.log(`    Chain Hash:   ${metadata.hash}`);
        console.log(`    Content Hash: ${metadata.content_hash}`);
        console.log(`    Size: ${(metadata.compressed_size / 1024).toFixed(1)} KB`);
        
        // Get boundary CIDs for next bundle
        prevBoundaryCIDs = getBoundaryCIDs(bundleOps);
        console.log(`    Boundary CIDs: ${prevBoundaryCIDs.size}`);
        console.log();
        
        parentHash = metadata.hash;
        currentBundle++;
        totalBundles++;
      }
      
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
    console.log(`Note: ${mempool.length} operations in mempool`);
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
