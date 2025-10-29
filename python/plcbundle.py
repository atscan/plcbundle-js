#!/usr/bin/env python3

"""
plcbundle.py - A compact, readable reference implementation for creating
plcbundle V1 compliant archives. This script demonstrates all critical spec
requirements, including hashing, serialization, ordering, and boundary handling.

PLC Bundle v1 Specification:
  https://github.com/atscan/plcbundle/blob/main/SPECIFICATION.md
"""

import asyncio
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict, Self

import httpx
import zstd

# --- Configuration ---
BUNDLE_SIZE = 10000
INDEX_FILE = 'plc_bundles.json'
DEFAULT_DIR = './plc_bundles_py'
PLC_URL = 'https://plc.directory'

# --- Types (as per spec) ---
class PLCOperation(TypedDict):
    did: str
    cid: str
    createdAt: str
    operation: dict
    nullified: bool | str | None
    _raw: str  # Holds the original raw JSON string for reproducibility

class BundleMetadata(TypedDict):
    bundle_number: int
    start_time: str
    end_time: str
    operation_count: int
    did_count: int
    hash: str  # The chain hash
    content_hash: str
    parent: str
    compressed_hash: str
    compressed_size: int
    uncompressed_size: int
    cursor: str
    created_at: str

class Index(TypedDict):
    version: str
    last_bundle: int
    updated_at: str
    total_size_bytes: int
    bundles: list[BundleMetadata]

class PlcBundleManager:
    """
    Manages the state and process of fetching, validating, and creating PLC bundles.
    """
    _index: Index
    _mempool: list[PLCOperation] = []
    # This set correctly de-duplicates operations, both from the previous bundle's
    # boundary and within new batches, and is pruned to stay memory-efficient.
    _seen_cids = set[str]()

    def __init__(self, bundle_dir: str):
        self._bundle_dir = Path(bundle_dir)
        self._http_client = httpx.AsyncClient(timeout=30)

    @classmethod
    async def create(cls, bundle_dir: str) -> Self:
        """Factory to create and asynchronously initialize a PlcBundleManager instance."""
        manager = cls(bundle_dir)
        await manager._init()
        return manager

    async def _init(self):
        """
        Initializes the manager by loading the index and seeding the `seen_cids`
        set with the CIDs from the last saved bundle's boundary.
        """
        self._bundle_dir.mkdir(exist_ok=True)
        self._index = await self._load_index()
        print(f"plcbundle Reference Implementation\nDirectory: {self._bundle_dir}\n")

        last_bundle = self._index['bundles'][-1] if self._index['bundles'] else None
        if last_bundle:
            print(f"Resuming from bundle {last_bundle['bundle_number'] + 1}. Last op time: {last_bundle['end_time']}")
            try:
                prev_ops = await self._load_bundle_ops(last_bundle['bundle_number'])
                self._seen_cids = self._get_boundary_cids(prev_ops)
                print(f"  Seeded de-duplication set with {len(self._seen_cids)} boundary CIDs.")
            except FileNotFoundError:
                print(f"  Warning: Could not load previous bundle file. Boundary deduplication may be incomplete.")
        else:
            print('Starting from the beginning (genesis bundle).')

    async def run(self):
        """
        The main execution loop. It continuously fetches operations, validates and
        de-duplicates them, fills the mempool, and creates bundles when ready.
        """
        last_bundle = self._index['bundles'][-1] if self._index['bundles'] else None
        cursor = last_bundle['end_time'] if last_bundle else None

        while True:
            try:
                print(f"\nFetching operations from cursor: {cursor or 'start'}...")
                fetched_ops = await self._fetch_operations(cursor)
                if not fetched_ops:
                    print('No more operations available from PLC directory.')
                    break
                
                self._process_and_validate_ops(fetched_ops)
                cursor = fetched_ops[-1]['createdAt']

                while len(self._mempool) >= BUNDLE_SIZE:
                    await self._create_and_save_bundle()

                await asyncio.sleep(0.2)  # Be nice to the server
            except httpx.HTTPStatusError as e:
                print(f"\nError: HTTP {e.response.status_code} - {e.response.text}")
                break
            except Exception as e:
                print(f"\nAn unexpected error occurred: {e}")
                break

        await self._save_index()
        print(f"\n---\nProcess complete.")
        print(f"Total bundles in index: {len(self._index['bundles'])}")
        print(f"Operations in mempool: {len(self._mempool)}")
        total_mb = self._index['total_size_bytes'] / 1024 / 1024
        print(f"Total size: {total_mb:.2f} MB")

    # --- Private Helper Methods ---

    async def _fetch_operations(self, after: str | None) -> list[PLCOperation]:
        params = {'count': 1000}
        if after:
            params['after'] = after
        
        response = await self._http_client.get(f"{PLC_URL}/export", params=params)
        response.raise_for_status()
        
        lines = response.text.strip().split('\n')
        if not lines or not lines[0]:
            return []
        
        # Important: The `_raw` key is added here to preserve the original JSON string,
        # ensuring byte-for-byte reproducibility as required by Spec 4.2.
        return [{**json.loads(line), '_raw': line} for line in lines]

    def _process_and_validate_ops(self, ops: list[PLCOperation]):
        last_op = self._mempool[-1] if self._mempool else None
        last_bundle = self._index['bundles'][-1] if self._index['bundles'] else None
        last_timestamp = last_op['createdAt'] if last_op else (last_bundle['end_time'] if last_bundle else '')
        
        new_ops_count = 0
        for op in ops:
            if op['cid'] in self._seen_cids:
                continue
            
            if op['createdAt'] < last_timestamp:
                raise ValueError(f"Chronological validation failed: op {op['cid']} at {op['createdAt']} is older than last op at {last_timestamp}")
            
            self._mempool.append(op)
            self._seen_cids.add(op['cid'])
            last_timestamp = op['createdAt']
            new_ops_count += 1
        print(f"  Added {new_ops_count} new operations to mempool.")

    async def _create_and_save_bundle(self):
        bundle_ops = self._mempool[:BUNDLE_SIZE]
        self._mempool = self._mempool[BUNDLE_SIZE:]

        last_bundle = self._index['bundles'][-1] if self._index['bundles'] else None
        parent_hash = last_bundle['hash'] if last_bundle else ''
        
        # Spec 4.2 & 6.3: Hashing and serialization must be exact.
        jsonl_data = "".join([op['_raw'] + '\n' for op in bundle_ops]).encode('utf-8')
        content_hash = hashlib.sha256(jsonl_data).hexdigest()
        chain_hash = self._calculate_chain_hash(parent_hash, content_hash)
        compressed_data = zstd.compress(jsonl_data, 3)
        
        bundle_number = self._index['last_bundle'] + 1
        filename = f"{bundle_number:06d}.jsonl.zst"
        (self._bundle_dir / filename).write_bytes(compressed_data)

        self._index['bundles'].append({
            'bundle_number': bundle_number,
            'start_time': bundle_ops[0]['createdAt'],
            'end_time': bundle_ops[-1]['createdAt'],
            'operation_count': len(bundle_ops),
            'did_count': len({op['did'] for op in bundle_ops}),
            'hash': chain_hash, 'content_hash': content_hash, 'parent': parent_hash,
            'compressed_hash': hashlib.sha256(compressed_data).hexdigest(),
            'compressed_size': len(compressed_data),
            'uncompressed_size': len(jsonl_data),
            'cursor': last_bundle['end_time'] if last_bundle else '',
            'created_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        })
        self._index['last_bundle'] = bundle_number
        self._index['total_size_bytes'] += len(compressed_data)

        # Prune `seen_cids` to keep it memory-efficient.
        new_boundary_cids = self._get_boundary_cids(bundle_ops)
        mempool_cids = {op['cid'] for op in self._mempool}
        self._seen_cids = new_boundary_cids.union(mempool_cids)

        await self._save_index()
        print(f"\nCreating bundle {filename}...")
        print(f"  ✓ Saved. Hash: {chain_hash[:16]}...")
        print(f"    Pruned de-duplication set to {len(self._seen_cids)} CIDs.")

    async def _load_index(self) -> Index:
        try:
            return json.loads((self._bundle_dir / INDEX_FILE).read_text())
        except FileNotFoundError:
            return {'version': '1.0', 'last_bundle': 0, 'updated_at': '', 'total_size_bytes': 0, 'bundles': []}

    async def _save_index(self):
        self._index['updated_at'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        temp_path = self._bundle_dir / f"{INDEX_FILE}.tmp"
        temp_path.write_text(json.dumps(self._index, indent=2))
        temp_path.rename(self._bundle_dir / INDEX_FILE)

    async def _load_bundle_ops(self, bundle_number: int) -> list[PLCOperation]:
        filename = f"{bundle_number:06d}.jsonl.zst"
        compressed = (self._bundle_dir / filename).read_bytes()
        decompressed = zstd.decompress(compressed).decode('utf-8')
        return [{**json.loads(line), '_raw': line} for line in decompressed.strip().split('\n')]
    
    # --- Static Utilities ---
    
    @staticmethod
    def _calculate_chain_hash(parent: str, content_hash: str) -> str:
        data = f"{parent}:{content_hash}" if parent else f"plcbundle:genesis:{content_hash}"
        return hashlib.sha256(data.encode('utf-8')).hexdigest()

    @staticmethod
    def _get_boundary_cids(ops: list[PLCOperation]) -> set[str]:
        if not ops: return set()
        last_time = ops[-1]['createdAt']
        return {op['cid'] for op in reversed(ops) if op['createdAt'] == last_time}

async def main():
    """Entry point for the script."""
    dir_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DIR
    manager = await PlcBundleManager.create(dir_path)
    await manager.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
    except Exception as e:
        print(f"\nFATAL ERROR: {e}", file=sys.stderr)
        sys.exit(1)

