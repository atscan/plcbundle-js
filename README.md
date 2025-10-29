# PLC Bundle V1 Reference Implementation in TypeScript

This script ([plcbundle.ts](plcbundle.ts)) is a compact, readable reference implementation for creating PLC Bundle v1 archives. It fetches operations from the PLC directory and generates a complete, verifiable repository of data bundles.

It is fully compliant with the [PLC Bundle v1 Specification](https://github.com/atscan/plcbundle/blob/main/SPECIFICATION.md).

## Features

-   **Spec Compliant:** Correctly implements hashing, chaining, serialization, and boundary de-duplication.
-   **Reproducible:** Generates byte-for-byte identical bundles to the official Go implementation.
-   **Efficient:** Uses a memory-efficient method to handle duplicates between bundle boundaries.
-   **Standalone:** Single-file script with clear dependencies.

## Usage

This script can be run with **Bun (recommended)**, Deno, or Node.js.

The script accepts one optional argument: the path to the output directory where bundles will be stored. If omitted, it defaults to `./plc_bundles`.

### Bun (Recommended)

Bun is the fastest and easiest way to run this script, as it handles TypeScript and dependencies automatically.

```sh
# Install dependencies
bun install

# Run the script to create bundles in ./my_plc_bundles
bun run plcbundle.ts ./my_plc_bundles
```

### Deno

Deno can also run the script directly. You will need to provide permissions for network access and file system I/O.

```sh
# Run the script with Deno
deno run --allow-net --allow-read --allow-write plcbundle.ts ./my_plc_bundles
```

### Node.js (with TypeScript)

If using Node.js, you must first install dependencies and compile the TypeScript file to JavaScript.

```sh
# Install dependencies
npm install

# Compile the script
npx tsc

# Run the compiled JavaScript
node dist/plcbundle.js ./my_plc_bundles
```

