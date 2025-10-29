# PLC Bundle V1 Reference Implementations

This set of scripts represents a compact, readable reference implementations for creating [PLC Bundle](https://tangled.org/@atscan.net/plcbundle) v1 archives. It fetches operations from the PLC directory and generates a complete, verifiable repository of data bundles.

It is fully compliant with the [PLC Bundle v1 Specification](https://tangled.org/atscan.net/plcbundle/blob/main/docs/specification.md).

## Features

-   **Spec Compliant:** Correctly implements hashing, chaining, serialization, and boundary de-duplication.
-   **Reproducible:** Generates byte-for-byte identical bundles to the other implementations.
-   **Standalone:** Single-file script with clear dependencies.

## Implementations

| Language   | File      |
| ---        | ---       |
| [TypeScript](#typescript) | [`typescript/plcbundle.ts`](typescript/plcbundle.ts) |
| [Python](#python) | [`python/plcbundle.py`](python/plcbundle.py) |
| [Ruby](#ruby) | [`ruby/plcbundle.rb`](ruby/plcbundle.rb) |

## TypeScript

File: [`typescript/plcbundle.ts`](typescript/plcbundle.ts)

### Usage

This script should run well with **[Bun](https://bun.com/) (recommended)**, [Deno](https://deno.com/), or [Node.js](https://nodejs.org/en).

The script accepts one optional argument: the path to the output directory where bundles will be stored. If omitted, it defaults to `./plc_bundles`.

#### Bun (Recommended)

Bun is the fastest and easiest way to run this script, as it handles TypeScript and dependencies automatically.

```sh
# Install dependencies
bun install

# Run the script to create bundles in ./my_plc_bundles
bun run plcbundle.ts ./my_plc_bundles
```

#### Deno

Deno can also run the script directly. You will need to provide permissions for network access and file system I/O.

```sh
# Run the script with Deno
deno run --allow-net --allow-read --allow-write plcbundle.ts ./my_plc_bundles
```

#### Node.js (with TypeScript)

If using Node.js, you must first install dependencies and compile the TypeScript file to JavaScript.

```sh
# Install dependencies
npm install

# Compile the script
npx tsc

# Run the compiled JavaScript
node dist/plcbundle.js ./my_plc_bundles
```

---



## Python

File: [`python/plcbundle.py`](python/plcbundle.py)

### Usage

TODO

## Ruby

File: [`ruby/plcbundle.rb`](ruby/plcbundle.rb)

### Prerequisites

You need Ruby installed (version 3+ recommended).

### Installation

The script relies on two external gems for HTTP requests and Zstandard compression.

```sh
# Install required gems
gem install zstd-ruby
```

### Usage

Run the script from your terminal. It accepts one optional argument: the path to the output directory.

```sh
# Run and save bundles to the default './plc_bundles_rb' directory
ruby plcbundle.rb

# Run and save to a custom directory
ruby plcbundle.rb ./my_ruby_bundles
```