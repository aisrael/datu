# datu-wasm browser example

A small static page that loads the `datu-wasm` package built by `wasm-pack` and runs its
schema-inspection and format-conversion functions (Parquet/Avro/CSV/JSON) against
`fixtures/table.parquet`, entirely client-side.

## Build the wasm package

From the repository root:

```sh
wasm-pack build crates/datu-wasm --target web --out-dir pkg
```

This produces `crates/datu-wasm/pkg/` (gitignored — a generated build artifact, rebuild it
whenever `crates/datu-wasm/src/**` changes).

## Serve and open

Browsers block `fetch()` on `file://` URLs, so this must be served over HTTP. From the
**repository root** (the page fetches `fixtures/table.parquet` via a relative path):

```sh
python3 -m http.server 8000
```

Then open <http://localhost:8000/crates/datu-wasm/examples/verification/index.html> and click
**Run verification**.
