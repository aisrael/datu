# datu-wasm conversion example

A small static page that loads the `datu-wasm` package built by `wasm-pack` and lets you drop
in your own Parquet or Avro file to inspect its embedded key/value metadata, its schema, and a
paginated preview of its rows — entirely client-side.

The preview tab converts the whole file to JSON client-side and paginates it in memory (20
rows/page); that's fine for this demo but isn't meant for huge files.

## Build the wasm package

From the repository root:

```sh
wasm-pack build crates/datu-wasm --target web --out-dir pkg
```

This produces `crates/datu-wasm/pkg/` (gitignored — a generated build artifact, rebuild it
whenever `crates/datu-wasm/src/**` changes).

## Serve and open

Browsers block `fetch()` on `file://` URLs, so this must be served over HTTP. From the
**repository root**:

```sh
python3 -m http.server 8000
```

Then open <http://localhost:8000/crates/datu-wasm/examples/conversion/index.html> and drop in
a `.parquet` or `.avro` file (e.g. `fixtures/table.parquet` or `fixtures/userdata5.avro` from
the repository root).
