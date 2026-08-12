# Formal verification

This directory contains executable specifications for the proposed vNext
catalog design. It does not describe the currently deployed SQL schema.

The verification layers have different guarantees:

- `GalleryDeduplication.lean` proves completed-snapshot owner and warning
  properties for arbitrary finite input lists.
- `lean/VNextSchema.lean` proves closed-world BCNF over `F+` for the exact
  relations generated from `schema/catalog.toml`. The proof assumes that the
  manifest declares every semantic functional dependency; software cannot
  infer omitted domain semantics from column names or sample rows.
- `schema/check_contract.py` independently enumerates candidate keys and all
  attribute subsets, validates BCNF, foreign-key shape, materialization
  rationale, and explicitly declared binary lossless decompositions.
- `tla/CatalogCore.tla` model-checks lease fencing, crash/takeover, sealing,
  analysis, protected artifacts, atomic publication, and garbage collection.

Hash semantics are explicit rather than global. Only attributes classified as
`canonical_identity_digest` or `payload_digest` in `schema/catalog.toml` are
modeled as collision-free identities; audit and observational digests imply no
reverse FD. Canonical encoders and policies are assumed deterministic. The
Lean and TLA+ results prove their specifications, not that the future Python,
SQL, or filesystem implementation conforms to them. Implementation
conformance still requires migrations, database introspection, differential
tests, and fault injection when the vNext design is implemented.

Passing BCNF therefore means “BCNF under every FD declared in the manifest,”
not “all business-semantic FDs were discovered.” Before accepting a schema
revision, audit domain rules such as identities, digest meaning, uniqueness,
and derived counters, declare the resulting FDs, and rerun both the independent
checker and Lean. Controlled materializations remain acceptable only when the
manifest records their authority, derivation, and refresh rationale.

## Commands

Lean and schema verification require only the pinned Lean toolchain and
Python 3.14:

```bash
uv run --no-sync python scripts/verify-formal.py schema
uv run --no-sync python scripts/verify-formal.py lean
```

TLC 1.7.4 and its SHA-256 are pinned in `tools.lock.toml`:

```bash
uv run --no-sync python scripts/fetch-formal-tools.py
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

`CatalogCoreSmall.cfg` is the required finite PR profile. The `Deep` profile
has a substantially larger state space and is reserved for manual or nightly
runs. A completed TLC run is a complete search only for the constants in its
configuration; it is not an unbounded theorem.

Run the larger profile explicitly:

```bash
uv run --no-sync python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar --deep
```
