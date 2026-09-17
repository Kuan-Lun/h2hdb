# Understand and reproduce verification results

This guide is for users who want to assess the evidence behind an H2HDB release
or reproduce its checks on a separate machine. None of these checks is needed
to browse an already configured library.

To inspect your own database, use `python -m h2hdb check --config core-reader.json`
as described in the [main guide](../README.md). The tools here check the software,
models, and synthetic test databases; passing them does not inspect your deployed
database or media files.

## What the evidence means

| Evidence | What it can establish | What it does not establish |
| --- | --- | --- |
| Schema checks | The declared data relationships satisfy the checked rules and the generated database definitions match them | That the declarations include every real-world requirement |
| Lean proofs | The stated mathematical results hold for all inputs covered by their assumptions | That Python, SQL, or filesystem operations implement those models correctly |
| TLA+/TLC checks | The selected finite model has no counterexample in the explored states | An unrestricted proof for every deployment or input size |
| Runtime and fault tests | The executed cases produce the expected results, including selected interruption/retry scenarios | That every possible failure, platform, or storage system was exercised |
| Performance measurements | The recorded work and timings for a particular input, machine, and software version | A latency or memory guarantee for your library |

A release-check receipt records success for one exact source tree and check
profile. It does not imply that every available test was run. A skipped test is
not evidence for its backend or scenario.

## Find the supporting evidence

- [Evidence index](invariants.toml): the contracts and their proof, runtime,
  fault, and integration references, including any declared blockers.
- [Catalog specification](schema/catalog.toml) and
  [operational specification](schema/operational.toml): the declared database
  relationships and assumptions.
- [Lean models](lean/) and [TLA+ models](tla/): the mathematical claims and the
  finite model configurations.
- [Test suite](../tests/): executable checks, including separately selected
  SQLite and MariaDB cases.
- [Catalog benchmark guide](../benchmarks/README.md): synthetic SQLite read
  measurements and how to interpret their reports.

The files and command results for the version being assessed are the source of
truth. Counts of tables, proofs, and tests can change between releases.

## Prepare a separate checkout

Use a source checkout matching the release you want to assess. The commands
below run from its root and use POSIX shell syntax. Prepare Python 3.14, `uv`,
and Node.js/npm, then install the checkout's local tools:

```bash
./scripts/rebuild-env.sh
```

This recreates the checkout's `.venv` and installs its Python and Markdown
checking dependencies. It needs access to the configured package registries.
Use a dedicated checkout if you need to keep an existing environment.

For proof checks, install Lean through `elan` using the version in
[`lean-toolchain`](../lean-toolchain). TLC uses the version and checksum in
[`tools.lock.toml`](tools.lock.toml); its command can use host Java or Docker.
The complete release check also needs a working Docker daemon for disposable
MariaDB 10.11.11 tests. These tests do not require your production credentials.

## Choose a check

### Schema and evidence index

```bash
.venv/bin/python scripts/verify-formal.py schema
.venv/bin/python scripts/verify-formal.py coverage
```

`schema` checks the declarations and generated artifacts. `coverage` validates
the evidence index and fails if it has declared production blockers. It checks
the index's references; it does not execute every referenced test.

For metadata validation alone, use:

```bash
.venv/bin/python scripts/verify-formal.py coverage --validate-only
```

This mode can exit successfully while reporting blockers. Read the report before
interpreting its status as a readiness result.

### Mathematical models

```bash
.venv/bin/python scripts/verify-formal.py lean
.venv/bin/python scripts/fetch-formal-tools.py
.venv/bin/python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar
```

The fetch command downloads the pinned TLC tool when a valid local copy is
unavailable. The default TLC command runs the small finite profiles. A model
check must finish exploring its state queue; an interrupted run provides no
completed result for that profile.

### Complete release-check profile

```bash
./scripts/check-full.sh
```

This runs formatting, type and Markdown checks; evidence metadata; schema and
generated-artifact checks; Lean; the bounded pytest profile; small TLC profiles;
and package distribution checks.

The pytest portion shares a 300-second deadline across its ordinary
non-MariaDB cases and a small, explicitly selected MariaDB smoke set, including
owned pytest process cleanup. Docker resource cleanup can finish separately.
This deadline applies to the pytest portion, not the entire shell command.
The receipt does not cover the full deep matrices, all live-MariaDB cases, or
deep TLC runs. Evidence metadata in this profile uses `--validate-only`; use
plain `coverage` above to additionally reject declared production blockers.

### Optional extended checks

Run these separately when you need the broader evidence and have time and
resources available:

```bash
./scripts/check-pytest-deep.sh
.venv/bin/python scripts/verify-formal.py tla \
  --tla-jar .formal-tools/tla2tools-1.7.4.jar --deep
```

The deep pytest script runs the full non-MariaDB and live-MariaDB suites and the
separate server-crash profile. It has no default overall timeout and requires
Docker. Deep TLC runs are also outside the ordinary release profile. Record
whether a TLC run was exhaustive model checking or only simulation.

Cleanup has a separate targeted acceptance profile:

```bash
.venv/bin/python scripts/run-pytest.py cleanup-acceptance
```

It exercises real compaction and recovery cases on SQLite and disposable
MariaDB. It is separate from the bounded release profile and from a full deep
run. Deployment-specific acceptance and power-loss experiments also require
separate results; software checks alone do not prove recovery on your NAS.

## Interpret failures and share results

A missing Lean, Java, Docker, or downloaded tool is an environment failure, not
a passing check. A schema, proof, or runtime failure needs investigation with
the exact software version and invocation preserved.

When sharing a result, include the source commit, commands, operating system,
backend, exit statuses, skipped cases, and any declared blockers. For timing
claims, also record the input size, repeated runs, measured work, and hardware.
Keep finite model results, runtime tests, and deployment measurements distinct.

Report reproducible problems in the
[issue tracker](https://github.com/Kuan-Lun/h2hdb/issues). Share synthetic reports
or redacted output without database credentials or private library content.
