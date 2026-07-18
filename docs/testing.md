# Testing

pg-kinetic keeps platform-neutral Rust checks separate from smoke checks that
need a local PostgreSQL stack or client runtimes. Run commands from the
repository root.

## Linux

Install Rust, Bash, Docker Compose, PostgreSQL client tools, Go, Node.js, and
Python before running the complete local workflow.

~~~bash
docker compose -f bench/compose.yml up -d --build postgres pg-kinetic
cargo run -p xtask -- ci-linux
~~~

The Linux smoke target runs the psql check and the Rust, Go, Node.js, and Python
compatibility clients, followed by the deterministic performance smoke.
scripts/smoke/psql.sh exits successfully with a stable SKIP marker when psql is
unavailable. scripts/smoke/compat.sh does the same when one of its required
runtimes is unavailable. A reachable local stack is still required once those
tools are installed.

Run individual smoke checks when iterating:

~~~bash
bash scripts/smoke/psql.sh
bash scripts/smoke/compat.sh
bash scripts/smoke/read-routing.sh
bash scripts/smoke/performance.sh
~~~

Validate the benchmark command plumbing without collecting live measurements:

~~~bash
bash scripts/bench/run-performance.sh --dry-run
bash scripts/bench/profile-performance.sh --dry-run
~~~

The default benchmark report path is under bench/results/, which is ignored.
Use scripts/bench/compare-performance.sh with reviewed baseline and candidate
reports to enforce the existing comparison budgets.

## Windows

Use PowerShell for the existing Windows smoke entry points:

~~~powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/smoke/psql.ps1
powershell.exe -ExecutionPolicy Bypass -File scripts/smoke/compat.ps1
~~~

Use Git Bash or WSL for the Linux Bash scripts. The host-oriented xtask smoke
command uses the PowerShell psql smoke script on Windows:

~~~powershell
cargo run -p xtask -- smoke --dry-run
cargo test -p pg-kinetic --test linux_smoke_scripts
cargo test --workspace -j 1
~~~

The ci-linux command remains useful as a dry run on Windows. Run its full form
only when Bash, the local stack, and all compatibility runtimes are available.

## Xtask Commands

~~~text
cargo run -p xtask -- check
cargo run -p xtask -- smoke
cargo run -p xtask -- smoke-linux
cargo run -p xtask -- regression --list
cargo run -p xtask -- bench-validate
cargo run -p xtask -- bench-score
cargo run -p xtask -- docs-check
cargo run -p xtask -- ci-linux
~~~

Regression dispatch preserves its Bash argument form. `bench-score` defaults to
the checked-in deterministic sample comparison and accepts reviewed `--baseline`
and `--current` JSON reports for real candidate runs.

## CI Mapping

The Linux workflow runs the same local entrypoints: `cargo run -p xtask --
ci-linux`, docs checks, regression manifest listing, smoke dry-runs, benchmark
scenario validation, and the deterministic sample score comparison. The full
driver compatibility matrix remains the next platform expansion; this phase
keeps the shared smoke and regression contracts ready for that work.
