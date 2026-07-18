# Testing

pg-kinetic keeps platform-neutral Rust checks separate from smoke checks that
need a local PostgreSQL stack or client runtimes. Run commands from the
repository root.

## Linux

Install Rust, Bash, Docker Compose, PostgreSQL client tools, Go, Node.js,
Python, Java, .NET, and native PostgreSQL development libraries before running
the complete local workflow.

~~~bash
docker compose -f bench/compose.yml up -d --build postgres pg-kinetic
cargo run -p xtask -- ci-linux
~~~

The Linux smoke target runs the psql check, compatibility smoke clients, and
the deterministic performance smoke. scripts/smoke/psql.sh exits successfully
with a stable SKIP marker when psql is unavailable. The compatibility runner
also records explicit skips when a toolchain, optional library, or live stack is
unavailable. A reachable local stack is required for live compatibility runs.

Run individual smoke checks when iterating:

~~~bash
bash scripts/smoke/psql.sh
bash scripts/smoke/compat.sh
bash scripts/compat/run.sh --language rust --target pg-kinetic --smoke
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
powershell.exe -ExecutionPolicy Bypass -File scripts\compat\run.ps1 -Language rust -Target pg-kinetic -Smoke
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
cargo run -p xtask -- compat --list
cargo run -p xtask -- compat --target pg-kinetic --smoke
cargo run -p xtask -- compat-ci --dry-run
cargo run -p xtask -- regression --list
cargo run -p xtask -- bench-validate
cargo run -p xtask -- bench-score
cargo run -p xtask -- docs-check
cargo run -p xtask -- ci-linux
~~~

Regression dispatch preserves its Bash argument form. `compat` passes language,
library, target, category, and smoke filters to the shared compatibility runner.
`bench-score` defaults to the checked-in deterministic sample comparison and
accepts reviewed `--baseline` and `--current` JSON reports for real candidate
runs.

## CI Mapping

The Linux workflow runs the same local entrypoints: `cargo run -p xtask --
ci-linux`, docs checks, regression manifest listing, smoke dry-runs, benchmark
scenario validation, and the deterministic sample score comparison. Pull
requests use the live compatibility smoke matrix. Heavy framework and
ORM suites stay bounded by manual or scheduled compatibility jobs.
