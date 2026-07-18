# Contributing

Thanks for helping improve pg-kinetic.

## Development Workflow

1. Open an issue for behavioral changes before writing a large patch.
2. Keep pull requests focused on one problem.
3. Add or update tests for protocol, pooling, routing, compatibility, and deployment behavior.
4. Run the focused checks for the files you changed.
5. Include enough context in the pull request for reviewers to reproduce the change.

## Local Checks

Use the narrowest check that covers your change first:

```bash
cargo test -p pg-kinetic --test <test-name>
```

Before a release-sensitive change, also run:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

On Windows, if the linker runs out of resources during a broad workspace check,
retry with serialized Cargo jobs:

```powershell
$env:CARGO_BUILD_JOBS = "1"
cargo test --workspace
```

## Documentation

Update documentation when a change affects:

- installation or release commands
- configuration fields or defaults
- admin views, metrics, or health endpoints
- compatibility, regression, or benchmark workflows
- Kubernetes, Helm, Docker, or CI behavior

Run the docs checks from the repository root:

```bash
bash scripts/docs/check-links.sh
bash scripts/docs/check-config-coverage.sh
```

On Windows:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/docs/check-links.ps1
powershell.exe -ExecutionPolicy Bypass -File scripts/docs/check-config-coverage.ps1
```

## License

pg-kinetic is licensed under either Apache-2.0 or MIT, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in pg-kinetic is licensed as Apache-2.0 OR MIT, without any
additional terms or conditions.
