# Security Policy

## Supported Versions

Security fixes target the current `main` branch and the latest published
release. Older releases may receive fixes when the change is low risk and
applies cleanly.

## Report A Vulnerability

Report security issues privately through GitHub Security Advisories:

https://github.com/HookWoods/pg-kinetic/security/advisories/new

Do not open a public issue for vulnerabilities, credentials, private keys,
customer data, or exploit details.

Please include:

- affected version, commit, container tag, or chart version
- deployment method and relevant configuration with secrets removed
- steps to reproduce
- expected and observed behavior
- logs, admin output, metrics, or packet details that are safe to share

## Scope

Security-sensitive areas include:

- PostgreSQL wire-protocol parsing and forwarding
- TLS and authentication behavior
- session reuse, reset, and pinning logic
- admin, health, and metrics endpoints
- container, Helm, and CI release artifacts
- handling of credentials and secret-bearing config

## Disclosure

The project will coordinate a fix and public disclosure after the issue is
understood and a patch is available. Credit is welcome when the reporter wants
to be named.
