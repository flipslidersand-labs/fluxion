# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/) (`0.y.z` until 1.0.0).
No version has been released yet; all crates are at `0.1.0`.

## [Unreleased]

### Added

- Initial public snapshot: Wasm component runtime and CLI, YAML workflows with a DAG scheduler,
  capability sandbox (filesystem / network / limits), SQLite persistence with retry from any job,
  OpenTelemetry tracing, and an MCP server with 5 tools.
- Release workflow that builds prebuilt binaries and a demo Wasm component on `v*` tags (#2).
- Quick start for the prebuilt package in the README (#5).

### Changed

- Release builds run on ubuntu-22.04 and check the maximum required glibc version (#7).
- Release caches are split per OS image, and `cargo-component` is detected by whether it runs (#8).
- README requirements now state the glibc version that was measured (#9).

### Fixed

- `fire_due_schedules` claim test no longer flakes at minute boundaries (#4).
- Reusable workflow pins point to the equivalent commit on qa-workflows `main` (#10).
