# Agents: Working with TiKV

## Mission & Scope
- Provide AI agents and other automation a fast orientation to the TiKV repository.
- Highlight critical workflows, testing expectations, and style constraints that gate pull requests.
- Surface references to deeper documentation when a change spans multiple subsystems (raftstore, storage, PD integration).

## Repository Tour
- `src/`: Main TiKV server implementation. Hot spots include `server/`, `raftstore/`, `storage/`, and `coprocessor/`.
- `components/`: Independent crates reused across TiKV (e.g. concurrency manager, engine abstractions). Changes here often affect multiple binaries.
- `tests/`: Integration and failpoint-heavy regression suites. Prefer targeted runs over the full matrix when iterating locally.
- `scripts/`: Tooling wrappers. Use `./scripts/env` to mirror CI build environments when invoking `cargo` directly.
- `doc/`, `metrics/`, `etc/`: Operations guides, Grafana dashboards, and configuration templates. Update alongside behavioral changes.

## Common Workflows
- Build with CI-aligned flags: `make build`. For quick feedback use `./scripts/env cargo check --all`.
- Developer gate before PRs: `make dev` (formats, runs clippy, executes unit tests).
- Format only: `make format`. Lint only: `make clippy`.
- Clean incremental state when toggling features: `make clean` or `cargo clean`.
- When adjusting protobuf or RocksDB dependencies, run through `./scripts/` helpers instead of calling tooling directly.

## Testing Guidance
- Unit and integration tests: `make test`. Target a module with `env EXTRA_CARGO_ARGS=<pattern> make test`.
- Nextest is available for faster iteration: `env EXTRA_CARGO_ARGS=<pattern> make test_with_nextest`.
- Expensive suites live under `tests/integration/` and may require PD/TiDB components; mark them for manual verification when CI coverage is sufficient.
- Watch for flakiness in failpoint-driven tests (documented intermittents exist); rerun inside `./scripts/env` before retrying in CI.

## Coding Standards & Review Expectations
- Follow Rust nightly toolchain pinned via `rust-toolchain.toml`. Install `rustfmt` and `clippy` components.
- Formatting and linting are enforced; run locally before sending patches.
- Refer to `CODE_COMMENT_STYLE.md` for conventions on inline explanations and TODOs.
- Consult `PERFORMANCE_CRITICAL_PATH.md` before modifying hot code (scheduler, raftstore, coprocessor). Provide benchmarks or reasoning for any algorithmic changes.
- Prefer incremental, well-scoped patches. Large mechanical refactors should include a note referencing prior discussion or design docs.

## Coordination & Further Reading
- Architecture overview: `README.md` and https://tikv.org/docs/latest/concepts/overview/.
- Development process and prerequisites: `CONTRIBUTING.md`.
- Security policy: `SECURITY.md`.
- Community support: https://tikv.org/chat.
- When uncertain about subsystem ownership, check `OWNERS` and `OWNERS_ALIASES`.

## Agent Checklist Before Exiting
- [ ] Code compiles (`make build` or `cargo check --all` via `./scripts/env`).
- [ ] Formatting and linting pass (`make format`, `make clippy`).
- [ ] Relevant tests executed or explicitly deferred with rationale.
- [ ] Documentation/config updates accompany behavioral changes.
- [ ] Notes left for humans when further manual validation or deployment steps are required.
