# Agents

## Pre-PR Checklist

Before opening a pull request, ensure:

1. `cargo fmt` — code is formatted
2. `cargo clippy --all-targets` — no warnings
3. `cargo test` — all unit tests pass
4. `cargo test --test rustfs_integ_test` — all integration tests pass (requires Docker)
