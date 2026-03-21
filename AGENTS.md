# Agents

## Pre-commit Checklist

Run these checks before every commit. They mirror the CI pipeline
(`.github/workflows/ci.yml`).

1. **Format** — `cargo fmt --all -- --check`
2. **Lint** — `cargo clippy --all-targets`
3. **Test** — `cargo test --all-targets`

All three must pass with zero warnings (`RUSTFLAGS=-Dwarnings` is set in CI).

### System dependency

The project requires `libfuse3-dev` (`sudo apt-get install libfuse3-dev`) for
compilation.
