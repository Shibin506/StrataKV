# Phase 6: figures, docs and polish

## What changed

- **Figures** (`docs/figures/*.svg`), drawn by `bench/figures.py` straight from the benchmark results in
  `bench/results/*.json`, with no dependencies beyond Python 3. Re-running `bench/run.sh` and then
  `python3 bench/figures.py` redraws them from new numbers. They follow the viewer's light or dark setting.
  Each engine keeps one colour in every figure, and the colours were checked as a set for colour-blind
  safety in both modes. Every bar carries its value as text, and the same numbers are in tables, so nothing
  depends on telling colours apart.
- **README** reorganised for a first-time reader: what it is and what it promises, the figure, how to use it, how
  fast it is, how it works, what the tests prove, the design decisions and their costs, the code layout, and
  every known limit in one place.
- **The README example is a doctest** (`src/lib.rs`), so `cargo test` fails if it ever stops compiling or
  stops being true.
- **CI** (`.github/workflows/ci.yml`): formatting, clippy with warnings as errors, all tests, and the API docs
  with warnings as errors, on Linux and macOS for every push and pull request.
- **Package metadata** in `Cargo.toml` (description, repository, minimum Rust version 1.89, the first with
  `File::try_lock`).

## The whole project in numbers

| | |
|---|---|
| Code | about 4,000 lines of Rust (`src/`), plus the benchmark (`bench/`) |
| Tests | 81: 40 unit tests, 40 integration tests, 1 doctest, including 4 property tests and 2 crash tests that kill a real process |
| Crash points | 10 named steps of flushes and compactions, each crashed at in both sync modes |
| Mutation checks | every rule added in Phases 2 to 5 was broken on purpose at least once to confirm a test catches it |
