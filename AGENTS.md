# AGENTS.md

Working notes for agents in this repo. `SPEC.md` holds the design; this file holds how the code is written.

`kvfs` is a Redis/Valkey-backed FUSE3 filesystem in Go. `cmd/kvfs` wires flags, `internal/store` owns every Redis call and the key builder, `internal/fs` owns FUSE nodes and errno mapping. Three layers on purpose — do not add a fourth.

## Commands

```bash
gofmt -l .          # must print nothing
go vet ./...
go test ./...
go build -o kvfs ./cmd/kvfs
```

## Errors

- Return errors; never log an error and return it too.
- Add context and wrap: `fmt.Errorf("create %s: %w", name, err)`.
- Match with `errors.Is`/`errors.As`, never by comparing strings.
- `internal/store` owns the sentinels (`ErrNotFound`, `ErrExist`, `ErrNotEmpty`, …); `internal/fs` maps them to errno in one place.
- No `panic` outside a startup invariant.

## Functions

- Guard clauses and early returns; no `else` after a `return`.
- One level of abstraction per function: it decides, or it does I/O — not both.
- Keep functions under roughly 40 lines and nesting under four levels.
- If a block needs a comment explaining *what* it does, extract it and use the comment as the name.
- A boolean parameter that flips behavior is two functions wearing one name.
- `switch` over a closed set of cases, not an if-chain repeating one comparison.

## Types and seams

- Pass the concrete type until a second implementation exists; then declare the smallest interface at the **consumer**, not beside the implementation.
- No interface with one implementation and one caller. No `XxxManager`/`XxxService` that forwards calls.
- Dependencies arrive through the constructor. No package-level mutable state, no `init()`.
- Only the key builder concatenates keys. Everything else passes a `Ref` or calls the builder.

## Spaghetti (flow you cannot follow)

- One entry point, one exit per branch, no phase-shifting loops — iterate once and collect, then act.
- Split a function at the seam where one half would change for a different reason, not at a line count.
- Prefer a data table (levels, suffixes, error mappings) to a chain of special cases.

## Ravioli (layers that only forward)

- Every indirection must earn its keep: a second caller, a test seam that cannot exist otherwise, or a real platform boundary (Redis, FUSE, syscalls).
- Do not add an abstraction to make a unit test pass. The seam already exists: the real client against `miniredis`.
- If a wrapper's body is a single call, delete the wrapper.
- One package per concern, not one package per type.

## Concurrency

- `context.Context` is the first parameter and is never stored in a struct.
- Every goroutine has an owner and a shutdown path; `Close` waits for it.
- Never publish or notify from the FUSE request path — hand it to the background loop, or it deadlocks.

## Tests

- Table-driven; `t.Helper()` in helpers, `t.TempDir()` for paths, no `time.Sleep`.
- Assert through the public surface, and where the spec makes a cost promise (one round trip, constant commands), assert the command count with the instrumented client.
- Anything that needs a real mount is not a unit test; it belongs in the README smoke checklist.

## Dependencies

Standard library first. The toolchain today is `go-fuse`, `rueidis`, and `miniredis` (tests only). Ask before adding another.

## Shared with pykvfs

The Python implementation (github.com/muayyad-alsadi/pykvfs) writes the same keys at `namespace` level. Its scripts are copied byte for byte from `internal/store` into `kvfs/store/lua.py` there. So a change to a script, a key, a publish payload or a round-trip count here is copied to pykvfs in the same change, and pykvfs's `bench/parity.py` is rerun against the harness. Otherwise the two stop being one protocol.

## Done means

- `gofmt -l .` is empty; `go vet ./...` and `go test ./...` are clean.
- New exported identifiers have doc comments; new behavior has a store-seam test.
- No key string outside the key builder, no Redis call outside `internal/store`.
- README and ROADMAP numbers say which run produced them. A round-trip change updates the table in `README.md` and the one in `SPEC.md`.
