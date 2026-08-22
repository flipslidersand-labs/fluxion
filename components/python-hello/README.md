# python-hello

Minimal Python Wasm component sample for Fluxion.

`process(input)` returns `b"hello from python: <input>"`.

## Prerequisites

- Python 3.10+
- componentize-py 0.13+: `pip install componentize-py`
- fluxion CLI built: `cargo build -j2`

## Build

```bash
cd components/python-hello

# Option A — use the fluxion CLI
fluxion build python task.py -o hello.wasm

# Option B — call componentize-py directly
componentize-py componentize \
  --wit-path ../../wit \
  --world task-component \
  task.py -o hello.wasm
```

## Run

```bash
# Run the built component directly
fluxion component run hello.wasm --input "fluxion"
# → hello from python: fluxion

# Or use the convenience script from the repo root
bash scripts/build-python-hello.sh
```

## Inspect

```bash
fluxion inspect hello.wasm
```
