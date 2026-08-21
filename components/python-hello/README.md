# python-hello

Minimal Fluxion Python component example.

## Prerequisites

```bash
pip install componentize-py
```

## Build

```bash
fluxion build python hello.py -o hello.wasm
```

## Run

```bash
# Single component
fluxion component run hello.wasm --input 'world'

# As a workflow
fluxion run workflow.yaml
```

## How it works

`hello.py` implements the `fluxion:task/processor` WIT interface — a single
`process(input: TaskInput) -> TaskOutput` function.  `componentize-py`
compiles it to a Wasm component that Fluxion can execute in a sandbox.
