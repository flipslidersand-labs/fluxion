# Map/Reduce Workflows

Fluxion supports multi-level **Map/Reduce** patterns using `foreach:` and `input_from:` job fields.

## Basic Map/Reduce

```yaml
name: map-reduce
jobs:
  fetch:
    component: ./components/hello/hello.wasm
    input: '{"items": ["alpha", "beta", "gamma"]}'

  transform:
    component: ./components/hello/hello.wasm
    foreach: "$.items" # spawns transform.0, transform.1, transform.2
    depends_on: [fetch]

  aggregate:
    component: ./components/hello/hello.wasm
    depends_on: [transform]
    input_from: transform # receives all child outputs as JSON array
    reduce: json_array
```

**Execution order**:

1. `fetch` runs and produces `{"items": ["alpha", "beta", "gamma"]}`
2. `transform.0 / .1 / .2` run in parallel, each receiving one item
3. `aggregate` runs after all children complete, receiving `[output0, output1, output2]`

## Reduce Modes

Set `reduce:` on a fan-in job or pass `--reduce-mode` to the CLI:

| Mode                     | YAML                                               | CLI flag                   | Behaviour                                           |
| ------------------------ | -------------------------------------------------- | -------------------------- | --------------------------------------------------- |
| **json_array** (default) | `reduce: json_array`                               | `--reduce-mode json-array` | Wraps all outputs in a JSON array `[o0, o1, …]`     |
| **concat**               | `reduce: concat`                                   | `--reduce-mode concat`     | Concatenates text outputs into a single JSON string |
| **json_merge**           | `reduce: json_merge`                               | `--reduce-mode json-merge` | Deep-merges JSON objects (last writer wins)         |
| **custom**               | `reduce:` <br>&nbsp;&nbsp;`custom: ./reducer.wasm` | _(not supported via flag)_ | Delegates reduction to a Wasm component             |

### CLI default

Apply a reduce mode to all fan-in jobs that don't specify one explicitly:

```bash
fluxion run examples/map-reduce/workflow.yaml --reduce-mode json-merge
```

## 2-Level Nested Map/Reduce

```yaml
name: nested-mapreduce
jobs:
  source:
    component: ./components/hello/hello.wasm
    input: '{"groups": ["alpha", "beta", "gamma"]}'

  # Level 1: one child per group (dynamic — no input: field)
  process-group:
    component: ./components/hello/hello.wasm
    foreach: "$.groups"
    depends_on: [source]

  # Level 2: expands from the merged output of all process-group.* children
  extract-items:
    component: ./components/hello/hello.wasm
    foreach: "$[*]"
    depends_on: [process-group]
    input_from: process-group # triggers fan-in merge before level-2 expansion

  aggregate:
    component: ./components/hello/hello.wasm
    depends_on: [extract-items]
    input_from: extract-items
```

**Depth limit**: nested foreach chains deeper than **4 levels** are rejected at validation time.

## Fail-Fast Propagation

Set `fail_fast: true` on a foreach job to cancel siblings when any child fails.
This flag is inherited by all dynamically-expanded children.

```yaml
transform:
  component: ./components/hello/hello.wasm
  foreach: "$.items"
  depends_on: [fetch]
  fail_fast: true
```

## Validation

```bash
fluxion validate examples/map-reduce/workflow.yaml
```

The validator checks for:

- Unknown dependencies
- `input_from` pointing to a non-foreach job
- `reduce` without `input_from`
- Foreach nesting depth > 4
- Cyclic dependencies
