"""
Minimal Fluxion Python component.

Reads UTF-8 text from input.content, prepends "Hello from Python: ",
and returns the result as bytes.

Build:
    fluxion build python hello.py -o hello.wasm

Run (standalone):
    fluxion component run hello.wasm --input '{"message":"world"}'

Run as a workflow:
    fluxion run workflow.yaml
"""

from task_component.imports import TaskInput, TaskOutput


def process(input: TaskInput) -> TaskOutput:
    text = bytes(input.content).decode("utf-8", errors="replace")
    result = f"Hello from Python: {text}"
    return TaskOutput(
        content=list(result.encode("utf-8")),
        metadata=[("lang", "python"), ("component", "python-hello")],
    )
