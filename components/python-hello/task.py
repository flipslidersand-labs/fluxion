"""
Minimal Python component for fluxion.

Build with:
    fluxion build python task.py -o hello.wasm

Run with:
    fluxion component run hello.wasm
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Tuple


@dataclass
class TaskInput:
    content: List[int] = field(default_factory=list)
    metadata: List[Tuple[str, str]] = field(default_factory=list)


@dataclass
class TaskOutput:
    content: List[int] = field(default_factory=list)
    metadata: List[Tuple[str, str]] = field(default_factory=list)


class Processor:
    def process(self, input: TaskInput) -> TaskOutput:
        message = b"hello from python"
        return TaskOutput(content=list(message))
