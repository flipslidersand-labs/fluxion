from dataclasses import dataclass, field


@dataclass
class TaskInput:
    content: bytes = b""
    metadata: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class TaskOutput:
    content: bytes = b""
    metadata: list[tuple[str, str]] = field(default_factory=list)


class Processor:
    def process(self, input: TaskInput) -> TaskOutput:
        name = input.content.decode("utf-8", errors="replace").strip() or "world"
        return TaskOutput(content=f"hello from python: {name}".encode())
