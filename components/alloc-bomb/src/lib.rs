#[allow(warnings)]
mod bindings;

use bindings::exports::fluxion::task::processor::{Guest, TaskInput, TaskOutput};

struct Component;

impl Guest for Component {
    fn process(input: TaskInput) -> Result<TaskOutput, String> {
        let mb: usize = String::from_utf8(input.content)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(64);

        let size = mb * 1024 * 1024;

        // Allocate and write to force actual page faults (not just reserved VA).
        // When memory_mb in the host limits is smaller than `mb`, the Wasm
        // memory.grow call inside the allocator fails and Rust panics with
        // "memory allocation failed" — caught by StoreLimits.
        let mut data = vec![0u8; size];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        Ok(TaskOutput {
            content: format!("Allocated {}MB ok", mb).into_bytes(),
            metadata: vec![
                ("mb".to_string(), mb.to_string()),
                ("checksum".to_string(), data.iter().map(|b| *b as u64).sum::<u64>().to_string()),
            ],
        })
    }
}

bindings::export!(Component with_types_in bindings);
