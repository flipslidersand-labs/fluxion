#[allow(warnings)]
mod bindings;

use bindings::exports::fluxion::task::processor::{Guest, TaskInput, TaskOutput};

struct Component;

impl Guest for Component {
    fn process(input: TaskInput) -> Result<TaskOutput, String> {
        // Parse iteration count from input; default to u64::MAX (spin forever).
        let n: u64 = String::from_utf8(input.content)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(u64::MAX);

        // CPU-bound busy loop. Wasm compiles loop back-edges with epoch check
        // points, so the host's epoch ticker will interrupt this at the deadline.
        let mut acc: u64 = 0;
        for i in 0..n {
            acc = acc.wrapping_add(i);
        }

        Ok(TaskOutput {
            content: format!("sum={}", acc).into_bytes(),
            metadata: vec![("iterations".to_string(), n.to_string())],
        })
    }
}

bindings::export!(Component with_types_in bindings);
