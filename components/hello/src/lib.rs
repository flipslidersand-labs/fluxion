#[allow(warnings)]
mod bindings;

use bindings::exports::fluxion::task::processor::{Guest, TaskInput, TaskOutput};

struct Component;

impl Guest for Component {
    fn process(input: TaskInput) -> Result<TaskOutput, String> {
        let msg = format!(
            "Hello from Fluxion! Received {} bytes.",
            input.content.len()
        );
        Ok(TaskOutput {
            content: msg.into_bytes(),
            metadata: vec![("source".to_string(), "hello-component".to_string())],
        })
    }
}

bindings::export!(Component with_types_in bindings);
