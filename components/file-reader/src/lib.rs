#[allow(warnings)]
mod bindings;

use bindings::exports::fluxion::task::processor::{Guest, TaskInput, TaskOutput};

struct Component;

impl Guest for Component {
    fn process(input: TaskInput) -> Result<TaskOutput, String> {
        // input.content is the file path to read
        let path = String::from_utf8(input.content)
            .map_err(|e| e.to_string())?;
        let path = if path.trim().is_empty() {
            "/data/input.txt".to_string()
        } else {
            path.trim().to_string()
        };

        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                let msg = format!("Read {} bytes from {}", contents.len(), path);
                Ok(TaskOutput {
                    content: msg.into_bytes(),
                    metadata: vec![
                        ("file".to_string(), path),
                        ("status".to_string(), "ok".to_string()),
                    ],
                })
            }
            Err(e) => Err(format!("Cannot read '{}': {}", path, e)),
        }
    }
}

bindings::export!(Component with_types_in bindings);
