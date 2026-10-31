#[allow(warnings)]
mod bindings;

use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};

use bindings::exports::fluxion::task::processor::{Guest, TaskInput, TaskOutput};

struct Component;

impl Guest for Component {
    fn process(input: TaskInput) -> Result<TaskOutput, String> {
        let addr = String::from_utf8(input.content)
            .map_err(|e| e.to_string())?
            .trim()
            .to_string();

        // `resolve:<host:port>` exercises name resolution (wasi:sockets/ip-name-lookup)
        // instead of a connect; used by sandbox_regression to pin that DNS is denied.
        if let Some(name) = addr.strip_prefix("resolve:") {
            return match name.to_socket_addrs() {
                Ok(addrs) => Ok(TaskOutput {
                    content: format!("Resolved {name}: {} address(es)", addrs.count())
                        .into_bytes(),
                    metadata: vec![("status".to_string(), "resolved".to_string())],
                }),
                Err(e) => Err(format!("Cannot resolve '{name}': {e}")),
            };
        }

        match TcpStream::connect(&addr) {
            Ok(_) => Ok(TaskOutput {
                content: format!("Connected to {addr}").into_bytes(),
                metadata: vec![("status".to_string(), "connected".to_string())],
            }),
            // Reached the network stack (permission was granted); server just not listening.
            Err(e) if e.kind() == ErrorKind::ConnectionRefused => Ok(TaskOutput {
                content: format!("Reached {addr} (no server — permission granted)").into_bytes(),
                metadata: vec![("status".to_string(), "reached".to_string())],
            }),
            Err(e) => Err(format!("Cannot connect to '{addr}': {e}")),
        }
    }
}

bindings::export!(Component with_types_in bindings);
