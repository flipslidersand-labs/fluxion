#[allow(warnings)]
mod bindings;

use std::fs::{self, OpenOptions};
use std::io::Write;

use bindings::exports::fluxion::task::processor::{Guest, TaskInput, TaskOutput};

struct Component;

/// Filesystem sandbox probe used by the host's sandbox regression tests.
///
/// (Symlinks are pre-created by the host test: `std::os::wasi::fs::symlink_path` is
/// unstable.)
///
/// Input is one operation per call, whitespace-separated: `<op> <arg> [<arg2>]`.
/// The component never returns `Err` for a failed filesystem call; it reports the
/// outcome as `ok[:<detail>]` or `err:<ErrorKind>` so the host test can tell a
/// sandbox rejection apart from a trap.
///
///   read <path>            read the whole file
///   list <path>            list a directory
///   truncate <path>        open write+truncate (no create)
///   create <path>          create_new + write a marker
///   append <path>          open append and write a marker
///   hardlink <src> <dst>   std::fs::hard_link
///   rename <src> <dst>     std::fs::rename
///   remove <path>          std::fs::remove_file
///   mkdir <path>           std::fs::create_dir
impl Guest for Component {
    fn process(input: TaskInput) -> Result<TaskOutput, String> {
        let line = String::from_utf8(input.content).map_err(|e| e.to_string())?;
        let mut it = line.split_whitespace();
        let op = it.next().ok_or("empty op")?;
        let a = it.next().ok_or("missing arg")?;
        let b = it.next();

        let res: std::io::Result<String> = match (op, b) {
            ("read", _) => fs::read_to_string(a),
            ("list", _) => fs::read_dir(a).map(|d| {
                d.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join(",")
            }),
            ("truncate", _) => OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(a)
                .map(|_| String::new()),
            ("create", _) => OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(a)
                .and_then(|mut f| f.write_all(b"probe"))
                .map(|_| String::new()),
            ("append", _) => OpenOptions::new()
                .append(true)
                .open(a)
                .and_then(|mut f| f.write_all(b"probe"))
                .map(|_| String::new()),
            ("hardlink", Some(dst)) => fs::hard_link(a, dst).map(|_| String::new()),
            ("rename", Some(dst)) => fs::rename(a, dst).map(|_| String::new()),
            ("remove", _) => fs::remove_file(a).map(|_| String::new()),
            ("mkdir", _) => fs::create_dir(a).map(|_| String::new()),
            _ => return Err(format!("bad op line: {line}")),
        };

        let out = match res {
            Ok(d) => format!("ok:{d}"),
            Err(e) => format!("err:{:?}", e.kind()),
        };
        Ok(TaskOutput {
            content: out.into_bytes(),
            metadata: vec![],
        })
    }
}

bindings::export!(Component with_types_in bindings);
