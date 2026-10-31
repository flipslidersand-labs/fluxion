//! Generate Python stub files from `wit/task.wit` for componentize-py.
//!
//! Parses the `processor` interface in WIT, converts record fields from
//! kebab-case to snake_case, and emits a `task.py` stub with dataclasses and
//! a default `process` implementation.
//!
//! Wired up to the CLI via `fluxion build python` (#115).

#![allow(dead_code)]

use anyhow::Result;
use std::path::Path;

/// Convert a WIT kebab-case identifier to Python snake_case.
fn kebab_to_snake(s: &str) -> String {
    s.replace('-', "_")
}

/// Parse WIT source and extract record fields for a named record.
///
/// Returns a list of `(field_name, wit_type)` pairs in declaration order.
/// Only handles the simple types used in `task.wit`; unknown types pass through.
fn extract_record_fields(wit: &str, record_name: &str) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    let marker = format!("record {} {{", record_name);
    let Some(start) = wit.find(&marker) else {
        return fields;
    };
    let body_start = start + marker.len();
    let Some(end) = wit[body_start..].find('}') else {
        return fields;
    };
    let body = &wit[body_start..body_start + end];

    for line in body.lines() {
        let line = line.trim().trim_end_matches(',');
        if line.is_empty() {
            continue;
        }
        if let Some((name, ty)) = line.split_once(':') {
            fields.push((kebab_to_snake(name.trim()), ty.trim().to_string()));
        }
    }
    fields
}

/// Map a WIT type string to a Python type annotation.
fn wit_type_to_python(wit_type: &str) -> &str {
    match wit_type.trim() {
        "list<u8>" => "bytes",
        "list<tuple<string, string>>" => "list[tuple[str, str]]",
        "string" => "str",
        "u8" | "u16" | "u32" | "u64" | "s8" | "s16" | "s32" | "s64" => "int",
        "f32" | "f64" => "float",
        "bool" => "bool",
        _ => "object",
    }
}

/// Default Python value for a WIT type.
fn wit_type_default(wit_type: &str) -> &str {
    match wit_type.trim() {
        "list<u8>" => "b\"\"",
        "list<tuple<string, string>>" => "[]",
        "string" => "\"\"",
        "u8" | "u16" | "u32" | "u64" | "s8" | "s16" | "s32" | "s64" => "0",
        "f32" | "f64" => "0.0",
        "bool" => "False",
        _ => "None",
    }
}

/// Generate a Python stub from the WIT source at `wit_path`.
///
/// Reads `wit/task.wit`, extracts `task-input` and `task-output` records,
/// and emits a Python module with:
/// - `from __future__ import annotations`
/// - `@dataclass` definitions for `TaskInput` and `TaskOutput`
/// - A default `process(input: TaskInput) -> TaskOutput` implementation
pub fn generate(wit_path: &Path) -> Result<String> {
    let wit = std::fs::read_to_string(wit_path)?;

    let input_fields = extract_record_fields(&wit, "task-input");
    let output_fields = extract_record_fields(&wit, "task-output");

    let mut out = String::new();
    out.push_str("from __future__ import annotations\n");
    out.push_str("from dataclasses import dataclass, field\n");
    out.push_str("from typing import Optional\n\n\n");

    // TaskInput dataclass
    out.push_str("@dataclass\n");
    out.push_str("class TaskInput:\n");
    if input_fields.is_empty() {
        out.push_str("    pass\n");
    } else {
        for (name, wit_ty) in &input_fields {
            let py_ty = wit_type_to_python(wit_ty);
            let default = wit_type_default(wit_ty);
            if default.starts_with('[') {
                out.push_str(&format!(
                    "    {name}: {py_ty} = field(default_factory=list)\n"
                ));
            } else {
                out.push_str(&format!("    {name}: {py_ty} = {default}\n"));
            }
        }
    }
    out.push('\n');
    out.push('\n');

    // TaskOutput dataclass
    out.push_str("@dataclass\n");
    out.push_str("class TaskOutput:\n");
    if output_fields.is_empty() {
        out.push_str("    pass\n");
    } else {
        for (name, wit_ty) in &output_fields {
            let py_ty = wit_type_to_python(wit_ty);
            let default = wit_type_default(wit_ty);
            if default.starts_with('[') {
                out.push_str(&format!(
                    "    {name}: {py_ty} = field(default_factory=list)\n"
                ));
            } else {
                out.push_str(&format!("    {name}: {py_ty} = {default}\n"));
            }
        }
    }
    out.push('\n');
    out.push('\n');

    // Default process implementation
    out.push_str("def process(input: TaskInput) -> TaskOutput:\n");
    out.push_str("    \"\"\"Default implementation — override with your logic.\"\"\"\n");
    out.push_str("    return TaskOutput()\n");

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile::NamedTempFile;

    fn sample_wit() -> &'static str {
        r#"
package fluxion:task@0.1.0;

interface processor {
    record task-input {
        content:  list<u8>,
        metadata: list<tuple<string, string>>,
    }

    record task-output {
        content:  list<u8>,
        metadata: list<tuple<string, string>>,
    }

    process: func(input: task-input) -> result<task-output, string>;
}

world task-component {
    export processor;
}
"#
    }

    fn generate_from_str(wit: &str) -> String {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(wit.as_bytes()).unwrap();
        generate(f.path()).unwrap()
    }

    #[test]
    fn contains_dataclass_imports() {
        let out = generate_from_str(sample_wit());
        assert!(out.contains("from dataclasses import dataclass"));
    }

    #[test]
    fn task_input_class_generated() {
        let out = generate_from_str(sample_wit());
        assert!(out.contains("class TaskInput"));
    }

    #[test]
    fn task_output_class_generated() {
        let out = generate_from_str(sample_wit());
        assert!(out.contains("class TaskOutput"));
    }

    #[test]
    fn kebab_fields_converted_to_snake_case() {
        let out = generate_from_str(sample_wit());
        assert!(out.contains("content"));
        assert!(out.contains("metadata"));
        // No kebab-case field names in output
        assert!(!out.contains("task-input"));
        assert!(!out.contains("task-output"));
    }

    #[test]
    fn process_function_stub_present() {
        let out = generate_from_str(sample_wit());
        assert!(out.contains("def process(input: TaskInput) -> TaskOutput:"));
    }

    #[test]
    fn list_fields_use_field_factory() {
        let out = generate_from_str(sample_wit());
        assert!(out.contains("field(default_factory=list)"));
    }

    #[test]
    fn kebab_to_snake_conversion() {
        assert_eq!(kebab_to_snake("task-input"), "task_input");
        assert_eq!(kebab_to_snake("my-field-name"), "my_field_name");
        assert_eq!(kebab_to_snake("already_snake"), "already_snake");
    }
}
