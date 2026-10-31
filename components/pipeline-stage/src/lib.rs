#[allow(warnings)]
mod bindings;

use bindings::exports::fluxion::task::processor::{Guest, TaskInput, TaskOutput};

struct Component;

impl Guest for Component {
    fn process(input: TaskInput) -> Result<TaskOutput, String> {
        let cmd = String::from_utf8(input.content).map_err(|e| e.to_string())?;
        let parts: Vec<&str> = cmd.trim().splitn(3, ':').collect();
        match parts.as_slice() {
            ["fetch", dir] => stage_fetch(dir),
            ["normalize", dir] => stage_normalize(dir),
            ["validate", dir] => stage_validate(dir),
            ["export", src, dst] => stage_export(src, dst),
            _ => Err(format!("Unknown stage command: {}", cmd.trim())),
        }
    }
}

// ── fetch ─────────────────────────────────────────────────────────────────────
// Generates 200 rows of synthetic vehicle data. Row 184 has year=1999 (invalid)
// to demonstrate the validate failure and retry flow.

fn stage_fetch(dir: &str) -> Result<TaskOutput, String> {
    const MAKES: &[(&str, &str)] = &[
        ("Toyota", "Camry"),
        ("Honda", "Civic"),
        ("Ford", "F-150"),
        ("BMW", "3 Series"),
        ("Tesla", "Model 3"),
        ("Nissan", "Altima"),
        ("Chevrolet", "Malibu"),
        ("Hyundai", "Elantra"),
    ];

    let mut csv = String::from("id,make,model,year,vin,mileage\n");
    for i in 1u32..=200 {
        let (make, model) = MAKES[((i - 1) as usize) % MAKES.len()];
        let year = if i == 184 { 1999u32 } else { 2015 + (i % 9) };
        let vin = format!("1HGCM{:011}", i);
        let mileage = 5000 + (i * 1247) % 80000;
        csv.push_str(&format!("{},{},{},{},{},{}\n", i, make, model, year, vin, mileage));
    }

    let path = format!("{}/raw.csv", dir);
    std::fs::write(&path, &csv).map_err(|e| format!("Cannot write {}: {}", path, e))?;

    Ok(TaskOutput {
        content: format!("Wrote 200 rows to {}", path).into_bytes(),
        metadata: vec![("rows".to_string(), "200".to_string())],
    })
}

// ── normalize ─────────────────────────────────────────────────────────────────
// Reads raw.csv, trims whitespace on every field, writes normalized.csv.

fn stage_normalize(dir: &str) -> Result<TaskOutput, String> {
    let raw = format!("{}/raw.csv", dir);
    let out = format!("{}/normalized.csv", dir);

    let src = std::fs::read_to_string(&raw)
        .map_err(|e| format!("Cannot read {}: {}", raw, e))?;

    let normalized: String = src
        .lines()
        .map(|line| {
            line.split(',')
                .map(|f| f.trim())
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";

    let rows = normalized.lines().count().saturating_sub(1); // minus header

    std::fs::write(&out, &normalized)
        .map_err(|e| format!("Cannot write {}: {}", out, e))?;

    Ok(TaskOutput {
        content: format!("Normalized {} rows -> {}", rows, out).into_bytes(),
        metadata: vec![("rows".to_string(), rows.to_string())],
    })
}

// ── validate ──────────────────────────────────────────────────────────────────
// Reads normalized.csv. Fails if any row has year outside 2000-2025.

fn stage_validate(dir: &str) -> Result<TaskOutput, String> {
    let path = format!("{}/normalized.csv", dir);
    let src = std::fs::read_to_string(&path)
        .map_err(|e| format!("Cannot read {}: {}", path, e))?;

    let mut lines = src.lines();
    let _header = lines.next();

    for (i, line) in lines.enumerate() {
        let row = i + 1;
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 4 {
            return Err(format!("malformed row {} (expected 6 fields, got {})", row, fields.len()));
        }
        let year: u32 = fields[3]
            .parse()
            .map_err(|_| format!("invalid registration_year at row {} (not a number)", row))?;

        if !(2000..=2025).contains(&year) {
            return Err(format!(
                "invalid registration_year at row {} (year={}, must be 2000-2025)\n\
                 Fix: sed -i '{}s/{}/2019/' {}\n\
                 Then retry from validate",
                row, year, row + 1, year, path
            ));
        }
    }

    Ok(TaskOutput {
        content: b"All 200 rows valid".to_vec(),
        metadata: vec![("status".to_string(), "valid".to_string())],
    })
}

// ── export ────────────────────────────────────────────────────────────────────
// Copies normalized.csv to the output directory as vehicles.csv.

fn stage_export(src_dir: &str, out_dir: &str) -> Result<TaskOutput, String> {
    let src = format!("{}/normalized.csv", src_dir);
    let dst = format!("{}/vehicles.csv", out_dir);

    let data = std::fs::read(&src)
        .map_err(|e| format!("Cannot read {}: {}", src, e))?;
    let bytes = data.len();

    std::fs::write(&dst, &data)
        .map_err(|e| format!("Cannot write {}: {}", dst, e))?;

    Ok(TaskOutput {
        content: format!("Exported {} bytes -> {}", bytes, dst).into_bytes(),
        metadata: vec![("output".to_string(), dst), ("bytes".to_string(), bytes.to_string())],
    })
}

bindings::export!(Component with_types_in bindings);
