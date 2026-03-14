//! Human-friendly table/key-value renderer for serde_json::Value.
//!
//! - Array of objects -> columnar table with headers
//! - Single object -> aligned key: value pairs
//! - ActionResult-like -> single status line
//! - Scalars -> plain text

use std::io::Write;

const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const GREEN: &str = "\x1b[32m";
#[allow(dead_code)]
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const RESET: &str = "\x1b[0m";
const MAX_COL_WIDTH: usize = 50;
const MAX_COLS: usize = 10;

pub fn render<W: Write>(mut w: W, value: &serde_json::Value) -> anyhow::Result<()> {
    match value {
        serde_json::Value::Array(arr) if arr.is_empty() => {
            writeln!(w, "{DIM}(no results){RESET}")?;
        }
        serde_json::Value::Array(arr)
            if arr.len() == 1
                && arr[0].is_object()
                && !is_action_result(arr[0].as_object().unwrap()) =>
        {
            render_object(&mut w, arr[0].as_object().unwrap())?;
        }
        serde_json::Value::Array(arr) if arr.iter().all(|v| v.is_object()) => {
            render_table(&mut w, arr)?;
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                writeln!(w, "  {}", format_scalar(item))?;
            }
        }
        serde_json::Value::Object(obj) => {
            if is_action_result(obj) {
                render_action_result(&mut w, obj)?;
            } else {
                render_object(&mut w, obj)?;
            }
        }
        other => {
            writeln!(w, "{}", format_scalar(other))?;
        }
    }
    Ok(())
}

fn render_table<W: Write>(w: &mut W, items: &[serde_json::Value]) -> anyhow::Result<()> {
    let first = items[0].as_object().unwrap();
    let columns: Vec<&String> = first.keys().take(MAX_COLS).collect();

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for item in items {
        if let Some(obj) = item.as_object() {
            for (i, col) in columns.iter().enumerate() {
                let val = obj.get(*col).map(format_cell).unwrap_or_default();
                widths[i] = widths[i].max(val.len()).min(MAX_COL_WIDTH);
            }
        }
    }

    // Header
    let header: String = columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let label = col.to_uppercase().replace('_', " ");
            format!("{label:<width$}", width = widths[i])
        })
        .collect::<Vec<_>>()
        .join("  ");
    writeln!(w, "{BOLD}{header}{RESET}")?;

    // Separator
    let sep: String = widths
        .iter()
        .map(|w| "\u{2500}".repeat(*w))
        .collect::<Vec<_>>()
        .join("\u{2500}\u{2500}");
    writeln!(w, "{DIM}{sep}{RESET}")?;

    // Rows
    for item in items {
        if let Some(obj) = item.as_object() {
            let row: String = columns
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let val = obj.get(*col).map(format_cell).unwrap_or_default();
                    let truncated = truncate(&val, widths[i]);
                    format!("{truncated:<width$}", width = widths[i])
                })
                .collect::<Vec<_>>()
                .join("  ");
            writeln!(w, "{row}")?;
        }
    }

    writeln!(w, "{DIM}{} items{RESET}", items.len())?;
    Ok(())
}

fn render_object<W: Write>(
    w: &mut W,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<()> {
    let max_key_len = obj.keys().map(|k| k.len()).max().unwrap_or(0);

    for (key, value) in obj {
        let label = key.replace('_', " ");
        let val_str = match value {
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    format!("{DIM}(none){RESET}")
                } else if arr.iter().all(|v| v.is_string() || v.is_number()) {
                    arr.iter().map(format_scalar).collect::<Vec<_>>().join(", ")
                } else {
                    format!("[{} items]", arr.len())
                }
            }
            serde_json::Value::Object(inner) => {
                let parts: Vec<String> = inner
                    .iter()
                    .take(5)
                    .map(|(k, v)| format!("{k}: {}", format_scalar(v)))
                    .collect();
                parts.join(", ")
            }
            other => format_scalar(other),
        };

        writeln!(
            w,
            "{BOLD}{label:>width$}{RESET}  {val_str}",
            width = max_key_len
        )?;
    }
    Ok(())
}

fn render_action_result<W: Write>(
    w: &mut W,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<()> {
    let ok = obj.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
    let action = obj.get("action").and_then(|v| v.as_str()).unwrap_or("done");
    let icon = if ok {
        format!("{GREEN}\u{2713}{RESET}")
    } else {
        format!("{YELLOW}\u{2717}{RESET}")
    };

    write!(w, "{icon} {BOLD}{action}{RESET}")?;

    if let Some(id) = obj.get("id").and_then(|v| v.as_str()) {
        if !id.is_empty() {
            write!(w, " {DIM}({id}){RESET}")?;
        }
    }
    if let Some(msg) = obj.get("message").and_then(|v| v.as_str()) {
        if !msg.is_empty() {
            write!(w, " \u{2014} {msg}")?;
        }
    }
    writeln!(w)?;
    Ok(())
}

fn is_action_result(obj: &serde_json::Map<String, serde_json::Value>) -> bool {
    obj.contains_key("ok") && obj.contains_key("action")
}

#[allow(dead_code)]
pub fn format_dollar(amount: f64) -> String {
    let abs = amount.abs();
    let formatted = if abs >= 1_000_000.0 {
        format!("${:.1}M", abs / 1_000_000.0)
    } else if abs >= 1_000.0 {
        // Add commas
        let whole = abs as i64;
        let frac = abs - whole as f64;
        let s = format!("{whole}");
        let mut with_commas = String::new();
        for (i, c) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                with_commas.push(',');
            }
            with_commas.push(c);
        }
        let with_commas: String = with_commas.chars().rev().collect();
        format!("${with_commas}.{:02}", (frac * 100.0).round() as i64)
    } else {
        format!("${abs:.2}")
    };

    if amount < 0.0 {
        format!("{RED}-{formatted}{RESET}")
    } else {
        formatted
    }
}

fn format_cell(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => {
            if *b {
                format!("{GREEN}\u{2713}{RESET}")
            } else {
                "\u{2717}".to_string()
            }
        }
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 && f.abs() < 1_000_000.0 {
                    format!("{}", f as i64)
                } else {
                    format!("{f:.2}")
                }
            } else {
                n.to_string()
            }
        }
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => format!("[{}]", arr.len()),
        serde_json::Value::Object(_) => "{\u{2026}}".to_string(),
    }
}

fn format_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => format!("{DIM}null{RESET}"),
        serde_json::Value::Bool(true) => format!("{GREEN}true{RESET}"),
        serde_json::Value::Bool(false) => "false".to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => format!("[{} items]", arr.len()),
        serde_json::Value::Object(obj) => format!("{{{} keys}}", obj.len()),
    }
}

fn truncate(s: &str, max: usize) -> String {
    let visible_len = strip_ansi_len(s);
    if visible_len <= max {
        s.to_string()
    } else {
        let mut visible = 0;
        let mut byte_pos = 0;
        let mut in_escape = false;
        for (i, c) in s.char_indices() {
            if c == '\x1b' {
                in_escape = true;
            } else if in_escape {
                if c.is_ascii_alphabetic() {
                    in_escape = false;
                }
            } else {
                visible += 1;
                if visible >= max.saturating_sub(1) {
                    byte_pos = i + c.len_utf8();
                    break;
                }
            }
            byte_pos = i + c.len_utf8();
        }
        format!("{}\u{2026}", &s[..byte_pos])
    }
}

fn strip_ansi_len(s: &str) -> usize {
    let mut len = 0;
    let mut in_escape = false;
    for c in s.chars() {
        if c == '\x1b' {
            in_escape = true;
        } else if in_escape {
            if c.is_ascii_alphabetic() {
                in_escape = false;
            }
        } else {
            len += 1;
        }
    }
    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_array_of_objects() {
        let value = serde_json::json!([
            {"name": "Alice", "balance": 1000.0},
            {"name": "Bob", "balance": 2000.0},
        ]);
        let mut buf = Vec::new();
        render(&mut buf, &value).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("NAME"));
        assert!(output.contains("Alice"));
        assert!(output.contains("2 items"));
    }

    #[test]
    fn test_render_empty_array() {
        let value = serde_json::json!([]);
        let mut buf = Vec::new();
        render(&mut buf, &value).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("no results"));
    }

    #[test]
    fn test_format_dollar() {
        assert_eq!(format_dollar(1234.56), "$1,234.56");
        assert_eq!(format_dollar(999.99), "$999.99");
    }

    #[test]
    fn test_format_dollar_millions() {
        assert_eq!(format_dollar(1_500_000.0), "$1.5M");
    }

    #[test]
    fn test_truncate_short() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_long() {
        let result = truncate("a very long string here", 10);
        assert!(result.ends_with('\u{2026}'));
    }
}
