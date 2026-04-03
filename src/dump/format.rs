// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Output formats for pg_dump: plain, custom, directory, tar.

use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::catalog::{quote_ident, TableInfo};
use super::DumpOptions;

/// Write a `CREATE TABLE` statement to the output buffer.
pub fn write_create_table(out: &mut String, table: &TableInfo) {
    let qname = table.qualified_name();

    out.push_str(&format!("--\n-- Name: {}; Type: TABLE\n--\n\n", table.name));
    out.push_str(&format!("CREATE TABLE {qname} (\n"));

    for (i, col) in table.columns.iter().enumerate() {
        out.push_str(&format!("    {} {}", quote_ident(&col.name), col.type_name));
        if col.not_null {
            out.push_str(" NOT NULL");
        }
        if let Some(ref default) = col.default_expr {
            out.push_str(&format!(" DEFAULT {default}"));
        }
        if i + 1 < table.columns.len() || table.primary_key.is_some() {
            out.push(',');
        }
        out.push('\n');
    }

    if let Some(ref pk) = table.primary_key {
        out.push_str(&format!(
            "    CONSTRAINT {} {}\n",
            quote_ident(&pk.name),
            pk.definition
        ));
    }

    out.push_str(");\n");
}

/// Write table data as COPY or INSERT statements.
pub async fn write_table_data(
    out: &mut String,
    client: &Client,
    table: &TableInfo,
    opts: &DumpOptions,
) -> Result<()> {
    let qname = table.qualified_name();
    let col_names: Vec<String> = table.columns.iter().map(|c| quote_ident(&c.name)).collect();
    let col_list = col_names.join(", ");

    let query = format!("select {col_list} from {qname} order by 1");
    let rows = client
        .query(&query, &[])
        .await
        .with_context(|| format!("query data from {qname}"))?;

    if rows.is_empty() {
        return Ok(());
    }

    if opts.inserts {
        write_inserts(out, table, &rows, opts)?;
    } else {
        write_copy(out, table, &rows)?;
    }

    Ok(())
}

/// Write data as COPY ... FROM stdin.
fn write_copy(out: &mut String, table: &TableInfo, rows: &[tokio_postgres::Row]) -> Result<()> {
    let qname = table.qualified_name();
    let col_names: Vec<String> = table.columns.iter().map(|c| quote_ident(&c.name)).collect();
    let col_list = col_names.join(", ");

    out.push_str(&format!(
        "--\n-- Data for Name: {}; Type: TABLE DATA\n--\n\n",
        table.name
    ));
    out.push_str(&format!("COPY {qname} ({col_list}) FROM stdin;\n"));

    for row in rows {
        let mut values = Vec::new();
        for (i, _col) in table.columns.iter().enumerate() {
            let val = format_copy_value(row, i);
            values.push(val);
        }
        out.push_str(&values.join("\t"));
        out.push('\n');
    }

    out.push_str("\\.\n");
    Ok(())
}

/// Write data as INSERT statements.
fn write_inserts(
    out: &mut String,
    table: &TableInfo,
    rows: &[tokio_postgres::Row],
    opts: &DumpOptions,
) -> Result<()> {
    let qname = table.qualified_name();
    let col_names: Vec<String> = table.columns.iter().map(|c| quote_ident(&c.name)).collect();

    out.push_str(&format!(
        "--\n-- Data for Name: {}; Type: TABLE DATA\n--\n\n",
        table.name
    ));

    let prefix = if opts.column_inserts {
        format!("INSERT INTO {qname} ({}) VALUES", col_names.join(", "))
    } else {
        format!("INSERT INTO {qname} VALUES")
    };

    let rows_per = opts.rows_per_insert.unwrap_or(1) as usize;

    for chunk in rows.chunks(rows_per) {
        out.push_str(&prefix);
        for (ci, row) in chunk.iter().enumerate() {
            if ci > 0 {
                out.push(',');
            }
            out.push_str(" (");
            for (i, _col) in table.columns.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                let val = format_insert_value(row, i);
                out.push_str(&val);
            }
            out.push(')');
        }
        out.push_str(";\n");
    }

    Ok(())
}

/// Format a single value for COPY output (tab-separated, `\N` for NULL).
fn format_copy_value(row: &tokio_postgres::Row, idx: usize) -> String {
    // Try common types; fall back to text representation.
    if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
        match v {
            Some(s) => escape_copy_value(&s),
            None => "\\N".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "\\N".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "\\N".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "\\N".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
        match v {
            Some(b) => if b { "t" } else { "f" }.to_string(),
            None => "\\N".to_string(),
        }
    } else {
        "\\N".to_string()
    }
}

/// Escape special characters in a COPY value.
fn escape_copy_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// Format a single value for INSERT output (SQL literals).
fn format_insert_value(row: &tokio_postgres::Row, idx: usize) -> String {
    if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
        match v {
            Some(s) => format!("'{}'", s.replace('\'', "''")),
            None => "NULL".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
        match v {
            Some(n) => n.to_string(),
            None => "NULL".to_string(),
        }
    } else if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
        match v {
            Some(b) => if b { "true" } else { "false" }.to_string(),
            None => "NULL".to_string(),
        }
    } else {
        "NULL".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_copy_value_basic() {
        assert_eq!(escape_copy_value("hello"), "hello");
        assert_eq!(escape_copy_value("a\tb"), "a\\tb");
        assert_eq!(escape_copy_value("a\\b"), "a\\\\b");
        assert_eq!(escape_copy_value("a\nb"), "a\\nb");
    }
}
