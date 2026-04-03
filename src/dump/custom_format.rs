// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! PostgreSQL custom archive format writer.
//!
//! Implements a compatible subset of PostgreSQL's custom archive format
//! (format byte = 3), consisting of:
//!   - PGDMP magic header
//!   - TOC entries (schema DDL + data references)
//!   - Compressed data blocks (zlib)
//!
//! Reference: https://github.com/postgres/postgres/blob/master/src/bin/pg_dump/pg_backup_archiver.c

use std::io::{self, Write};

use flate2::{write::ZlibEncoder, Compression};

// ──────────────────────────────────────────────────────────────────────────────
// Format constants (from pg_backup_archiver.h)
// ──────────────────────────────────────────────────────────────────────────────

/// "PGDMP" magic bytes.
pub const MAGIC: &[u8] = b"PGDMP";

/// Custom archive format identifier.
pub const FMT_CUSTOM: u8 = 3;

/// Archive format version supported by this writer (1.15.0).
pub const VERSION_MAJOR: u8 = 1;
pub const VERSION_MINOR: u8 = 15;
pub const VERSION_REV: u8 = 0;

/// Number of bytes used for integers in the archive (4).
pub const INT_SIZE: u8 = 4;

/// Number of bytes used for file offsets (8).
pub const OFF_SIZE: u8 = 8;

// Section constants.
pub const SECTION_PRE_DATA: i32 = 1;
pub const SECTION_DATA: i32 = 2;
pub const SECTION_POST_DATA: i32 = 3;

// Data block type: actual data.
pub const BLK_DATA: u8 = 1;
// Data block: end of archive.
pub const BLK_EOF: u8 = 3;

// ──────────────────────────────────────────────────────────────────────────────
// TOC entry
// ──────────────────────────────────────────────────────────────────────────────

/// A single TOC entry in a custom archive.
#[derive(Debug, Clone)]
pub struct TocEntry {
    pub dump_id: i32,
    /// Nonzero if this entry has associated table data.
    pub had_dumper: i32,
    pub table_oid: String,
    pub oid: String,
    /// Short object name (e.g. table name).
    pub tag: String,
    /// Object type description (e.g. "TABLE", "TABLE DATA").
    pub desc: String,
    pub section: i32,
    /// DDL SQL (CREATE TABLE …).
    pub defn: String,
    pub drop_stmt: String,
    pub copy_stmt: String,
    pub namespace: String,
    pub tablespace: String,
    pub tableam: String,
    pub owner: String,
    /// Dependencies (dump IDs of entries this depends on).
    pub deps: Vec<i32>,
    /// Byte offset of the data block in the file; set after writing data.
    pub data_offset: u64,
}

impl TocEntry {
    /// Create a schema-only (DDL) TOC entry with no data.
    pub fn schema(dump_id: i32, tag: &str, desc: &str, defn: &str, namespace: &str) -> Self {
        TocEntry {
            dump_id,
            had_dumper: 0,
            table_oid: "0".to_string(),
            oid: "0".to_string(),
            tag: tag.to_string(),
            desc: desc.to_string(),
            section: SECTION_PRE_DATA,
            defn: defn.to_string(),
            drop_stmt: String::new(),
            copy_stmt: String::new(),
            namespace: namespace.to_string(),
            tablespace: String::new(),
            tableam: String::new(),
            owner: String::new(),
            deps: Vec::new(),
            data_offset: u64::MAX, // no data
        }
    }

    /// Create a table-data TOC entry (COPY block).
    pub fn data(dump_id: i32, tag: &str, copy_stmt: &str, namespace: &str, deps: Vec<i32>) -> Self {
        TocEntry {
            dump_id,
            had_dumper: 1,
            table_oid: "0".to_string(),
            oid: "0".to_string(),
            tag: tag.to_string(),
            desc: "TABLE DATA".to_string(),
            section: SECTION_DATA,
            defn: String::new(),
            drop_stmt: String::new(),
            copy_stmt: copy_stmt.to_string(),
            namespace: namespace.to_string(),
            tablespace: String::new(),
            tableam: String::new(),
            owner: String::new(),
            deps,
            data_offset: 0, // filled in during write
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Low-level encoding helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Write a PostgreSQL archive integer: 1-byte sign + 4 bytes little-endian.
///
/// PostgreSQL sign byte: 0 = negative, 1 = positive, 0xFF = NULL/special.
pub fn write_int(w: &mut impl Write, val: i32) -> io::Result<()> {
    if val < 0 {
        w.write_all(&[0u8])?; // sign = negative
        let bytes = (-val as u32).to_le_bytes();
        w.write_all(&bytes)?;
    } else {
        w.write_all(&[1u8])?; // sign = positive
        let bytes = (val as u32).to_le_bytes();
        w.write_all(&bytes)?;
    }
    Ok(())
}

/// Write a PostgreSQL archive offset (8 bytes little-endian, with a 1-byte flag).
pub fn write_offset(w: &mut impl Write, offset: u64) -> io::Result<()> {
    // Flag byte: 1 = valid offset.
    w.write_all(&[1u8])?;
    w.write_all(&offset.to_le_bytes())?;
    Ok(())
}

/// Write a PostgreSQL archive string: 4-byte length (signed, -1 = NULL) + bytes.
pub fn write_str(w: &mut impl Write, s: &str) -> io::Result<()> {
    if s.is_empty() {
        // Use empty string (length=0), not NULL.
        w.write_all(&0i32.to_le_bytes())?;
    } else {
        let bytes = s.as_bytes();
        let len = bytes.len() as i32;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(bytes)?;
    }
    Ok(())
}

/// Write a NULL string (-1 length).
pub fn write_null_str(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&(-1i32).to_le_bytes())?;
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// Header
// ──────────────────────────────────────────────────────────────────────────────

/// Write the PGDMP file header.
pub fn write_header(w: &mut impl Write, toc_count: usize) -> io::Result<()> {
    // Magic
    w.write_all(MAGIC)?;
    // Version: major, minor, revision
    w.write_all(&[VERSION_MAJOR, VERSION_MINOR, VERSION_REV])?;
    // intSize: number of bytes per integer
    w.write_all(&[INT_SIZE])?;
    // offSize: number of bytes per file offset
    w.write_all(&[OFF_SIZE])?;
    // format: 3 = custom
    w.write_all(&[FMT_CUSTOM])?;

    // Compression algorithm flag (0 = none for TOC; data blocks are individually compressed).
    // In PG's format this is a 4-byte int; we write: sign=1, val=0.
    write_int(w, 0)?; // compression

    // Timestamp (seconds since epoch) — use 0 for reproducibility.
    write_int(w, 0)?; // tm_sec
    write_int(w, 0)?; // tm_min
    write_int(w, 0)?; // tm_hour
    write_int(w, 1)?; // tm_mday
    write_int(w, 0)?; // tm_mon  (0 = January)
    write_int(w, 126)?; // tm_year (126 = 2026 - 1900)
    write_int(w, 0)?; // tm_isdst

    // Database name.
    write_str(w, "postgres")?;
    // Server version string.
    write_str(w, "16.0")?;
    // pg_dump version string.
    write_str(w, "pg_plumbing 0.1.0")?;

    // TOC count.
    write_int(w, toc_count as i32)?;

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// TOC entry
// ──────────────────────────────────────────────────────────────────────────────

/// Write one TOC entry.
pub fn write_toc_entry(w: &mut impl Write, entry: &TocEntry) -> io::Result<()> {
    write_int(w, entry.dump_id)?;
    write_int(w, entry.had_dumper)?;
    write_str(w, &entry.table_oid)?;
    write_str(w, &entry.oid)?;
    write_str(w, &entry.tag)?;
    write_str(w, &entry.desc)?;
    write_int(w, entry.section)?;
    write_str(w, &entry.defn)?;
    write_str(w, &entry.drop_stmt)?;
    write_str(w, &entry.copy_stmt)?;
    write_str(w, &entry.namespace)?;
    write_str(w, &entry.tablespace)?;
    write_str(w, &entry.tableam)?;
    write_str(w, &entry.owner)?;
    // withOids: always "false"
    write_str(w, "false")?;

    // Dependencies: write each dump_id as string, terminated by -1.
    for dep in &entry.deps {
        write_str(w, &dep.to_string())?;
    }
    write_null_str(w)?; // terminator

    // Data offset: if this entry has data (had_dumper != 0), write its offset.
    // If no data, write a special sentinel.
    if entry.had_dumper != 0 {
        write_offset(w, entry.data_offset)?;
    } else {
        // No data block: flag = 0 (invalid offset).
        w.write_all(&[0u8])?;
        w.write_all(&[0u8; 8])?;
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// Data blocks
// ──────────────────────────────────────────────────────────────────────────────

/// Write a compressed data block for one TOC entry.
///
/// Format:
///   - BLK_DATA (1 byte)
///   - dump_id  (5-byte int)
///   - compressed_size (5-byte int)
///   - compressed data
///   - BLK_DATA + dump_id + size=0 (end-of-data marker for this entry)
pub fn write_data_block(w: &mut impl Write, dump_id: i32, data: &[u8]) -> io::Result<()> {
    // Compress the data.
    let compressed = compress_zlib(data)?;

    // Block header: type + dump_id.
    w.write_all(&[BLK_DATA])?;
    write_int(w, dump_id)?;
    // Compressed data size.
    write_int(w, compressed.len() as i32)?;
    // Compressed data.
    w.write_all(&compressed)?;

    // End-of-data marker: another BLK_DATA with size=0.
    w.write_all(&[BLK_DATA])?;
    write_int(w, dump_id)?;
    write_int(w, 0)?;

    Ok(())
}

/// Write the end-of-archive marker.
pub fn write_eof(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[BLK_EOF])?;
    Ok(())
}

/// Compress bytes with zlib (deflate).
fn compress_zlib(data: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    encoder.finish()
}

// ──────────────────────────────────────────────────────────────────────────────
// Reader (for pg_restore)
// ──────────────────────────────────────────────────────────────────────────────

/// Read a 4-byte little-endian signed integer with a sign byte prefix.
pub fn read_int(r: &mut impl std::io::Read) -> io::Result<i32> {
    let mut sign = [0u8; 1];
    r.read_exact(&mut sign)?;
    let mut bytes = [0u8; 4];
    r.read_exact(&mut bytes)?;
    let val = i32::from_le_bytes(bytes);
    if sign[0] == 0 {
        Ok(-val)
    } else {
        Ok(val)
    }
}

/// Read a PostgreSQL archive string (4-byte length + bytes; -1 = NULL → "").
pub fn read_str(r: &mut impl std::io::Read) -> io::Result<String> {
    let mut len_bytes = [0u8; 4];
    r.read_exact(&mut len_bytes)?;
    let len = i32::from_le_bytes(len_bytes);
    if len < 0 {
        return Ok(String::new());
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

/// Read a file offset (1-byte flag + 8 bytes).
pub fn read_offset(r: &mut impl std::io::Read) -> io::Result<u64> {
    let mut flag = [0u8; 1];
    r.read_exact(&mut flag)?;
    let mut bytes = [0u8; 8];
    r.read_exact(&mut bytes)?;
    if flag[0] == 0 {
        Ok(u64::MAX)
    } else {
        Ok(u64::from_le_bytes(bytes))
    }
}

/// Parsed TOC entry returned by `read_toc_entry`.
#[derive(Debug, Clone)]
pub struct ParsedTocEntry {
    pub dump_id: i32,
    pub had_dumper: i32,
    pub tag: String,
    pub desc: String,
    pub section: i32,
    pub defn: String,
    pub copy_stmt: String,
    pub namespace: String,
    pub deps: Vec<i32>,
    pub data_offset: u64,
}

/// Read one TOC entry from the reader.
pub fn read_toc_entry(r: &mut impl std::io::Read) -> io::Result<ParsedTocEntry> {
    let dump_id = read_int(r)?;
    let had_dumper = read_int(r)?;
    let _table_oid = read_str(r)?;
    let _oid = read_str(r)?;
    let tag = read_str(r)?;
    let desc = read_str(r)?;
    let section = read_int(r)?;
    let defn = read_str(r)?;
    let _drop_stmt = read_str(r)?;
    let copy_stmt = read_str(r)?;
    let namespace = read_str(r)?;
    let _tablespace = read_str(r)?;
    let _tableam = read_str(r)?;
    let _owner = read_str(r)?;
    let _with_oids = read_str(r)?;

    // Read dependency list (strings, terminated by NULL/-1).
    let mut deps = Vec::new();
    loop {
        let s = read_str(r)?;
        if s.is_empty() {
            // Check if it was a NULL terminator by looking at what read_str returned.
            // We distinguish NULL (-1) from empty (0) by re-reading — but since read_str
            // converts NULL to "", we terminate on empty string (our terminator is NULL).
            break;
        }
        if let Ok(id) = s.parse::<i32>() {
            deps.push(id);
        }
    }

    let data_offset = read_offset(r)?;

    Ok(ParsedTocEntry {
        dump_id,
        had_dumper,
        tag,
        desc,
        section,
        defn,
        copy_stmt,
        namespace,
        deps,
        data_offset,
    })
}

/// Validate the PGDMP magic and return the format byte.
pub fn read_header(r: &mut (impl std::io::Read + std::io::Seek)) -> io::Result<(u8, usize)> {
    let mut magic = [0u8; 5];
    r.read_exact(&mut magic)?;
    if magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a pg_dump custom archive (missing PGDMP magic)",
        ));
    }

    let mut version = [0u8; 3];
    r.read_exact(&mut version)?;
    // version[0]=major, version[1]=minor, version[2]=rev

    let mut sizes = [0u8; 2];
    r.read_exact(&mut sizes)?;
    // sizes[0]=intSize, sizes[1]=offSize

    let mut fmt = [0u8; 1];
    r.read_exact(&mut fmt)?;
    let format = fmt[0];

    // compression int
    let _compression = read_int(r)?;

    // Timestamp: 7 ints
    for _ in 0..7 {
        let _t = read_int(r)?;
    }

    // Strings: dbname, server_version, dump_version
    let _dbname = read_str(r)?;
    let _server_version = read_str(r)?;
    let _dump_version = read_str(r)?;

    // TOC count
    let toc_count = read_int(r)? as usize;

    Ok((format, toc_count))
}

/// Read a compressed data block from the current position.
/// Returns the dump_id and decompressed data.
/// Returns None if we hit BLK_EOF.
pub fn read_next_data_block(r: &mut impl std::io::Read) -> io::Result<Option<(i32, Vec<u8>)>> {
    let mut block_type = [0u8; 1];
    r.read_exact(&mut block_type)?;

    match block_type[0] {
        BLK_EOF => Ok(None),
        BLK_DATA => {
            let dump_id = read_int(r)?;
            let compressed_size = read_int(r)?;

            if compressed_size <= 0 {
                // End-of-data marker for a previous entry or zero-size block.
                return Ok(Some((dump_id, Vec::new())));
            }

            let mut compressed = vec![0u8; compressed_size as usize];
            r.read_exact(&mut compressed)?;

            // Decompress
            use flate2::read::ZlibDecoder;
            use std::io::Read;
            let mut decoder = ZlibDecoder::new(&compressed[..]);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;

            // Skip the end-of-data marker (BLK_DATA + dump_id + size=0).
            let mut marker_type = [0u8; 1];
            r.read_exact(&mut marker_type)?;
            if marker_type[0] == BLK_DATA {
                let _id = read_int(r)?;
                let _sz = read_int(r)?;
            }

            Ok(Some((dump_id, decompressed)))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected block type {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_int() {
        let mut buf = Vec::new();
        write_int(&mut buf, 42).unwrap();
        write_int(&mut buf, -7).unwrap();
        write_int(&mut buf, 0).unwrap();
        let mut cursor = std::io::Cursor::new(&buf);
        assert_eq!(read_int(&mut cursor).unwrap(), 42);
        assert_eq!(read_int(&mut cursor).unwrap(), -7);
        assert_eq!(read_int(&mut cursor).unwrap(), 0);
    }

    #[test]
    fn roundtrip_str() {
        let mut buf = Vec::new();
        write_str(&mut buf, "hello").unwrap();
        write_str(&mut buf, "").unwrap();
        let mut cursor = std::io::Cursor::new(&buf);
        assert_eq!(read_str(&mut cursor).unwrap(), "hello");
        assert_eq!(read_str(&mut cursor).unwrap(), "");
    }

    #[test]
    fn header_starts_with_pgdmp() {
        let mut buf = Vec::new();
        write_header(&mut buf, 2).unwrap();
        assert!(buf.starts_with(b"PGDMP"));
    }
}
