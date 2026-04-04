// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Hardening tests for custom_format.rs:
//! 1. i32 overflow guard in write_data_block for large compressed data.
//! 2. End-of-data marker validation in read_next_data_block.

use pg_plumbing::dump::custom_format::{
    read_next_data_block, write_data_block, write_data_block_compressed, BLK_DATA, BLK_EOF,
};
use std::io::Cursor;

// ──────────────────────────────────────────────────────────────────────────────
// Fix 1: i32 overflow guard in write_data_block
// ──────────────────────────────────────────────────────────────────────────────

/// write_data_block must return an error when the compressed output would
/// exceed i32::MAX bytes.
///
/// We call `write_data_block_compressed` — the inner function that owns the
/// overflow guard — with a fake pre-"compressed" slice whose length is
/// i32::MAX + 1.  This avoids allocating >2 GB while still exercising the
/// exact `i32::try_from` check in production code.
///
/// This test **will fail** if the guard is removed from
/// `write_data_block_compressed`.
#[test]
fn write_data_block_len_overflow_returns_error() {
    // Build a 1-byte backing buffer and reinterpret its length via a raw-slice
    // fat pointer so we never allocate >2 GB.  The writer is a sink — it will
    // error before any bytes are written because the overflow check runs first.
    let backing = [0u8; 1];
    let oversized_len = (i32::MAX as usize) + 1;
    // SAFETY: we construct a slice header with a fake length. The overflow
    // guard in write_data_block_compressed checks `compressed.len()` before
    // doing any memory access into the slice, so the backing store is never
    // read beyond its real size.
    let oversized: &[u8] = unsafe { std::slice::from_raw_parts(backing.as_ptr(), oversized_len) };

    let mut sink = Vec::new();
    let result = write_data_block_compressed(&mut sink, 1, oversized);
    assert!(
        result.is_err(),
        "write_data_block_compressed must error when compressed len > i32::MAX"
    );
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("i32::MAX") || msg.contains("exceeds"),
        "error message should mention the overflow, got: {err}"
    );
    // The writer must not have been touched (guard fires before any write).
    assert!(sink.is_empty(), "no bytes must be written on overflow");
}

/// write_data_block succeeds for normal-sized data and round-trips correctly.
#[test]
fn write_data_block_normal_data_roundtrips() {
    let data = b"COPY users (id, name) FROM stdin;\n1\tAlice\n2\tBob\n\\.\n";
    let mut buf = Vec::new();
    write_data_block(&mut buf, 42, data).expect("write_data_block must succeed for small data");

    // The buffer must start with BLK_DATA.
    assert_eq!(buf[0], BLK_DATA, "first byte must be BLK_DATA");

    // Read back via read_next_data_block.
    let mut cursor = Cursor::new(&buf);
    let result = read_next_data_block(&mut cursor)
        .expect("read_next_data_block must not error on valid data");
    let (id, decompressed) = result.expect("must return Some for a data block");
    assert_eq!(id, 42);
    assert_eq!(decompressed, data);
}

/// write_data_block with empty data still writes a valid block.
/// Note: zlib output for empty input is ~8 bytes (not size=0), so the
/// compressed_size field in the block is positive.
#[test]
fn write_data_block_empty_data_roundtrips() {
    let mut buf = Vec::new();
    write_data_block(&mut buf, 7, b"").expect("write_data_block must succeed for empty data");

    let mut cursor = Cursor::new(&buf);
    let result = read_next_data_block(&mut cursor);
    assert!(
        result.is_ok(),
        "empty block must not produce an error: {:?}",
        result
    );
    let block = result
        .unwrap()
        .expect("must return Some for an empty-data block");
    assert_eq!(block.0, 7, "dump_id must round-trip");
    assert!(block.1.is_empty(), "decompressed data must be empty");
}

// ──────────────────────────────────────────────────────────────────────────────
// Fix 2: end-of-data marker validation in read_next_data_block
// ──────────────────────────────────────────────────────────────────────────────

/// Helper: build a valid compressed data block buffer for dump_id.
fn make_valid_block(dump_id: i32, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_data_block(&mut buf, dump_id, data).unwrap();
    buf
}

/// A corrupt archive where the end-of-data marker byte is wrong (not BLK_DATA)
/// must return an error.
#[test]
fn read_next_data_block_bad_marker_byte_returns_error() {
    // Build a valid block first.
    let mut buf = make_valid_block(1, b"hello world");

    // The end-of-data marker is: BLK_DATA(1) + write_int(id)(5) + write_int(0)(5) = 11 bytes
    let marker_start = buf.len() - 11;
    // Corrupt the marker byte to something invalid (e.g. 0x42).
    buf[marker_start] = 0x42;

    let mut cursor = Cursor::new(&buf);
    let result = read_next_data_block(&mut cursor);
    assert!(
        result.is_err(),
        "corrupt end-of-data marker byte must return an error, got: {:?}",
        result
    );
    let err = result.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("marker")
            || msg.contains("corrupt")
            || msg.contains("invalid")
            || msg.contains("end"),
        "error message should describe the problem, got: {err}"
    );
}

/// A corrupt archive where the end-of-data marker size field is nonzero
/// (indicating a truncated or corrupt archive) must return an error.
#[test]
fn read_next_data_block_nonzero_marker_size_returns_error() {
    let mut buf = make_valid_block(1, b"hello world");

    // End-of-data marker layout (last 11 bytes):
    //   offset 0: BLK_DATA (0x01)
    //   offset 1-5: write_int(dump_id) — sign byte + 4 LE bytes
    //   offset 6-10: write_int(0)      — sign byte + 4 LE bytes (the size=0 field)
    let marker_start = buf.len() - 11;

    // Overwrite the size field (bytes 6-10 of the marker) with write_int(999).
    // write_int layout: [sign_byte=1, val_le32]
    let size_offset = marker_start + 6;
    buf[size_offset] = 1u8; // sign = positive
    let val_bytes = 999u32.to_le_bytes();
    buf[size_offset + 1..size_offset + 5].copy_from_slice(&val_bytes);

    let mut cursor = Cursor::new(&buf);
    let result = read_next_data_block(&mut cursor);
    assert!(
        result.is_err(),
        "nonzero end-of-data marker size must return an error, got: {:?}",
        result
    );
    let err = result.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("marker")
            || msg.contains("corrupt")
            || msg.contains("invalid")
            || msg.contains("size")
            || msg.contains("truncat"),
        "error message should describe the problem, got: {err}"
    );
}

/// A corrupt archive where the end-of-data marker dump_id mismatches the
/// block dump_id must return an error (spliced-archive detection).
#[test]
fn read_next_data_block_mismatched_marker_id_returns_error() {
    let mut buf = make_valid_block(10, b"hello world");

    // End-of-data marker layout (last 11 bytes):
    //   offset 0:   BLK_DATA
    //   offset 1-5: write_int(dump_id=10)
    //   offset 6-10: write_int(0)
    let marker_start = buf.len() - 11;

    // Overwrite the marker dump_id (bytes 1-5) with write_int(99).
    let id_offset = marker_start + 1;
    buf[id_offset] = 1u8; // sign = positive
    let val_bytes = 99u32.to_le_bytes();
    buf[id_offset + 1..id_offset + 5].copy_from_slice(&val_bytes);

    let mut cursor = Cursor::new(&buf);
    let result = read_next_data_block(&mut cursor);
    assert!(
        result.is_err(),
        "mismatched marker dump_id must return an error, got: {:?}",
        result
    );
    let err = result.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("marker") || msg.contains("corrupt") || msg.contains("dump_id"),
        "error message should describe the mismatch, got: {err}"
    );
}

/// A valid archive round-trips through read_next_data_block followed by BLK_EOF.
#[test]
fn read_next_data_block_followed_by_eof() {
    let data = b"some table data\n";
    let mut buf = make_valid_block(5, data);
    buf.push(BLK_EOF);

    let mut cursor = Cursor::new(&buf);

    // First call: data block.
    let block = read_next_data_block(&mut cursor)
        .expect("must not error")
        .expect("must be Some");
    assert_eq!(block.0, 5);
    assert_eq!(block.1, data);

    // Second call: EOF.
    let eof = read_next_data_block(&mut cursor).expect("must not error on EOF");
    assert!(eof.is_none(), "must return None at BLK_EOF");
}

/// Marker with correct byte and size=0 does not produce an error (positive case).
#[test]
fn read_next_data_block_valid_marker_ok() {
    let data = b"id\tname\n1\tAlice\n";
    let buf = make_valid_block(3, data);
    let mut cursor = Cursor::new(&buf);
    let result = read_next_data_block(&mut cursor);
    assert!(result.is_ok(), "valid block must not error: {:?}", result);
    let block = result.unwrap().expect("must be Some");
    assert_eq!(block.0, 3);
    assert_eq!(block.1, data);
}
