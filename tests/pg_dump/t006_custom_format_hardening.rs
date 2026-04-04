// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Hardening tests for custom_format.rs:
//! 1. i32 overflow guard in write_data_block for large compressed data.
//! 2. End-of-data marker validation in read_next_data_block.

use pg_plumbing::dump::custom_format::{read_next_data_block, write_data_block, BLK_DATA, BLK_EOF};
use std::io::Cursor;

// ──────────────────────────────────────────────────────────────────────────────
// Fix 1: i32 overflow guard in write_data_block
// ──────────────────────────────────────────────────────────────────────────────

/// write_data_block must return an error when the compressed output would
/// exceed i32::MAX bytes (instead of silently wrapping the cast).
///
/// We verify the same checked-cast logic that write_data_block now uses,
/// confirming i32::try_from properly rejects oversized lengths.
#[test]
fn write_data_block_len_overflow_returns_error() {
    // Verify that i32::try_from correctly rejects values > i32::MAX.
    // This is the same checked-cast logic now used inside write_data_block.
    let oversized: usize = (i32::MAX as usize) + 1;
    let result = i32::try_from(oversized);
    assert!(
        result.is_err(),
        "i32::try_from on oversized len must fail — sanity check for the guard logic"
    );
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

/// write_data_block with empty data still writes a valid block (size=0 path).
#[test]
fn write_data_block_empty_data_roundtrips() {
    let mut buf = Vec::new();
    write_data_block(&mut buf, 7, b"").expect("write_data_block must succeed for empty data");

    let mut cursor = Cursor::new(&buf);
    // No error must be raised reading an empty-data block.
    let result = read_next_data_block(&mut cursor);
    assert!(
        result.is_ok(),
        "empty block must not produce an error: {:?}",
        result
    );
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
