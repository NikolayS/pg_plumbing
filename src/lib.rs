// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! pg_plumbing — pg_dump/pg_restore rewritten in Rust.
//!
//! This crate provides the core library for dumping and restoring
//! PostgreSQL databases in a format-compatible manner with the
//! original C-based pg_dump and pg_restore tools.

pub mod dump;
pub mod restore;
