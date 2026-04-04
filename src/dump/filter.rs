// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Schema and table filtering for pg_dump (-t, -T, -n, -N, --exclude-table-data).
//!
//! Provides fnmatch-style wildcard pattern matching:
//! - `*` matches any sequence of characters (including empty)
//! - `?` matches any single character
//! - Matching is case-sensitive
//!
//! Each pattern is tested against both the unqualified name and the
//! schema-qualified name (e.g. `foo` and `public.foo`).

/// Returns `true` if `text` matches the glob `pattern`.
///
/// Supports `*` (any sequence) and `?` (any single character).
/// Uses an O(m×n) DP algorithm to avoid exponential backtracking.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (m, n) = (p.len(), t.len());
    let mut dp = vec![vec![false; n + 1]; m + 1];
    dp[0][0] = true;
    for i in 1..=m {
        if p[i - 1] == '*' {
            dp[i][0] = dp[i - 1][0];
        }
    }
    for i in 1..=m {
        for j in 1..=n {
            dp[i][j] = match p[i - 1] {
                '*' => dp[i - 1][j] || dp[i][j - 1],
                '?' => dp[i - 1][j - 1],
                c => dp[i - 1][j - 1] && c == t[j - 1],
            };
        }
    }
    dp[m][n]
}

/// Returns `true` if `name` matches any of the given patterns.
///
/// Each pattern is tested against both the unqualified `name` and the
/// fully-qualified `schema.name`.
pub fn matches_any(patterns: &[String], schema: &str, name: &str) -> bool {
    if patterns.is_empty() {
        return false;
    }
    let qualified = format!("{schema}.{name}");
    patterns
        .iter()
        .any(|p| glob_match(p, name) || glob_match(p, &qualified))
}

/// Returns `true` if `schema` matches any of the given schema patterns.
pub fn schema_matches_any(patterns: &[String], schema: &str) -> bool {
    if patterns.is_empty() {
        return false;
    }
    patterns.iter().any(|p| glob_match(p, schema))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(glob_match("foo", "foo"));
        assert!(!glob_match("foo", "bar"));
    }

    #[test]
    fn star_wildcard() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        assert!(glob_match("dump_*", "dump_test_simple"));
        assert!(!glob_match("dump_*", "other_table"));
        assert!(glob_match("*simple", "dump_test_simple"));
        assert!(glob_match("dump_*_simple", "dump_test_simple"));
    }

    #[test]
    fn question_wildcard() {
        assert!(glob_match("fo?", "foo"));
        assert!(glob_match("fo?", "fob"));
        assert!(!glob_match("fo?", "fooo"));
        assert!(!glob_match("fo?", "fo"));
    }

    #[test]
    fn qualified_name_match() {
        assert!(matches_any(
            &["dump_test_simple".to_string()],
            "public",
            "dump_test_simple"
        ));
        assert!(matches_any(
            &["public.dump_test_simple".to_string()],
            "public",
            "dump_test_simple"
        ));
        assert!(!matches_any(
            &["other.dump_test_simple".to_string()],
            "public",
            "dump_test_simple"
        ));
    }

    #[test]
    fn schema_match() {
        assert!(schema_matches_any(&["public".to_string()], "public"));
        assert!(!schema_matches_any(&["other".to_string()], "public"));
        assert!(schema_matches_any(&["pub*".to_string()], "public"));
    }

    #[test]
    fn empty_patterns() {
        assert!(!matches_any(&[], "public", "foo"));
        assert!(!schema_matches_any(&[], "public"));
    }
}
