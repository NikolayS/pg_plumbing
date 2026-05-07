// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Shared library for pg_plumbing (pg_dump/pg_restore).

pub mod dump;
pub mod restore;

/// Parameters that override environment variables when building a conninfo
/// string.  All fields are optional; `None` means "fall back to the
/// corresponding PG* environment variable (or the compiled-in default)".
#[derive(Debug, Default, Clone)]
pub struct ConnParams {
    pub host: Option<String>,
    pub port: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl ConnParams {
    fn has_explicit_values(&self) -> bool {
        self.host.is_some() || self.port.is_some() || self.user.is_some() || self.password.is_some()
    }
}

fn conninfo_value(value: &str) -> String {
    let simple = !value.is_empty()
        && value
            .bytes()
            .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-'));

    if simple {
        value.to_string()
    } else {
        let escaped = value.replace('\\', "\\\\").replace('\'', "\\'");
        format!("'{escaped}'")
    }
}

fn append_explicit_params(mut conninfo: String, params: &ConnParams) -> String {
    if let Some(ref host) = params.host {
        conninfo.push_str(&format!(" host={}", conninfo_value(host)));
    }
    if let Some(ref port) = params.port {
        conninfo.push_str(&format!(" port={}", conninfo_value(port)));
    }
    if let Some(ref user) = params.user {
        conninfo.push_str(&format!(" user={}", conninfo_value(user)));
    }
    if let Some(ref password) = params.password {
        conninfo.push_str(&format!(" password={}", conninfo_value(password)));
    }
    conninfo
}

fn looks_like_key_value_conninfo(value: &str) -> bool {
    let Some(eq_pos) = value.find('=') else {
        return false;
    };

    let key = &value[..eq_pos];
    matches!(
        key,
        "application_name"
            | "connect_timeout"
            | "dbname"
            | "fallback_application_name"
            | "host"
            | "hostaddr"
            | "options"
            | "passfile"
            | "password"
            | "port"
            | "replication"
            | "service"
            | "sslcert"
            | "sslkey"
            | "sslmode"
            | "sslrootcert"
            | "target_session_attrs"
            | "user"
    )
}

/// Build a libpq-style connection string from a database name and optional
/// connection parameters.
///
/// Pass-through rules (in order):
/// 1. If `dbname` starts with `postgresql://` or `postgres://` it is a URI —
///    return it unchanged (tokio-postgres handles URIs natively).
/// 2. If `dbname` contains `=` it is already a `key=value` connstring —
///    return it unchanged.
/// 3. Otherwise treat `dbname` as a bare database name and build a minimal
///    `host=… port=… user=… dbname=…` string, substituting values from
///    `params` first and falling back to environment variables.
pub fn build_conninfo_with_params(dbname: &str, params: &ConnParams) -> String {
    // URI pass-through
    if dbname.starts_with("postgresql://") || dbname.starts_with("postgres://") {
        if params.has_explicit_values() {
            return append_explicit_params(format!("dbname={}", conninfo_value(dbname)), params);
        }
        return dbname.to_string();
    }

    // key=value connstring pass-through
    if looks_like_key_value_conninfo(dbname) {
        if params.has_explicit_values() {
            return append_explicit_params(dbname.to_string(), params);
        }
        return dbname.to_string();
    }

    // Bare database name — build from params + env
    let host = params
        .host
        .clone()
        .or_else(|| std::env::var("PGHOST").ok())
        .unwrap_or_else(|| "localhost".to_string());
    let port = params
        .port
        .clone()
        .or_else(|| std::env::var("PGPORT").ok())
        .unwrap_or_else(|| "5432".to_string());
    let user = params
        .user
        .clone()
        .or_else(|| std::env::var("PGUSER").ok())
        .unwrap_or_else(|| "postgres".to_string());
    let password = params
        .password
        .clone()
        .or_else(|| std::env::var("PGPASSWORD").ok())
        .unwrap_or_default();

    let mut s = format!(
        "host={} port={} user={} dbname={}",
        conninfo_value(&host),
        conninfo_value(&port),
        conninfo_value(&user),
        conninfo_value(dbname)
    );
    if !password.is_empty() {
        s.push_str(&format!(" password={}", conninfo_value(&password)));
    }
    s
}

/// Convenience wrapper — equivalent to `build_conninfo_with_params(dbname,
/// &ConnParams::default())`.
pub fn build_conninfo(dbname: &str) -> String {
    build_conninfo_with_params(dbname, &ConnParams::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conninfo_uri_passthrough() {
        // URI-style should be passed through unchanged
        let uri = "postgresql://user:pass@host:5432/mydb";
        assert_eq!(build_conninfo(uri), uri);

        let uri2 = "postgres://localhost/template1";
        assert_eq!(build_conninfo(uri2), uri2);
    }

    #[test]
    fn conninfo_keyvalue_passthrough() {
        // key=value style should be passed through unchanged
        let kv = "dbname=template1 host=localhost";
        assert_eq!(build_conninfo(kv), kv);
    }

    #[test]
    fn conninfo_bare_dbname_uses_params() {
        // Bare name → builds host=… port=… user=… dbname=… password=… string.
        // Supplying an explicit password via ConnParams to avoid env-var
        // interference in CI (PGPASSWORD may be set).
        let params = ConnParams {
            host: Some("myhost".to_string()),
            port: Some("5433".to_string()),
            user: Some("myuser".to_string()),
            password: Some("mypass".to_string()),
        };
        let result = build_conninfo_with_params("mydb", &params);
        assert_eq!(
            result,
            "host=myhost port=5433 user=myuser dbname=mydb password=mypass"
        );
    }

    #[test]
    fn conninfo_params_override_produce_password() {
        let params = ConnParams {
            host: Some("h".to_string()),
            port: Some("5432".to_string()),
            user: Some("u".to_string()),
            password: Some("secret".to_string()),
        };
        let result = build_conninfo_with_params("db", &params);
        assert_eq!(result, "host=h port=5432 user=u dbname=db password=secret");
    }

    #[test]
    fn conninfo_bare_dbname_quotes_special_chars() {
        let params = ConnParams {
            host: Some("local socket".to_string()),
            port: Some("5432".to_string()),
            user: Some("user name\\with'quotes".to_string()),
            password: Some("pass word".to_string()),
        };
        let result = build_conninfo_with_params("db name\\with'quotes", &params);
        assert_eq!(
            result,
            "host='local socket' port=5432 user='user name\\\\with\\'quotes' dbname='db name\\\\with\\'quotes' password='pass word'"
        );
    }

    #[test]
    fn conninfo_bare_dbname_with_equals_is_not_connstring() {
        let params = ConnParams {
            host: Some("h".to_string()),
            port: Some("5432".to_string()),
            user: Some("u".to_string()),
            password: Some("".to_string()),
        };
        let result = build_conninfo_with_params("db=name", &params);
        assert_eq!(result, "host=h port=5432 user=u dbname='db=name'");
    }

    #[test]
    fn conninfo_connstring_appends_explicit_overrides() {
        let params = ConnParams {
            host: None,
            port: Some("6543".to_string()),
            user: Some("override user".to_string()),
            password: None,
        };
        assert_eq!(
            build_conninfo_with_params("dbname=template1 user=original", &params),
            "dbname=template1 user=original port=6543 user='override user'"
        );
    }
}
