// Copyright 2026 pg_plumbing contributors
// SPDX-License-Identifier: MIT

//! Shared library for pg_plumbing (pg_dump/pg_restore).

pub mod dump;
pub mod restore;

use std::time::Duration;

use tokio_postgres::config::{
    ChannelBinding, Host, LoadBalanceHosts, SslMode, SslNegotiation, TargetSessionAttrs,
};
use tokio_postgres::Config;

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

fn is_tokio_postgres_conninfo_key(key: &str) -> bool {
    matches!(
        key,
        "application_name"
            | "channel_binding"
            | "connect_timeout"
            | "dbname"
            | "fallback_application_name"
            | "host"
            | "hostaddr"
            | "keepalives"
            | "keepalives_idle"
            | "keepalives_interval"
            | "keepalives_retries"
            | "load_balance_hosts"
            | "options"
            | "passfile"
            | "password"
            | "port"
            | "replication"
            | "service"
            | "sslcert"
            | "sslkey"
            | "sslmode"
            | "sslnegotiation"
            | "sslrootcert"
            | "target_session_attrs"
            | "tcp_user_timeout"
            | "user"
    )
}

fn looks_like_key_value_conninfo(value: &str) -> bool {
    let value = value.trim_start();
    let Some(eq_pos) = value.find('=') else {
        return false;
    };

    let key = &value[..eq_pos];
    !key.is_empty() && !key.chars().any(char::is_whitespace) && is_tokio_postgres_conninfo_key(key)
}

fn push_param(out: &mut Vec<String>, key: &str, value: impl AsRef<str>) {
    out.push(format!("{key}={}", conninfo_value(value.as_ref())));
}

fn duration_secs(duration: Duration) -> String {
    duration.as_secs().to_string()
}

fn target_session_attrs_value(value: TargetSessionAttrs) -> Option<&'static str> {
    match value {
        TargetSessionAttrs::Any => None,
        TargetSessionAttrs::ReadWrite => Some("read-write"),
        TargetSessionAttrs::ReadOnly => Some("read-only"),
        _ => None,
    }
}

fn sslmode_value(value: SslMode) -> Option<&'static str> {
    match value {
        SslMode::Disable => Some("disable"),
        SslMode::Prefer => None,
        SslMode::Require => Some("require"),
        _ => None,
    }
}

fn sslnegotiation_value(value: SslNegotiation) -> Option<&'static str> {
    match value {
        SslNegotiation::Postgres => None,
        SslNegotiation::Direct => Some("direct"),
        _ => None,
    }
}

fn channel_binding_value(value: ChannelBinding) -> Option<&'static str> {
    match value {
        ChannelBinding::Disable => Some("disable"),
        ChannelBinding::Prefer => None,
        ChannelBinding::Require => Some("require"),
        _ => None,
    }
}

fn load_balance_hosts_value(value: LoadBalanceHosts) -> Option<&'static str> {
    match value {
        LoadBalanceHosts::Disable => None,
        LoadBalanceHosts::Random => Some("random"),
        _ => None,
    }
}

fn host_value(host: &Host) -> String {
    match host {
        Host::Tcp(host) => host.clone(),
        #[cfg(unix)]
        Host::Unix(path) => path.to_string_lossy().into_owned(),
    }
}

fn conninfo_from_config_with_overrides(config: &Config, params: &ConnParams) -> String {
    let mut out = Vec::new();

    if let Some(host) = &params.host {
        push_param(&mut out, "host", host);
    } else if !config.get_hosts().is_empty() {
        push_param(
            &mut out,
            "host",
            config
                .get_hosts()
                .iter()
                .map(host_value)
                .collect::<Vec<_>>()
                .join(","),
        );
    }

    if params.host.is_none() && !config.get_hostaddrs().is_empty() {
        push_param(
            &mut out,
            "hostaddr",
            config
                .get_hostaddrs()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
    }

    if let Some(port) = &params.port {
        push_param(&mut out, "port", port);
    } else if !config.get_ports().is_empty() {
        push_param(
            &mut out,
            "port",
            config
                .get_ports()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
    }

    if let Some(user) = &params.user {
        push_param(&mut out, "user", user);
    } else if let Some(user) = config.get_user() {
        push_param(&mut out, "user", user);
    }

    if let Some(password) = &params.password {
        push_param(&mut out, "password", password);
    } else if let Some(password) = config.get_password() {
        push_param(&mut out, "password", String::from_utf8_lossy(password));
    }

    if let Some(dbname) = config.get_dbname() {
        push_param(&mut out, "dbname", dbname);
    }
    if let Some(options) = config.get_options() {
        push_param(&mut out, "options", options);
    }
    if let Some(application_name) = config.get_application_name() {
        push_param(&mut out, "application_name", application_name);
    }
    if let Some(sslmode) = sslmode_value(config.get_ssl_mode()) {
        push_param(&mut out, "sslmode", sslmode);
    }
    if let Some(sslnegotiation) = sslnegotiation_value(config.get_ssl_negotiation()) {
        push_param(&mut out, "sslnegotiation", sslnegotiation);
    }
    if let Some(timeout) = config.get_connect_timeout() {
        push_param(&mut out, "connect_timeout", duration_secs(*timeout));
    }
    if let Some(timeout) = config.get_tcp_user_timeout() {
        push_param(&mut out, "tcp_user_timeout", duration_secs(*timeout));
    }
    if !config.get_keepalives() {
        push_param(&mut out, "keepalives", "0");
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        if config.get_keepalives_idle() != Duration::from_secs(2 * 60 * 60) {
            push_param(
                &mut out,
                "keepalives_idle",
                duration_secs(config.get_keepalives_idle()),
            );
        }
        if let Some(interval) = config.get_keepalives_interval() {
            push_param(&mut out, "keepalives_interval", duration_secs(interval));
        }
        if let Some(retries) = config.get_keepalives_retries() {
            push_param(&mut out, "keepalives_retries", retries.to_string());
        }
    }
    if let Some(target_session_attrs) =
        target_session_attrs_value(config.get_target_session_attrs())
    {
        push_param(&mut out, "target_session_attrs", target_session_attrs);
    }
    if let Some(channel_binding) = channel_binding_value(config.get_channel_binding()) {
        push_param(&mut out, "channel_binding", channel_binding);
    }
    if let Some(load_balance_hosts) = load_balance_hosts_value(config.get_load_balance_hosts()) {
        push_param(&mut out, "load_balance_hosts", load_balance_hosts);
    }

    out.join(" ")
}

/// Build a libpq-style connection string from a database name and optional
/// connection parameters.
///
/// Pass-through rules (in order):
/// 1. If `dbname` starts with `postgresql://` or `postgres://` it is a URI —
///    return it unchanged when there are no explicit CLI params. With explicit
///    params, parse it as tokio-postgres `Config` and emit an equivalent
///    key/value conninfo with CLI values applied as real overrides.
/// 2. If `dbname` starts with a recognized conninfo key, it is already a
///    `key=value` connstring — return it unchanged when there are no explicit
///    CLI params, otherwise apply CLI values through tokio-postgres `Config`
///    semantics where the conninfo is parseable.
/// 3. Otherwise treat `dbname` as a bare database name and build a minimal
///    `host=… port=… user=… dbname=…` string, substituting values from
///    `params` first and falling back to environment variables.
pub fn build_conninfo_with_params(dbname: &str, params: &ConnParams) -> String {
    // URI pass-through
    if dbname.starts_with("postgresql://") || dbname.starts_with("postgres://") {
        if params.has_explicit_values() {
            if let Ok(config) = dbname.parse::<Config>() {
                return conninfo_from_config_with_overrides(&config, params);
            }
        }
        return dbname.to_string();
    }

    // key=value connstring pass-through
    if looks_like_key_value_conninfo(dbname) {
        if params.has_explicit_values() {
            if let Ok(config) = dbname.parse::<Config>() {
                return conninfo_from_config_with_overrides(&config, params);
            }
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
    fn conninfo_connstring_applies_explicit_overrides_semantically() {
        let params = ConnParams {
            host: None,
            port: Some("6543".to_string()),
            user: Some("override user".to_string()),
            password: None,
        };
        let result = build_conninfo_with_params("dbname=template1 user=original", &params);
        let config = result.parse::<Config>().unwrap();

        assert_eq!(config.get_dbname(), Some("template1"));
        assert_eq!(config.get_user(), Some("override user"));
        assert_eq!(config.get_ports(), &[6543]);
    }

    #[test]
    fn conninfo_uri_applies_explicit_overrides_without_wrapping_as_dbname() {
        let params = ConnParams {
            host: Some("newhost".to_string()),
            port: Some("6543".to_string()),
            user: Some("override user".to_string()),
            password: Some("override password".to_string()),
        };
        let result = build_conninfo_with_params(
            "postgresql://original:secret@oldhost:5432/mydb?connect_timeout=10&keepalives=0&channel_binding=require&sslnegotiation=direct&load_balance_hosts=random",
            &params,
        );
        let config = result.parse::<Config>().unwrap();

        assert!(!result.contains("dbname='postgresql://"));
        assert_eq!(config.get_dbname(), Some("mydb"));
        assert_eq!(config.get_hosts(), &[Host::Tcp("newhost".to_string())]);
        assert_eq!(config.get_ports(), &[6543]);
        assert_eq!(config.get_user(), Some("override user"));
        assert_eq!(config.get_password(), Some("override password".as_bytes()));
        assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(10)));
        assert!(!config.get_keepalives());
        assert_eq!(config.get_channel_binding(), ChannelBinding::Require);
        assert_eq!(config.get_ssl_negotiation(), SslNegotiation::Direct);
        assert_eq!(config.get_load_balance_hosts(), LoadBalanceHosts::Random);
    }

    #[test]
    fn conninfo_keyvalue_detection_accepts_tokio_postgres_options() {
        let input = "keepalives=0 channel_binding=require sslnegotiation=direct load_balance_hosts=random dbname=template1 host=oldhost";
        assert_eq!(build_conninfo(input), input);

        let params = ConnParams {
            host: Some("newhost".to_string()),
            port: None,
            user: None,
            password: None,
        };
        let result = build_conninfo_with_params(input, &params);
        let config = result.parse::<Config>().unwrap();

        assert_eq!(config.get_dbname(), Some("template1"));
        assert_eq!(config.get_hosts(), &[Host::Tcp("newhost".to_string())]);
        assert!(!config.get_keepalives());
        assert_eq!(config.get_channel_binding(), ChannelBinding::Require);
        assert_eq!(config.get_ssl_negotiation(), SslNegotiation::Direct);
        assert_eq!(config.get_load_balance_hosts(), LoadBalanceHosts::Random);
    }
}
