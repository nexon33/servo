/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Proxy configuration types

use serde::{Deserialize, Serialize};
use std::fmt;

/// Proxy authentication credentials
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ProxyAuth {
    /// Username for proxy authentication
    pub username: String,
    /// Password for proxy authentication
    pub password: String,
}

/// Proxy configuration for a WebView or browsing context
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ProxyConfig {
    /// Proxy server URL (e.g., "http://proxy.com:8080", "socks5://proxy.com:1080")
    pub server: String,

    /// Optional authentication credentials
    pub auth: Option<ProxyAuth>,

    /// List of domains/hosts to bypass the proxy for (e.g., ["localhost", "*.local"])
    pub bypass_list: Vec<String>,
}

impl ProxyConfig {
    /// Create a new proxy configuration with just a server URL
    pub fn new(server: impl Into<String>) -> Self {
        Self {
            server: server.into(),
            auth: None,
            bypass_list: Vec::new(),
        }
    }

    /// Add authentication to this proxy configuration
    pub fn with_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some(ProxyAuth {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    /// Add bypass list to this proxy configuration
    pub fn with_bypass_list(mut self, bypass_list: Vec<String>) -> Self {
        self.bypass_list = bypass_list;
        self
    }
}

impl fmt::Display for ProxyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.server)?;
        if self.auth.is_some() {
            write!(f, " (authenticated)")?;
        }
        if !self.bypass_list.is_empty() {
            write!(f, " [bypass: {}]", self.bypass_list.join(", "))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_config_new() {
        let config = ProxyConfig::new("http://proxy.example.com:8080");
        assert_eq!(config.server, "http://proxy.example.com:8080");
        assert!(config.auth.is_none());
        assert!(config.bypass_list.is_empty());
    }

    #[test]
    fn test_proxy_config_with_auth() {
        let config = ProxyConfig::new("socks5://proxy.example.com:1080").with_auth("user", "pass");

        assert_eq!(config.server, "socks5://proxy.example.com:1080");
        assert!(config.auth.is_some());
        let auth = config.auth.unwrap();
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "pass");
    }

    #[test]
    fn test_proxy_config_with_bypass() {
        let config = ProxyConfig::new("http://proxy.example.com:8080")
            .with_bypass_list(vec!["localhost".to_string(), "*.local".to_string()]);

        assert_eq!(config.bypass_list.len(), 2);
        assert!(config.bypass_list.contains(&"localhost".to_string()));
    }
}
