/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::collections::hash_map::HashMap;
use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::Future;
use futures::task::{Context, Poll};
use http::uri::{Authority, Scheme, Uri as Destination};
use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use hyper::rt::Executor;
use hyper_rustls::HttpsConnector as HyperRustlsHttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector as HyperHttpConnector;
use log::{info, warn};
use rustls::client::WebPkiServerVerifier;
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use tower_service::Service;

use crate::async_runtime::spawn_task;
use crate::hosts::replace_host;
use net_traits::proxy_config::ProxyConfig;

pub const BUF_SIZE: usize = 32768;

#[derive(Clone)]
pub struct ServoHttpConnector {
    inner: HyperHttpConnector,
    proxy_uri: Option<Destination>,
}

impl ServoHttpConnector {
    fn new() -> ServoHttpConnector {
        let mut inner = HyperHttpConnector::new();
        inner.enforce_http(false);
        inner.set_happy_eyeballs_timeout(None);
        ServoHttpConnector {
            inner,
            proxy_uri: None,
        }
    }

    fn with_proxy(proxy_url: &str) -> Result<ServoHttpConnector, String> {
        let mut inner = HyperHttpConnector::new();
        inner.enforce_http(false);
        inner.set_happy_eyeballs_timeout(None);

        let proxy_uri = proxy_url
            .parse::<Destination>()
            .map_err(|e| format!("Invalid proxy URL: {}", e))?;

        Ok(ServoHttpConnector {
            inner,
            proxy_uri: Some(proxy_uri),
        })
    }
}

tokio::task_local! {
    pub static SESSION_ID: Option<String>;
}

impl Service<Destination> for ServoHttpConnector {
    type Response = <HyperHttpConnector as Service<Destination>>::Response;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&mut self, dest: Destination) -> Self::Future {
        let mut inner = self.inner.clone();
        let proxy_uri = self.proxy_uri.clone();

        Box::pin(async move {
            // If proxy is configured, connect to proxy instead of destination
            if let Some(ref proxy) = proxy_uri {
                info!("Routing request to {} through proxy {}", dest, proxy);
                eprintln!("[CONNECTOR] Routing request to {} through proxy {}", dest, proxy);

                // Connect to the proxy server
                let mut stream = inner
                    .call(proxy.clone())
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                // If destination is HTTPS, we need to establish a CONNECT tunnel
                if dest.scheme() == Some(&http::uri::Scheme::HTTPS) {
                    let host = dest.host().unwrap_or("");
                    let port = dest.port_u16().unwrap_or(443);

                    eprintln!("[CONNECTOR] HTTPS destination detected: {}:{}", host, port);
                    
                    // Check for session ID in task-local storage
                    let session_id_result = SESSION_ID.try_with(|id| id.clone());
                    eprintln!("[CONNECTOR] SESSION_ID task_local result: {:?}", session_id_result);
                    
                    let connect_req = if let Ok(Some(session_id)) = session_id_result {
                        eprintln!("[CONNECTOR] ✓ Injecting X-Session-ID: '{}' into CONNECT", session_id);
                        format!(
                            "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\nX-Session-ID: {}\r\n\r\n",
                            host, port, host, port, session_id
                        )
                    } else {
                        eprintln!("[CONNECTOR] ✗ WARNING: No session ID available for HTTPS CONNECT");
                        format!(
                            "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
                            host, port, host, port
                        )
                    };

                    // Send CONNECT request
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    // Access the inner TcpStream which implements Tokio's AsyncRead/AsyncWrite
                    stream
                        .inner_mut()
                        .write_all(connect_req.as_bytes())
                        .await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                    // Read response
                    let mut buf = [0u8; 4096];
                    let n = stream
                        .inner_mut()
                        .read(&mut buf)
                        .await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                    let response = String::from_utf8_lossy(&buf[..n]);
                    if !response.starts_with("HTTP/1.1 200")
                        && !response.starts_with("HTTP/1.0 200")
                    {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "Proxy CONNECT failed: {}",
                                response.lines().next().unwrap_or("Unknown")
                            ),
                        ))
                            as Box<dyn std::error::Error + Send + Sync>);
                    }

                    info!("CONNECT tunnel established to {}", dest);
                }

                Ok(stream)
            } else {
                // Perform host replacement when making the actual TCP connection.
                let mut new_dest = dest.clone();
                let mut parts = dest.into_parts();

                if let Some(auth) = parts.authority {
                    let host = auth.host();
                    let host = replace_host(host);

                    let authority = if let Some(port) = auth.port() {
                        format!("{}:{}", host, port.as_str())
                    } else {
                        (*host).to_string()
                    };

                    if let Ok(authority) = Authority::from_maybe_shared(authority) {
                        parts.authority = Some(authority);
                        if let Ok(dest) = Destination::from_parts(parts) {
                            new_dest = dest
                        }
                    }
                }

                inner
                    .call(new_dest)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
        })
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_ready(cx)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

pub type Connector = HyperRustlsHttpsConnector<ServoHttpConnector>;
pub type TlsConfig = ClientConfig;

#[derive(Clone, Debug, Default)]
struct CertificateErrorOverrideManagerInternal {
    /// A mapping of certificates and their hosts, which have seen certificate errors.
    /// This is used to later create an override in this [CertificateErrorOverrideManager].
    certificates_failing_to_verify: HashMap<ServerName<'static>, CertificateDer<'static>>,
    /// A list of certificates that should be accepted despite encountering verification
    /// errors.
    overrides: Vec<CertificateDer<'static>>,
}

/// This data structure is used to track certificate verification errors and overrides.
/// It tracks:
///  - A list of [Certificate]s with verification errors mapped by their [ServerName]
///  - A list of [Certificate]s for which to ignore verification errors.
#[derive(Clone, Debug, Default)]
pub struct CertificateErrorOverrideManager(Arc<Mutex<CertificateErrorOverrideManagerInternal>>);

impl CertificateErrorOverrideManager {
    pub fn new() -> Self {
        Self(Default::default())
    }

    /// Add a certificate to this manager's list of certificates for which to ignore
    /// validation errors.
    pub fn add_override(&self, certificate: &CertificateDer<'static>) {
        self.0.lock().unwrap().overrides.push(certificate.clone());
    }

    /// Given the a string representation of a sever host name, remove information about
    /// a [Certificate] with verification errors. If a certificate with
    /// verification errors was found, return it, otherwise None.
    pub(crate) fn remove_certificate_failing_verification(
        &self,
        host: &str,
    ) -> Option<CertificateDer<'static>> {
        let server_name = match ServerName::try_from(host) {
            Ok(name) => name.to_owned(),
            Err(error) => {
                warn!("Could not convert host string into RustTLS ServerName: {error:?}");
                return None;
            },
        };
        self.0
            .lock()
            .unwrap()
            .certificates_failing_to_verify
            .remove(&server_name)
    }
}

#[derive(Clone, Debug)]
pub enum CACertificates {
    Default,
    Override(RootCertStore),
}

/// Create a [TlsConfig] to use for managing a HTTP connection. This currently creates
/// a rustls [ClientConfig].
///
/// FIXME: The `ignore_certificate_errors` argument ignores all certificate errors. This
/// is used when running the WPT tests, because rustls currently rejects the WPT certificiate.
/// See <https://github.com/servo/servo/issues/30080>
pub fn create_tls_config(
    ca_certificates: CACertificates,
    ignore_certificate_errors: bool,
    override_manager: CertificateErrorOverrideManager,
) -> TlsConfig {
    let verifier = CertificateVerificationOverrideVerifier::new(
        ca_certificates,
        ignore_certificate_errors,
        override_manager,
    );
    rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth()
}

#[derive(Clone)]
struct TokioExecutor {}

impl<F> Executor<F> for TokioExecutor
where
    F: Future<Output = ()> + 'static + std::marker::Send,
{
    fn execute(&self, fut: F) {
        spawn_task(fut);
    }
}

#[derive(Debug)]
struct CertificateVerificationOverrideVerifier {
    webpki_verifier: Arc<WebPkiServerVerifier>,
    ignore_certificate_errors: bool,
    override_manager: CertificateErrorOverrideManager,
}

impl CertificateVerificationOverrideVerifier {
    fn new(
        ca_certficates: CACertificates,
        ignore_certificate_errors: bool,
        override_manager: CertificateErrorOverrideManager,
    ) -> Self {
        let root_cert_store = match ca_certficates {
            CACertificates::Default => rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            },
            CACertificates::Override(root_cert_store) => root_cert_store,
        };

        Self {
            // See https://github.com/rustls/rustls/blame/v/0.21.6/rustls/src/client/builder.rs#L141
            // This is the default verifier for Rustls that we are wrapping.
            webpki_verifier: WebPkiServerVerifier::builder(root_cert_store.into())
                .build()
                .unwrap(),
            ignore_certificate_errors,
            override_manager,
        }
    }
}

impl rustls::client::danger::ServerCertVerifier for CertificateVerificationOverrideVerifier {
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.webpki_verifier
            .verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.webpki_verifier
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.webpki_verifier.supported_verify_schemes()
    }

    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let error = match self.webpki_verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(result) => return Ok(result),
            Err(error) => error,
        };

        if self.ignore_certificate_errors {
            warn!("Ignoring certficate error: {error:?}");
            return Ok(rustls::client::danger::ServerCertVerified::assertion());
        }

        // If there's an override for this certificate, just accept it.
        for cert_with_exception in &*self.override_manager.0.lock().unwrap().overrides {
            if *end_entity == *cert_with_exception {
                return Ok(rustls::client::danger::ServerCertVerified::assertion());
            }
        }
        self.override_manager
            .0
            .lock()
            .unwrap()
            .certificates_failing_to_verify
            .insert(server_name.to_owned(), end_entity.clone().into_owned());
        Err(error)
    }
}

pub type BoxedBody = BoxBody<Bytes, hyper::Error>;

pub fn create_http_client(
    tls_config: TlsConfig,
    proxy_config: Option<ProxyConfig>,
) -> Client<Connector, BoxedBody> {
    // Create base HTTP connector with optional proxy support
    let base_connector = if let Some(ref config) = proxy_config {
        info!("Servo HTTP client configured with proxy: {}", config.server);
        if let Some(ref auth) = config.auth {
            info!("  Proxy authentication: {}:***", auth.username);
        }
        if !config.bypass_list.is_empty() {
            info!("  Bypass list: {:?}", config.bypass_list);
        }

        match ServoHttpConnector::with_proxy(&config.server) {
            Ok(connector) => {
                info!("✓ Proxy connector created successfully");
                connector
            },
            Err(e) => {
                warn!(
                    "Failed to create proxy connector: {}. Using direct connection.",
                    e
                );
                ServoHttpConnector::new()
            },
        }
    } else {
        ServoHttpConnector::new()
    };

    // Wrap with HTTPS/TLS support
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .wrap_connector(base_connector);

    Client::builder(TokioExecutor {})
        .http1_title_case_headers(true)
        .build(connector)
}
