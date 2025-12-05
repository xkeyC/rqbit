//! WebSeed manager that coordinates WebSeed downloads.
//!
//! Manages multiple WebSeed sources and coordinates piece downloads
//! to fill gaps in the torrent.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use reqwest::Client;
use sha1w::{ISha1, Sha1};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};
use url::Url;

use librqbit_core::hash_id::Id20;
use librqbit_core::lengths::{Lengths, ValidPieceIndex};

use super::client::WebSeedClient;
use super::{BytesReceivedCallback, WebSeedConfig, WebSeedDownloadResult, WebSeedError, WebSeedFileInfo, WebSeedUrl};

/// Manages WebSeed downloads for a torrent.
pub struct WebSeedManager {
    /// The WebSeed URLs.
    webseeds: RwLock<Vec<WebSeedUrl>>,
    /// The HTTP client.
    client: WebSeedClient,
    /// Torrent lengths info.
    lengths: Lengths,
    /// The expected piece hashes.
    piece_hashes: Arc<Vec<Id20>>,
    /// Torrent name.
    torrent_name: Option<String>,
    /// File information for multi-file torrents.
    file_infos: Vec<WebSeedFileInfo>,
    /// Whether this is a multi-file torrent.
    is_multi_file: bool,
    /// Configuration.
    config: WebSeedConfig,
    /// Semaphore for limiting concurrent downloads.
    semaphore: Semaphore,
    /// Cancellation token.
    cancel_token: CancellationToken,
    /// Pieces currently being downloaded by webseeds.
    in_flight: RwLock<HashSet<ValidPieceIndex>>,
    /// Callback for reporting bytes received during download.
    on_bytes_received: Option<BytesReceivedCallback>,
}

impl WebSeedManager {
    /// Create a new WebSeed manager.
    pub fn new(
        webseeds: Vec<Url>,
        reqwest_client: Client,
        lengths: Lengths,
        piece_hashes: Vec<Id20>,
        torrent_name: Option<String>,
        file_infos: Vec<WebSeedFileInfo>,
        is_multi_file: bool,
        config: WebSeedConfig,
        cancel_token: CancellationToken,
        on_bytes_received: Option<BytesReceivedCallback>,
    ) -> Self {
        let webseed_urls: Vec<WebSeedUrl> = webseeds.into_iter().map(WebSeedUrl::new).collect();
        let webseed_count = webseed_urls.len();

        // Calculate effective concurrency:
        // min(max_total_concurrent, webseed_count * max_concurrent_per_source)
        let effective_concurrency = config
            .max_total_concurrent
            .min(webseed_count.saturating_mul(config.max_concurrent_per_source));

        info!(
            count = webseed_count,
            concurrency = effective_concurrency,
            "initialized webseed manager with {} sources, effective concurrency {}",
            webseed_count,
            effective_concurrency
        );

        Self {
            webseeds: RwLock::new(webseed_urls),
            client: WebSeedClient::new(reqwest_client, config.request_timeout_secs),
            lengths,
            piece_hashes: Arc::new(piece_hashes),
            torrent_name,
            file_infos,
            is_multi_file,
            semaphore: Semaphore::new(effective_concurrency.max(1)),
            config,
            cancel_token,
            in_flight: RwLock::new(HashSet::new()),
            on_bytes_received,
        }
    }

    /// Check if there are any enabled WebSeeds, considering cooldown periods.
    pub fn has_webseeds(&self) -> bool {
        let cooldown = self.config.disable_cooldown_secs;
        self.webseeds
            .read()
            .iter()
            .any(|ws| !ws.disabled || ws.can_retry(cooldown))
    }

    /// Get the number of available WebSeeds.
    pub fn available_webseed_count(&self) -> usize {
        let cooldown = self.config.disable_cooldown_secs;
        self.webseeds
            .read()
            .iter()
            .filter(|ws| !ws.disabled || ws.can_retry(cooldown))
            .count()
    }

    /// Re-enable webseeds that have passed their cooldown period.
    fn refresh_disabled_webseeds(&self) {
        let cooldown = self.config.disable_cooldown_secs;
        let mut webseeds = self.webseeds.write();
        for ws in webseeds.iter_mut() {
            if ws.disabled && ws.can_retry(cooldown) {
                info!(url = %ws.url, "re-enabling webseed after cooldown");
                ws.reset();
            }
        }
    }

    /// Mark a piece as being downloaded by webseed.
    pub fn mark_in_flight(&self, piece: ValidPieceIndex) -> bool {
        self.in_flight.write().insert(piece)
    }

    /// Unmark a piece from webseed download.
    pub fn unmark_in_flight(&self, piece: ValidPieceIndex) {
        self.in_flight.write().remove(&piece);
    }

    /// Check if a piece is being downloaded by webseed.
    pub fn is_in_flight(&self, piece: ValidPieceIndex) -> bool {
        self.in_flight.read().contains(&piece)
    }

    /// Download a piece from WebSeeds.
    ///
    /// Tries each available WebSeed until one succeeds or all fail.
    pub async fn download_piece(
        &self,
        piece_index: ValidPieceIndex,
    ) -> Result<WebSeedDownloadResult, WebSeedError> {
        // Acquire semaphore permit
        let _permit = self.semaphore.acquire().await.map_err(|_| {
            WebSeedError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "semaphore closed",
            ))
        })?;

        if self.cancel_token.is_cancelled() {
            return Err(WebSeedError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "cancelled",
            )));
        }

        // Refresh disabled webseeds that have passed cooldown
        self.refresh_disabled_webseeds();

        // Get a list of enabled webseeds
        let webseeds: Vec<WebSeedUrl> = self
            .webseeds
            .read()
            .iter()
            .filter(|ws| !ws.disabled)
            .cloned()
            .collect();

        if webseeds.is_empty() {
            return Err(WebSeedError::NoWebSeeds);
        }

        let expected_hash = self.piece_hashes.get(piece_index.get() as usize).copied();

        // Try each webseed until one succeeds
        for webseed in &webseeds {
            trace!(
                piece = piece_index.get(),
                url = %webseed.url,
                "attempting webseed download"
            );

            match self.try_download_piece(webseed, piece_index).await {
                Ok(data) => {
                    // Verify piece hash
                    if let Some(expected) = expected_hash {
                        let mut hasher = Sha1::new();
                        hasher.update(&data);
                        let computed = Id20::new(hasher.finish());

                        if computed != expected {
                            warn!(
                                piece = piece_index.get(),
                                url = %webseed.url,
                                "webseed piece hash mismatch"
                            );
                            self.mark_webseed_error(&webseed.url);
                            continue;
                        }
                    }

                    // Success!
                    self.mark_webseed_success(&webseed.url);
                    debug!(
                        piece = piece_index.get(),
                        url = %webseed.url,
                        len = data.len(),
                        "webseed download successful"
                    );

                    return Ok(WebSeedDownloadResult::Success {
                        piece_index,
                        data,
                    });
                }
                Err(e) => {
                    debug!(
                        piece = piece_index.get(),
                        url = %webseed.url,
                        error = %e,
                        "webseed download failed"
                    );
                    self.mark_webseed_error(&webseed.url);
                }
            }
        }

        // All webseeds failed
        Err(WebSeedError::NoWebSeeds)
    }

    async fn try_download_piece(
        &self,
        webseed: &WebSeedUrl,
        piece_index: ValidPieceIndex,
    ) -> Result<Bytes, WebSeedError> {
        self.client
            .download_piece(
                webseed,
                piece_index,
                &self.lengths,
                self.torrent_name.as_deref(),
                &self.file_infos,
                self.is_multi_file,
                self.on_bytes_received.as_ref(),
            )
            .await
    }

    fn mark_webseed_error(&self, url: &Url) {
        let max_errors = self.config.max_errors_before_disable;
        let mut webseeds = self.webseeds.write();
        if let Some(ws) = webseeds.iter_mut().find(|ws| &ws.url == url) {
            ws.mark_error(max_errors);
            if ws.disabled {
                warn!(
                    url = %url,
                    error_count = ws.error_count,
                    cooldown_secs = self.config.disable_cooldown_secs,
                    "webseed disabled due to too many errors, will retry after cooldown"
                );
            }
        }
    }

    fn mark_webseed_success(&self, url: &Url) {
        let mut webseeds = self.webseeds.write();
        if let Some(ws) = webseeds.iter_mut().find(|ws| &ws.url == url) {
            ws.mark_success();
        }
    }

    /// Re-enable all disabled webseeds.
    pub fn reset_disabled_webseeds(&self) {
        let mut webseeds = self.webseeds.write();
        for ws in webseeds.iter_mut() {
            ws.reset();
        }
        info!("reset all disabled webseeds");
    }

    /// Get configuration.
    pub fn config(&self) -> &WebSeedConfig {
        &self.config
    }

    /// Get status of all webseeds.
    pub fn get_webseed_status(&self) -> Vec<WebSeedStatus> {
        self.webseeds
            .read()
            .iter()
            .map(|ws| WebSeedStatus {
                url: ws.url.to_string(),
                disabled: ws.disabled,
                error_count: ws.error_count,
                can_retry: ws.can_retry(self.config.disable_cooldown_secs),
            })
            .collect()
    }
}

/// Status information for a webseed.
#[derive(Debug, Clone)]
pub struct WebSeedStatus {
    pub url: String,
    pub disabled: bool,
    pub error_count: u32,
    pub can_retry: bool,
}
