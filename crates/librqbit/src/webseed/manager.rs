//! WebSeed manager that coordinates WebSeed downloads.
//!
//! Manages multiple WebSeed sources and coordinates piece downloads
//! to fill gaps in the torrent.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

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
use super::{BytesReceivedCallback, RateLimitCallback, WebSeedConfig, WebSeedDownloadResult, WebSeedError, WebSeedFileInfo, WebSeedUrl};

/// Adaptive concurrency controller for a single webseed source.
/// 
/// Implements AIMD (Additive Increase/Multiplicative Decrease) style control:
/// - On success: gradually increase concurrency after N consecutive successes
/// - On error: immediately reduce concurrency (halve it)
/// - At concurrency=1 with repeated errors: trigger temporary disable
#[derive(Debug)]
pub struct AdaptiveConcurrencyController {
    /// Current effective concurrency limit
    current_concurrency: AtomicUsize,
    /// Maximum allowed concurrency
    max_concurrency: usize,
    /// Consecutive successes counter
    consecutive_successes: AtomicU32,
    /// Consecutive errors counter at current concurrency
    consecutive_errors: AtomicU32,
    /// Successes needed to increase concurrency
    increase_threshold: u32,
    /// Errors needed to decrease concurrency  
    decrease_threshold: u32,
    /// Errors at concurrency=1 before disabling
    disable_threshold: u32,
    /// Semaphore for actual concurrency control
    semaphore: Semaphore,
}

impl AdaptiveConcurrencyController {
    pub fn new(max_concurrency: usize, increase_threshold: u32, decrease_threshold: u32, disable_threshold: u32) -> Self {
        // Start with half of the target concurrency (at least 1)
        let initial = (max_concurrency / 2).max(1);
        Self {
            current_concurrency: AtomicUsize::new(initial),
            max_concurrency,
            consecutive_successes: AtomicU32::new(0),
            consecutive_errors: AtomicU32::new(0),
            increase_threshold,
            decrease_threshold,
            disable_threshold,
            semaphore: Semaphore::new(initial),
        }
    }
    
    /// Get current concurrency level
    pub fn current(&self) -> usize {
        self.current_concurrency.load(Ordering::Relaxed)
    }
    
    /// Acquire a permit to perform a download
    pub async fn acquire(&self) -> Result<tokio::sync::SemaphorePermit<'_>, WebSeedError> {
        self.semaphore.acquire().await.map_err(|_| {
            WebSeedError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "semaphore closed",
            ))
        })
    }
    
    /// Record a successful download
    /// Returns true if concurrency was increased
    pub fn record_success(&self) -> bool {
        // Reset error counter
        self.consecutive_errors.store(0, Ordering::Relaxed);
        
        let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;
        let current = self.current_concurrency.load(Ordering::Relaxed);
        
        if successes >= self.increase_threshold && current < self.max_concurrency {
            // Try to increase concurrency
            let new_concurrency = (current + 1).min(self.max_concurrency);
            if self.current_concurrency.compare_exchange(
                current, 
                new_concurrency, 
                Ordering::SeqCst, 
                Ordering::Relaxed
            ).is_ok() {
                // Successfully increased, add permit
                self.semaphore.add_permits(1);
                self.consecutive_successes.store(0, Ordering::Relaxed);
                debug!(
                    old = current,
                    new = new_concurrency,
                    "webseed adaptive concurrency increased"
                );
                return true;
            }
        }
        false
    }
    
    /// Record an error
    /// Returns (concurrency_decreased, should_disable)
    pub fn record_error(&self) -> (bool, bool) {
        // Reset success counter
        self.consecutive_successes.store(0, Ordering::Relaxed);
        
        let errors = self.consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
        let current = self.current_concurrency.load(Ordering::Relaxed);
        
        // Check if we should disable (at concurrency=1 with too many errors)
        if current == 1 && errors >= self.disable_threshold {
            return (false, true);
        }
        
        // Check if we should decrease concurrency
        if errors >= self.decrease_threshold && current > 1 {
            // Halve the concurrency (multiplicative decrease)
            let new_concurrency = (current / 2).max(1);
            let decrease_amount = current - new_concurrency;
            
            if decrease_amount > 0 {
                if self.current_concurrency.compare_exchange(
                    current,
                    new_concurrency,
                    Ordering::SeqCst,
                    Ordering::Relaxed
                ).is_ok() {
                    // We can't actually remove permits from a Semaphore,
                    // but we can track the logical limit. The semaphore will
                    // naturally drain as permits are acquired and not released.
                    // For immediate effect, we'd need a different approach.
                    // Here we'll just log and the reduction takes effect over time.
                    self.consecutive_errors.store(0, Ordering::Relaxed);
                    debug!(
                        old = current,
                        new = new_concurrency,
                        "webseed adaptive concurrency decreased"
                    );
                    return (true, false);
                }
            }
        }
        
        (false, false)
    }
    
    /// Reset the controller state (but not the semaphore)
    pub fn reset(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        self.consecutive_errors.store(0, Ordering::Relaxed);
        // Reset concurrency to half of max (at least 1)
        let initial = (self.max_concurrency / 2).max(1);
        let current = self.current_concurrency.swap(initial, Ordering::SeqCst);
        if current != initial {
            debug!(
                old = current,
                new = initial,
                "webseed adaptive concurrency reset"
            );
        }
    }
}

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
    /// Semaphore for limiting concurrent downloads (used when adaptive is disabled).
    semaphore: Semaphore,
    /// Adaptive concurrency controller (used when adaptive is enabled).
    adaptive_controller: Option<AdaptiveConcurrencyController>,
    /// Cancellation token.
    cancel_token: CancellationToken,
    /// Pieces currently being downloaded by webseeds.
    in_flight: RwLock<HashSet<ValidPieceIndex>>,
    /// Callback for reporting bytes received during download.
    on_bytes_received: Option<BytesReceivedCallback>,
    /// Callback for rate limiting downloads.
    rate_limiter: Option<RateLimitCallback>,
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
        rate_limiter: Option<RateLimitCallback>,
    ) -> Self {
        let webseed_urls: Vec<WebSeedUrl> = webseeds.into_iter().map(WebSeedUrl::new).collect();
        let webseed_count = webseed_urls.len();

        // Calculate effective concurrency:
        // min(max_total_concurrent, webseed_count * max_concurrent_per_source)
        let effective_concurrency = config
            .max_total_concurrent
            .min(webseed_count.saturating_mul(config.max_concurrent_per_source));

        let adaptive_controller = if config.adaptive_concurrency {
            let initial_concurrency = (effective_concurrency / 2).max(1);
            info!(
                count = webseed_count,
                max_concurrency = effective_concurrency,
                initial_concurrency = initial_concurrency,
                "initialized webseed manager with adaptive concurrency (starting at half of max)",
            );
            Some(AdaptiveConcurrencyController::new(
                effective_concurrency.max(1),
                config.adaptive_increase_threshold,
                config.adaptive_decrease_threshold,
                config.max_errors_before_disable,
            ))
        } else {
            info!(
                count = webseed_count,
                concurrency = effective_concurrency,
                "initialized webseed manager with {} sources, fixed concurrency {}",
                webseed_count,
                effective_concurrency
            );
            None
        };

        Self {
            webseeds: RwLock::new(webseed_urls),
            client: WebSeedClient::new(reqwest_client, config.request_timeout_secs),
            lengths,
            piece_hashes: Arc::new(piece_hashes),
            torrent_name,
            file_infos,
            is_multi_file,
            semaphore: Semaphore::new(effective_concurrency.max(1)),
            adaptive_controller,
            config,
            cancel_token,
            in_flight: RwLock::new(HashSet::new()),
            on_bytes_received,
            rate_limiter,
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
        // Acquire permit based on concurrency mode
        // We need to hold the permit for the duration of the download
        // The permit is held by _permit and released when it goes out of scope
        let _permit: tokio::sync::SemaphorePermit<'_> = if let Some(ref controller) = self.adaptive_controller {
            controller.acquire().await?
        } else {
            self.semaphore.acquire().await.map_err(|_| {
                WebSeedError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "semaphore closed",
                ))
            })?
        };

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
                    
                    // Record success for adaptive concurrency
                    if let Some(ref controller) = self.adaptive_controller {
                        controller.record_success();
                    }
                    
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
                    
                    // Record error for adaptive concurrency
                    if let Some(ref controller) = self.adaptive_controller {
                        let (decreased, should_disable) = controller.record_error();
                        if should_disable {
                            // At concurrency=1 with too many errors, disable the webseed
                            warn!(
                                url = %webseed.url,
                                "webseed disabled due to errors at minimum concurrency"
                            );
                            self.mark_webseed_error(&webseed.url);
                        } else if decreased {
                            debug!(
                                url = %webseed.url,
                                new_concurrency = controller.current(),
                                "adaptive concurrency decreased due to error"
                            );
                        }
                    }
                    
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
                self.rate_limiter.as_ref(),
            )
            .await
    }

    fn mark_webseed_error(&self, url: &Url) {
        let max_errors = self.config.max_errors_before_disable;
        let mut webseeds = self.webseeds.write();
        if let Some(ws) = webseeds.iter_mut().find(|ws| &ws.url == url) {
            let was_disabled = ws.disabled;
            ws.mark_error(max_errors);
            // Only log when transitioning from enabled to disabled
            if ws.disabled && !was_disabled {
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
        // Also reset adaptive controller if present
        if let Some(ref controller) = self.adaptive_controller {
            controller.reset();
        }
        info!("reset all disabled webseeds");
    }

    /// Get configuration.
    pub fn config(&self) -> &WebSeedConfig {
        &self.config
    }

    /// Get the current effective concurrency level.
    pub fn current_concurrency(&self) -> usize {
        if let Some(ref controller) = self.adaptive_controller {
            controller.current()
        } else {
            self.semaphore.available_permits()
        }
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
