//! WebSeed manager that coordinates WebSeed downloads.
//!
//! Manages multiple WebSeed sources and coordinates piece downloads
//! to fill gaps in the torrent.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

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

/// Per-source concurrency state
#[derive(Debug)]
struct PerSourceState {
    /// Current active downloads for this source
    active: AtomicUsize,
    /// Allocated concurrency limit for this source
    limit: AtomicUsize,
}

impl PerSourceState {
    fn new(limit: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            limit: AtomicUsize::new(limit),
        }
    }
    
    fn try_acquire(&self) -> bool {
        loop {
            let active = self.active.load(Ordering::Relaxed);
            let limit = self.limit.load(Ordering::Relaxed);
            
            if active >= limit {
                return false;
            }
            
            if self.active.compare_exchange(
                active,
                active + 1,
                Ordering::SeqCst,
                Ordering::Relaxed
            ).is_ok() {
                return true;
            }
        }
    }
    
    fn release(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
    
    fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
    
    fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }
    
    fn set_limit(&self, new_limit: usize) {
        self.limit.store(new_limit, Ordering::Relaxed);
    }
}

/// Adaptive concurrency controller that manages per-source limits.
/// 
/// Implements AIMD (Additive Increase/Multiplicative Decrease) style control:
/// - On success: gradually increase concurrency after N consecutive successes
/// - On error: immediately reduce concurrency (halve it)
/// - At concurrency=1 with repeated errors: trigger temporary disable
/// 
/// Also enforces:
/// - Per-source maximum concurrency limit
/// - Fair distribution of total concurrency across available sources
pub struct AdaptiveConcurrencyController {
    /// Per-source concurrency state, keyed by URL string
    per_source: RwLock<HashMap<String, Arc<PerSourceState>>>,
    /// Total current concurrency limit (adaptive)
    current_total_concurrency: AtomicUsize,
    /// Currently active downloads across all sources
    total_active_downloads: AtomicUsize,
    /// Maximum total concurrency allowed
    max_total_concurrency: usize,
    /// Maximum concurrency per single source
    max_per_source: usize,
    /// Consecutive successes counter
    consecutive_successes: AtomicU32,
    /// Consecutive errors counter
    consecutive_errors: AtomicU32,
    /// Successes needed to increase concurrency
    increase_threshold: u32,
    /// Errors needed to decrease concurrency  
    decrease_threshold: u32,
    /// Errors at concurrency=1 before disabling
    disable_threshold: u32,
    /// Last error record time (epoch millis for atomic storage)
    last_error_millis: AtomicU64,
    /// Creation instant for calculating elapsed time
    start_instant: Instant,
    /// Debounce interval in milliseconds
    debounce_millis: u64,
}

impl AdaptiveConcurrencyController {
    /// Debounce interval for error counting: 10 seconds
    const ERROR_DEBOUNCE_MILLIS: u64 = 10_000;
    
    pub fn new(
        max_total_concurrency: usize, 
        max_per_source: usize,
        increase_threshold: u32, 
        decrease_threshold: u32, 
        disable_threshold: u32,
    ) -> Self {
        // Start with half of the total concurrency (at least 1)
        let initial = (max_total_concurrency / 2).max(1);
        
        Self {
            per_source: RwLock::new(HashMap::new()),
            current_total_concurrency: AtomicUsize::new(initial),
            total_active_downloads: AtomicUsize::new(0),
            max_total_concurrency,
            max_per_source,
            consecutive_successes: AtomicU32::new(0),
            consecutive_errors: AtomicU32::new(0),
            increase_threshold,
            decrease_threshold,
            disable_threshold,
            last_error_millis: AtomicU64::new(0),
            start_instant: Instant::now(),
            debounce_millis: Self::ERROR_DEBOUNCE_MILLIS,
        }
    }
    
    /// Initialize or get per-source state
    fn get_or_create_source(&self, url: &Url) -> Arc<PerSourceState> {
        let key = url.to_string();
        
        // Fast path: check if already exists
        {
            let sources = self.per_source.read();
            if let Some(state) = sources.get(&key) {
                return Arc::clone(state);
            }
        }
        
        // Slow path: create new entry
        let mut sources = self.per_source.write();
        sources.entry(key)
            .or_insert_with(|| Arc::new(PerSourceState::new(0)))
            .clone()
    }
    
    /// Recalculate and redistribute concurrency limits across available sources
    pub fn redistribute_concurrency(&self, available_urls: &[&Url]) {
        let total = self.current_total_concurrency.load(Ordering::Relaxed);
        let source_count = available_urls.len();
        
        if source_count == 0 {
            return;
        }
        
        // Calculate per-source limit: total / source_count, but capped by max_per_source
        let per_source_limit = (total / source_count).min(self.max_per_source).max(1);
        
        // Distribute any remainder to first few sources
        let remainder = total.saturating_sub(per_source_limit * source_count);
        
        let mut sources = self.per_source.write();
        
        // First, set all sources to 0 (will reset non-available ones)
        for state in sources.values() {
            state.set_limit(0);
        }
        
        // Then set limits for available sources
        for (i, url) in available_urls.iter().enumerate() {
            let key = url.to_string();
            let extra = if i < remainder { 1 } else { 0 };
            let limit = (per_source_limit + extra).min(self.max_per_source);
            
            sources.entry(key)
                .or_insert_with(|| Arc::new(PerSourceState::new(0)))
                .set_limit(limit);
        }
        
        debug!(
            total_concurrency = total,
            source_count = source_count,
            per_source_limit = per_source_limit,
            "redistributed concurrency across sources"
        );
    }
    
    /// Check if debounce period has passed since last error record
    fn can_record_error(&self) -> bool {
        let now_millis = self.start_instant.elapsed().as_millis() as u64;
        let last = self.last_error_millis.load(Ordering::Relaxed);
        now_millis.saturating_sub(last) >= self.debounce_millis
    }
    
    /// Mark the current time as the last error record time
    fn mark_error_recorded(&self) {
        let now_millis = self.start_instant.elapsed().as_millis() as u64;
        self.last_error_millis.store(now_millis, Ordering::Relaxed);
    }
    
    /// Get current total concurrency level
    pub fn current(&self) -> usize {
        self.current_total_concurrency.load(Ordering::Relaxed)
    }
    
    /// Get number of active downloads across all sources
    pub fn active(&self) -> usize {
        self.total_active_downloads.load(Ordering::Relaxed)
    }
    
    /// Try to acquire a slot for a specific source. Returns true if acquired.
    pub fn try_acquire(&self, url: &Url) -> bool {
        let source_state = self.get_or_create_source(url);
        
        // Check per-source limit
        if !source_state.try_acquire() {
            return false;
        }
        
        // Check total limit
        loop {
            let total_active = self.total_active_downloads.load(Ordering::Relaxed);
            let total_limit = self.current_total_concurrency.load(Ordering::Relaxed);
            
            if total_active >= total_limit {
                // Release per-source slot since we can't proceed
                source_state.release();
                return false;
            }
            
            if self.total_active_downloads.compare_exchange(
                total_active,
                total_active + 1,
                Ordering::SeqCst,
                Ordering::Relaxed
            ).is_ok() {
                return true;
            }
            // CAS failed, retry total check
        }
    }
    
    /// Release a download slot for a specific source
    pub fn release(&self, url: &Url) {
        let source_state = self.get_or_create_source(url);
        source_state.release();
        self.total_active_downloads.fetch_sub(1, Ordering::SeqCst);
    }
    
    /// Get per-source concurrency info
    pub fn get_source_info(&self, url: &Url) -> (usize, usize) {
        let source_state = self.get_or_create_source(url);
        (source_state.active(), source_state.limit())
    }
    
    /// Record a successful download
    /// Returns true if concurrency was increased
    pub fn record_success(&self) -> bool {
        // Reset error counter on success
        self.consecutive_errors.store(0, Ordering::Relaxed);
        
        let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;
        let current = self.current_total_concurrency.load(Ordering::Relaxed);
        
        if successes >= self.increase_threshold && current < self.max_total_concurrency {
            // Try to increase concurrency
            let new_concurrency = (current + 1).min(self.max_total_concurrency);
            if self.current_total_concurrency.compare_exchange(
                current, 
                new_concurrency, 
                Ordering::SeqCst, 
                Ordering::Relaxed
            ).is_ok() {
                // Reset success counter after adjustment
                self.consecutive_successes.store(0, Ordering::Relaxed);
                println!("[webseed] Adaptive concurrency INCREASED: {} -> {} (after {} consecutive successes)", 
                    current, new_concurrency, self.increase_threshold);
                return true;
            }
        }
        false
    }
    
    /// Record an error
    /// Returns (concurrency_decreased, should_disable)
    pub fn record_error(&self) -> (bool, bool) {
        // Debounce: only count one error per 10 seconds to avoid all threads failing at once
        if !self.can_record_error() {
            return (false, false);
        }
        self.mark_error_recorded();
        
        // Reset success counter on error
        self.consecutive_successes.store(0, Ordering::Relaxed);
        
        let errors = self.consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
        let current = self.current_total_concurrency.load(Ordering::Relaxed);
        
        if current == 1 && errors >= self.disable_threshold {
            if errors == self.disable_threshold {
                println!("[webseed] Adaptive concurrency: DISABLE triggered (errors={} at concurrency=1)", errors);
            }
            return (false, true);
        }
        
        // Check if we should decrease concurrency
        if errors >= self.decrease_threshold && current > 1 {
            // Halve the concurrency (multiplicative decrease)
            let new_concurrency = (current / 2).max(1);
            
            if self.current_total_concurrency.compare_exchange(
                current,
                new_concurrency,
                Ordering::SeqCst,
                Ordering::Relaxed
            ).is_ok() {
                // Reset error counter after adjustment
                self.consecutive_errors.store(0, Ordering::Relaxed);
                println!("[webseed] Adaptive concurrency DECREASED: {} -> {} (after {} consecutive errors)", 
                    current, new_concurrency, self.decrease_threshold);
                return (true, false);
            }
        }
        
        (false, false)
    }
    
    /// Reset the controller state
    pub fn reset(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        self.consecutive_errors.store(0, Ordering::Relaxed);
        // Reset concurrency to half of max (at least 1)
        let initial = (self.max_total_concurrency / 2).max(1);
        let current = self.current_total_concurrency.swap(initial, Ordering::SeqCst);
        // Also reset debounce timer
        self.mark_error_recorded();
        if current != initial {
            println!("[webseed] Adaptive concurrency RESET: {} -> {}", current, initial);
        }
    }
}

impl std::fmt::Debug for AdaptiveConcurrencyController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdaptiveConcurrencyController")
            .field("current_total_concurrency", &self.current_total_concurrency.load(Ordering::Relaxed))
            .field("total_active_downloads", &self.total_active_downloads.load(Ordering::Relaxed))
            .field("max_total_concurrency", &self.max_total_concurrency)
            .field("max_per_source", &self.max_per_source)
            .field("consecutive_successes", &self.consecutive_successes.load(Ordering::Relaxed))
            .field("consecutive_errors", &self.consecutive_errors.load(Ordering::Relaxed))
            .finish()
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

        // Calculate effective max concurrency (capped by total and per-source limits)
        let effective_max_concurrency = config
            .max_total_concurrent
            .min(webseed_count.saturating_mul(config.max_concurrent_per_source));

        let adaptive_controller = if config.adaptive_concurrency {
            let controller = AdaptiveConcurrencyController::new(
                config.max_total_concurrent,
                config.max_concurrent_per_source,
                config.adaptive_increase_threshold,
                config.adaptive_decrease_threshold,
                config.max_errors_before_disable,
            );
            
            // Initialize per-source limits
            let available_urls: Vec<&Url> = webseed_urls.iter().map(|ws| &ws.url).collect();
            controller.redistribute_concurrency(&available_urls);
            
            let initial_concurrency = controller.current();
            info!(
                count = webseed_count,
                max_total = config.max_total_concurrent,
                max_per_source = config.max_concurrent_per_source,
                initial_concurrency = initial_concurrency,
                "initialized webseed manager with adaptive concurrency (per-source limits enforced)",
            );
            Some(controller)
        } else {
            info!(
                count = webseed_count,
                concurrency = effective_max_concurrency,
                "initialized webseed manager with {} sources, fixed concurrency {}",
                webseed_count,
                effective_max_concurrency
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
            semaphore: Semaphore::new(effective_max_concurrency.max(1)),
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
    /// Enforces per-source concurrency limits.
    pub async fn download_piece(
        &self,
        piece_index: ValidPieceIndex,
    ) -> Result<WebSeedDownloadResult, WebSeedError> {
        // Refresh disabled webseeds that have passed cooldown and redistribute concurrency
        self.refresh_disabled_webseeds();
        
        // Update concurrency distribution when sources change
        if let Some(ref controller) = self.adaptive_controller {
            let webseeds = self.webseeds.read();
            let available_urls: Vec<&Url> = webseeds.iter()
                .filter(|ws| !ws.disabled)
                .map(|ws| &ws.url)
                .collect();
            controller.redistribute_concurrency(&available_urls);
        }

        // Guard for releasing the slot on drop
        struct SourceGuard<'a> {
            controller: Option<&'a AdaptiveConcurrencyController>,
            url: Option<Url>,
        }
        
        impl Drop for SourceGuard<'_> {
            fn drop(&mut self) {
                if let (Some(controller), Some(url)) = (self.controller, &self.url) {
                    controller.release(url);
                }
            }
        }

        let _permit: Option<tokio::sync::SemaphorePermit<'_>>;
        
        // For non-adaptive mode, use simple semaphore
        if self.adaptive_controller.is_none() {
            _permit = Some(self.semaphore.acquire().await.map_err(|_| {
                WebSeedError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "semaphore closed",
                ))
            })?);
        } else {
            _permit = None;
        }

        if self.cancel_token.is_cancelled() {
            return Err(WebSeedError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "cancelled",
            )));
        }

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

        // For adaptive mode, try to acquire a slot from any available source
        if let Some(ref controller) = self.adaptive_controller {
            // Try each webseed, attempting to acquire a slot
            loop {
                if self.cancel_token.is_cancelled() {
                    return Err(WebSeedError::Io(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "cancelled",
                    )));
                }
                
                // Try to find a webseed with available slot
                for webseed in &webseeds {
                    if controller.try_acquire(&webseed.url) {
                        let mut guard = SourceGuard {
                            controller: Some(controller),
                            url: Some(webseed.url.clone()),
                        };
                        
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
                                controller.record_success();
                                
                                debug!(
                                    piece = piece_index.get(),
                                    url = %webseed.url,
                                    len = data.len(),
                                    "webseed download successful"
                                );

                                // Guard will release on drop
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
                                
                                let (decreased, should_disable) = controller.record_error();
                                if should_disable {
                                    warn!(
                                        url = %webseed.url,
                                        "webseed disabled due to errors at minimum concurrency"
                                    );
                                    self.mark_webseed_error(&webseed.url);
                                } else if decreased {
                                    // Redistribute after concurrency decrease
                                    let webseeds = self.webseeds.read();
                                    let available_urls: Vec<&Url> = webseeds.iter()
                                        .filter(|ws| !ws.disabled)
                                        .map(|ws| &ws.url)
                                        .collect();
                                    controller.redistribute_concurrency(&available_urls);
                                }
                                
                                // Clear guard URL to prevent double release
                                guard.url = None;
                                controller.release(&webseed.url);
                                
                                // Try next webseed
                                continue;
                            }
                        }
                    }
                }
                
                // No slot available on any source, wait and retry
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        } else {
            // Non-adaptive mode: simple sequential try
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
                
                // Redistribute concurrency when a source is disabled
                if let Some(ref controller) = self.adaptive_controller {
                    let available_urls: Vec<&Url> = webseeds.iter()
                        .filter(|ws| !ws.disabled)
                        .map(|ws| &ws.url)
                        .collect();
                    controller.redistribute_concurrency(&available_urls);
                }
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
        // Also reset adaptive controller and redistribute
        if let Some(ref controller) = self.adaptive_controller {
            controller.reset();
            let available_urls: Vec<&Url> = webseeds.iter()
                .filter(|ws| !ws.disabled)
                .map(|ws| &ws.url)
                .collect();
            controller.redistribute_concurrency(&available_urls);
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

    /// Get the number of active downloads.
    pub fn active_downloads(&self) -> usize {
        if let Some(ref controller) = self.adaptive_controller {
            controller.active()
        } else {
            // For fixed semaphore, we don't track this
            0
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
