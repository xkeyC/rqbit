//! BEP-19: WebSeed - HTTP/FTP Seeding (GetRight style)
//!
//! This module implements HTTP seeding support for BitTorrent downloads.
//! WebSeeds are HTTP servers that can provide pieces of a torrent file,
//! acting as a permanent unchoked seed.
//!
//! See: https://www.bittorrent.org/beps/bep_0019.html

mod client;
mod manager;

pub use client::WebSeedClient;
pub use manager::{WebSeedManager, WebSeedStatus};

use librqbit_core::lengths::ValidPieceIndex;
use std::sync::Arc;
use std::time::Instant;
use url::Url;

/// Callback type for reporting bytes downloaded from webseed in real-time.
/// This is called during the download process, not after completion.
pub type BytesReceivedCallback = Arc<dyn Fn(u64) + Send + Sync>;

/// A WebSeed source URL.
#[derive(Debug, Clone)]
pub struct WebSeedUrl {
    /// The base URL for the WebSeed.
    pub url: Url,
    /// Whether this URL has been disabled due to errors.
    pub disabled: bool,
    /// Number of consecutive errors.
    pub error_count: u32,
    /// Time when this URL was disabled (for cooldown tracking).
    pub disabled_at: Option<Instant>,
}

impl WebSeedUrl {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            disabled: false,
            error_count: 0,
            disabled_at: None,
        }
    }

    pub fn mark_error(&mut self, max_errors: u32) {
        self.error_count += 1;
        // Disable after max_errors consecutive errors
        if self.error_count >= max_errors {
            self.disabled = true;
            self.disabled_at = Some(Instant::now());
        }
    }

    pub fn mark_success(&mut self) {
        self.error_count = 0;
    }

    /// Reset error count and re-enable the URL.
    pub fn reset(&mut self) {
        self.error_count = 0;
        self.disabled = false;
        self.disabled_at = None;
    }

    /// Check if cooldown has passed and the URL can be re-enabled.
    pub fn can_retry(&self, cooldown_secs: u64) -> bool {
        if !self.disabled {
            return true;
        }
        match self.disabled_at {
            Some(disabled_time) => {
                disabled_time.elapsed() >= std::time::Duration::from_secs(cooldown_secs)
            }
            None => true,
        }
    }
}

/// Result of a WebSeed download operation.
#[derive(Debug)]
pub enum WebSeedDownloadResult {
    /// Successfully downloaded piece data.
    Success {
        piece_index: ValidPieceIndex,
        data: bytes::Bytes,
    },
    /// The WebSeed returned an error.
    Error {
        piece_index: ValidPieceIndex,
        error: WebSeedError,
    },
    /// The piece is not available from this WebSeed.
    NotAvailable { piece_index: ValidPieceIndex },
}

/// Errors that can occur during WebSeed operations.
#[derive(Debug, thiserror::Error)]
pub enum WebSeedError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("Invalid byte range response")]
    InvalidByteRange,

    #[error("SHA1 checksum mismatch")]
    ChecksumMismatch,

    #[error("Server returned error status: {0}")]
    ServerError(reqwest::StatusCode),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WebSeed disabled due to too many errors")]
    Disabled,

    #[error("No WebSeeds available")]
    NoWebSeeds,
}

/// Configuration for WebSeed downloads.
#[derive(Debug, Clone)]
pub struct WebSeedConfig {
    /// Maximum number of concurrent WebSeed connections per webseed source.
    /// Total concurrency = max_concurrent_per_source * available_webseeds
    pub max_concurrent_per_source: usize,
    /// Total maximum concurrent downloads across all webseeds.
    pub max_total_concurrent: usize,
    /// Timeout for HTTP requests in seconds.
    pub request_timeout_secs: u64,
    /// Whether to prefer WebSeeds over peers for large gaps.
    pub prefer_for_large_gaps: bool,
    /// Minimum gap size (in pieces) to trigger WebSeed usage.
    pub min_gap_for_webseed: u32,
    /// Maximum consecutive errors before disabling a WebSeed URL.
    pub max_errors_before_disable: u32,
    /// Cooldown period in seconds before retrying a disabled webseed.
    pub disable_cooldown_secs: u64,
}

impl Default for WebSeedConfig {
    fn default() -> Self {
        Self {
            max_concurrent_per_source: 2,
            max_total_concurrent: 8,
            request_timeout_secs: 30,
            prefer_for_large_gaps: true,
            min_gap_for_webseed: 10,
            max_errors_before_disable: 5,
            disable_cooldown_secs: 300,
        }
    }
}

/// File information for multi-file torrent webseed downloads.
#[derive(Debug, Clone)]
pub struct WebSeedFileInfo {
    /// Relative path within the torrent.
    pub path: String,
    /// Offset of this file within the torrent's data stream.
    pub offset: u64,
    /// Length of the file.
    pub length: u64,
}
