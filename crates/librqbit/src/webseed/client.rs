//! WebSeed HTTP client implementation.
//!
//! Handles HTTP byte-range requests to WebSeed servers.

use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use tracing::{debug, trace, warn};
use url::Url;

use librqbit_core::lengths::{Lengths, ValidPieceIndex};

use super::{WebSeedError, WebSeedUrl};

/// HTTP client for WebSeed downloads.
#[derive(Clone)]
pub struct WebSeedClient {
    client: Client,
    timeout: Duration,
}

impl WebSeedClient {
    /// Create a new WebSeed client with the given reqwest client.
    pub fn new(client: Client, timeout_secs: u64) -> Self {
        Self {
            client,
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Build a URL for downloading a specific byte range.
    ///
    /// For single-file torrents: base_url points directly to the file.
    /// For multi-file torrents: base_url is the root folder, and we append the file path.
    pub fn build_url(
        base_url: &Url,
        torrent_name: Option<&str>,
        file_path: Option<&str>,
        is_multi_file: bool,
    ) -> Result<Url, WebSeedError> {
        let mut url = base_url.clone();

        // If URL ends with /, append torrent name
        if url.path().ends_with('/') {
            if let Some(name) = torrent_name {
                url = url.join(name)?;
            }
        }

        // For multi-file torrents, append the file path
        if is_multi_file {
            if let Some(path) = file_path {
                // Ensure we have a trailing slash before appending
                let current_path = url.path().to_string();
                if !current_path.ends_with('/') {
                    url.set_path(&format!("{}/", current_path));
                }
                url = url.join(path)?;
            }
        }

        Ok(url)
    }

    /// Download a byte range from a WebSeed URL.
    ///
    /// Uses HTTP Range header to request specific bytes.
    pub async fn download_range(
        &self,
        url: &Url,
        range: Range<u64>,
    ) -> Result<Bytes, WebSeedError> {
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);

        trace!(
            url = %url,
            range = %range_header,
            "downloading byte range from webseed"
        );

        let response = self
            .client
            .get(url.clone())
            .header("Range", &range_header)
            .timeout(self.timeout)
            .send()
            .await?;

        let status = response.status();

        // Check for success - either 200 (full content) or 206 (partial content)
        if status == StatusCode::PARTIAL_CONTENT {
            // Server supports byte ranges
            let bytes = response.bytes().await?;
            let expected_len = (range.end - range.start) as usize;

            if bytes.len() != expected_len {
                warn!(
                    expected = expected_len,
                    got = bytes.len(),
                    "webseed returned unexpected content length"
                );
                return Err(WebSeedError::InvalidByteRange);
            }

            Ok(bytes)
        } else if status == StatusCode::OK {
            // Server doesn't support byte ranges, returned full content
            // We need to extract the relevant portion
            let bytes = response.bytes().await?;
            let start = range.start as usize;
            let end = range.end as usize;

            if end > bytes.len() {
                warn!(
                    requested_end = end,
                    actual_len = bytes.len(),
                    "webseed returned insufficient content"
                );
                return Err(WebSeedError::InvalidByteRange);
            }

            Ok(bytes.slice(start..end))
        } else if status == StatusCode::RANGE_NOT_SATISFIABLE {
            debug!(url = %url, "webseed: range not satisfiable");
            Err(WebSeedError::InvalidByteRange)
        } else {
            debug!(url = %url, status = %status, "webseed returned error status");
            Err(WebSeedError::ServerError(status))
        }
    }

    /// Download a complete piece from a WebSeed.
    ///
    /// For single-file torrents, this downloads a byte range directly.
    /// For multi-file torrents, this may need to download from multiple files
    /// and concatenate the results.
    pub async fn download_piece(
        &self,
        webseed_url: &WebSeedUrl,
        piece_index: ValidPieceIndex,
        lengths: &Lengths,
        torrent_name: Option<&str>,
        file_paths: &[String],
        is_multi_file: bool,
    ) -> Result<Bytes, WebSeedError> {
        if webseed_url.disabled {
            return Err(WebSeedError::Disabled);
        }

        let piece_offset = lengths.piece_offset(piece_index);
        let piece_length = lengths.piece_length(piece_index) as u64;

        if !is_multi_file || file_paths.len() == 1 {
            // Single file torrent - simple case
            let url = Self::build_url(
                &webseed_url.url,
                torrent_name,
                file_paths.first().map(|s| s.as_str()),
                is_multi_file,
            )?;

            self.download_range(&url, piece_offset..piece_offset + piece_length)
                .await
        } else {
            // Multi-file torrent - piece may span multiple files
            // This is more complex and requires knowing file boundaries
            self.download_piece_multi_file(
                webseed_url,
                piece_offset,
                piece_length,
                torrent_name,
                file_paths,
            )
            .await
        }
    }

    /// Download a piece that spans multiple files.
    async fn download_piece_multi_file(
        &self,
        webseed_url: &WebSeedUrl,
        piece_offset: u64,
        piece_length: u64,
        torrent_name: Option<&str>,
        _file_paths: &[String],
    ) -> Result<Bytes, WebSeedError> {
        // For now, we'll use a simplified approach that downloads from the first matching file
        // A full implementation would need file offset information to properly span files

        // Build URL with just the torrent name for multi-file
        let url = Self::build_url(&webseed_url.url, torrent_name, None, true)?;

        // Try to download the range - some servers handle multi-file torrents as a single stream
        self.download_range(&url, piece_offset..piece_offset + piece_length)
            .await
    }

    /// Check if a WebSeed URL is valid and responding.
    pub async fn probe(&self, url: &Url) -> Result<(), WebSeedError> {
        let response = self
            .client
            .head(url.clone())
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(WebSeedError::ServerError(response.status()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_url_single_file() {
        let base = Url::parse("http://example.com/files/test.iso").unwrap();
        let url = WebSeedClient::build_url(&base, Some("test.iso"), None, false).unwrap();
        assert_eq!(url.as_str(), "http://example.com/files/test.iso");
    }

    #[test]
    fn test_build_url_single_file_trailing_slash() {
        let base = Url::parse("http://example.com/files/").unwrap();
        let url = WebSeedClient::build_url(&base, Some("test.iso"), None, false).unwrap();
        assert_eq!(url.as_str(), "http://example.com/files/test.iso");
    }

    #[test]
    fn test_build_url_multi_file() {
        let base = Url::parse("http://example.com/torrents/").unwrap();
        let url = WebSeedClient::build_url(
            &base,
            Some("my-torrent"),
            Some("subdir/file.txt"),
            true,
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "http://example.com/torrents/my-torrent/subdir/file.txt"
        );
    }
}
