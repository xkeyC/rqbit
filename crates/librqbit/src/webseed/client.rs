//! WebSeed HTTP client implementation.
//!
//! Handles HTTP byte-range requests to WebSeed servers.

use std::num::NonZeroU32;
use std::ops::Range;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use reqwest::{Client, StatusCode};
use tracing::{debug, trace, warn};
use url::Url;

use librqbit_core::lengths::{Lengths, ValidPieceIndex};

use super::{BytesReceivedCallback, RateLimitCallback, WebSeedError, WebSeedFileInfo, WebSeedUrl};

/// HTTP client for WebSeed downloads.
#[derive(Clone)]
pub struct WebSeedClient {
    client: Client,
    timeout: Duration,
    user_agent: Option<String>,
}

impl WebSeedClient {
    /// Create a new WebSeed client with the given reqwest client.
    pub fn new(client: Client, timeout_secs: u64, user_agent: Option<String>) -> Self {
        Self {
            client,
            timeout: Duration::from_secs(timeout_secs),
            user_agent,
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

    /// Download a byte range from a WebSeed URL with streaming and progress callback.
    ///
    /// Uses HTTP Range header to request specific bytes.
    /// Calls the progress callback with bytes received during download for real-time stats.
    pub async fn download_range(
        &self,
        url: &Url,
        range: Range<u64>,
        on_bytes_received: Option<&BytesReceivedCallback>,
        rate_limiter: Option<&RateLimitCallback>,
    ) -> Result<Bytes, WebSeedError> {
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);

        trace!(
            url = %url,
            range = %range_header,
            "downloading byte range from webseed"
        );

        let mut request = self
            .client
            .get(url.clone())
            .header("Range", &range_header)
            .timeout(self.timeout);

        if let Some(ua) = &self.user_agent {
            request = request.header("User-Agent", ua);
        }

        let response = request.send().await?;

        let status = response.status();

        // Check for success - either 200 (full content) or 206 (partial content)
        if status == StatusCode::PARTIAL_CONTENT {
            // Server supports byte ranges - use streaming to report progress
            let expected_len = (range.end - range.start) as usize;
            let bytes = self.stream_response(response, on_bytes_received, rate_limiter).await?;

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
            let bytes = self.stream_response(response, on_bytes_received, rate_limiter).await?;
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

    /// Stream a response body and call the progress callback for each chunk.
    /// Also applies rate limiting if configured.
    async fn stream_response(
        &self,
        response: reqwest::Response,
        on_bytes_received: Option<&BytesReceivedCallback>,
        rate_limiter: Option<&RateLimitCallback>,
    ) -> Result<Bytes, WebSeedError> {
        let mut stream = response.bytes_stream();
        let mut buffer = BytesMut::new();

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            let chunk_len = chunk.len();

            // Apply rate limiting before processing the chunk
            if let Some(limiter) = rate_limiter {
                if let Some(len) = NonZeroU32::new(chunk_len as u32) {
                    limiter(len).await.map_err(|e| {
                        WebSeedError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("rate limit error: {}", e),
                        ))
                    })?;
                }
            }

            // Report bytes received for real-time speed calculation
            if let Some(callback) = on_bytes_received {
                callback(chunk_len as u64);
            }

            buffer.extend_from_slice(&chunk);
        }

        Ok(buffer.freeze())
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
        file_infos: &[WebSeedFileInfo],
        is_multi_file: bool,
        on_bytes_received: Option<&BytesReceivedCallback>,
        rate_limiter: Option<&RateLimitCallback>,
    ) -> Result<Bytes, WebSeedError> {
        if webseed_url.disabled {
            return Err(WebSeedError::Disabled);
        }

        let piece_offset = lengths.piece_offset(piece_index);
        let piece_length = lengths.piece_length(piece_index) as u64;

        if !is_multi_file || file_infos.len() == 1 {
            // Single file torrent - simple case
            let url = Self::build_url(
                &webseed_url.url,
                torrent_name,
                file_infos.first().map(|f| f.path.as_str()),
                is_multi_file,
            )?;

            self.download_range(&url, piece_offset..piece_offset + piece_length, on_bytes_received, rate_limiter)
                .await
        } else {
            // Multi-file torrent - piece may span multiple files
            self.download_piece_multi_file(
                webseed_url,
                piece_offset,
                piece_length,
                torrent_name,
                file_infos,
                on_bytes_received,
                rate_limiter,
            )
            .await
        }
    }

    /// Download a piece that spans multiple files.
    ///
    /// BEP-19 specifies that for multi-file torrents, each file must be
    /// downloaded separately using its path relative to the torrent name.
    async fn download_piece_multi_file(
        &self,
        webseed_url: &WebSeedUrl,
        piece_offset: u64,
        piece_length: u64,
        torrent_name: Option<&str>,
        file_infos: &[WebSeedFileInfo],
        on_bytes_received: Option<&BytesReceivedCallback>,
        rate_limiter: Option<&RateLimitCallback>,
    ) -> Result<Bytes, WebSeedError> {
        let piece_end = piece_offset + piece_length;
        let mut result = BytesMut::with_capacity(piece_length as usize);

        // Find all files that overlap with this piece
        for file_info in file_infos {
            let file_start = file_info.offset;
            let file_end = file_start + file_info.length;

            // Skip files that don't overlap with this piece
            if file_end <= piece_offset || file_start >= piece_end {
                continue;
            }

            // Calculate the byte range within this file
            let range_start_in_torrent = piece_offset.max(file_start);
            let range_end_in_torrent = piece_end.min(file_end);

            // Convert to file-relative offsets
            let file_range_start = range_start_in_torrent - file_start;
            let file_range_end = range_end_in_torrent - file_start;

            // Build URL for this specific file
            let url = Self::build_url(
                &webseed_url.url,
                torrent_name,
                Some(&file_info.path),
                true,
            )?;

            trace!(
                url = %url,
                file_path = %file_info.path,
                file_range = %format!("{}-{}", file_range_start, file_range_end),
                "downloading file range for multi-file piece"
            );

            // Download the range from this file
            let bytes = self
                .download_range(&url, file_range_start..file_range_end, on_bytes_received, rate_limiter)
                .await?;

            result.extend_from_slice(&bytes);
        }

        if result.len() != piece_length as usize {
            warn!(
                expected = piece_length,
                got = result.len(),
                "multi-file piece download size mismatch"
            );
            return Err(WebSeedError::InvalidByteRange);
        }

        Ok(result.freeze())
    }

    /// Check if a WebSeed URL is valid and responding.
    pub async fn probe(&self, url: &Url) -> Result<(), WebSeedError> {
        let mut request = self
            .client
            .head(url.clone())
            .timeout(Duration::from_secs(10));

        if let Some(ua) = &self.user_agent {
            request = request.header("User-Agent", ua);
        }

        let response = request.send().await?;

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
