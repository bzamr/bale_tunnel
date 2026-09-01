use lz4_flex::{compress_prepend_size, decompress_size_prepended};
// compress_prepend_size add 4 byte header: original data size
// decompress_size_prepend read the header and decompress

// Attempts to compress data using LZ4, but only if it is beneficial.
// Returns a tuple: (processed_data, was_compressed).
// - processed_data: either the original data (if compression skipped or not beneficial)
//   or the compressed data (with prepended original size).
// - was_compressed: true if the returned data is compressed, false otherwise.
// Compression is skipped for chunks smaller than 1 KiB.
// Compression is only applied if the resulting size is at least 5% smaller than the original.
/// Attempts to compress data using LZ4, but only if it is beneficial.
/// Returns a tuple: `(processed_data, was_compressed)`.
/// Compression is skipped for chunks smaller than 1 KiB.
/// Only applied if the result is at least 5% smaller than the original.
#[must_use]
pub fn try_compress(data: &[u8]) -> (Vec<u8>, bool) {
    if data.len() < 1024 {
        return (data.to_vec(), false);
    }

    let compressed: Vec<u8> = compress_prepend_size(data);

    // Integer arithmetic avoids f64 precision loss on large buffers.
    let original_len = data.len();
    let compressed_len = compressed.len();
    if compressed_len < original_len {
        let saved = original_len - compressed_len;
        // saved × 100 ≥ original × 5
        if saved * 100 >= original_len * 5 {
            return (compressed, true);
        }
    }

    (data.to_vec(), false)
}

/// Decompresses data if it was previously compressed.
///
/// # Errors
/// Returns an error if decompression fails (e.g., corrupted input).
pub fn decompress(data: &[u8], was_compressed: bool) -> Result<Vec<u8>, anyhow::Error> {
    if was_compressed {
        // The compressed format includes a 4-byte header with original length,
        // which `decompress_size_prepend` reads and uses to allocate the output.
        Ok(decompress_size_prepended(data)?)
    } else {
        // No compression: return a copy of the original data.
        Ok(data.to_vec())
    }
}