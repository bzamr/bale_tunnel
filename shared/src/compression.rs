use lz4_flex::{compress_prepend_size, decompress_size_prepend};
// compress_prepend_size add 4 byte header: original data size
// decompress_size_prepend read the header and decompress

// Attempts to compress data using LZ4, but only if it is beneficial.
// Returns a tuple: (processed_data, was_compressed).
// - processed_data: either the original data (if compression skipped or not beneficial)
//   or the compressed data (with prepended original size).
// - was_compressed: true if the returned data is compressed, false otherwise.
// Compression is skipped for chunks smaller than 1 KiB.
// Compression is only applied if the resulting size is at least 5% smaller than the original.
pub fn try_compress(data: &[u8]) -> (Vec<u8>, bool) {
    // Skip compression for very small chunks: overhead of LZ4 header (4 bytes)
    // and compression effort is not worth the potential saving.
    if data.len() < 1024 {
        return (data.to_vec(), false);
    }

    // Compress the data; compress_prepend_size adds a 4-byte header with original length.
    let compressed: Vec<u8> = compress_prepend_size(data);

    let savings = (data.len() as f64 - compressed.len() as f64) / data.len() as f64;

    // Apply compression only if the saving is at least 5% and the compressed size
    if savings >= 0.05 && compressed.len() < data.len() {
        (compressed, true)
    } else {
        // Not beneficial: return a copy of the original data.
        (data.to_vec(), false)
    }
}

// Decompresses data if it was previously compressed.
// - data: the byte slice (may be compressed with `compress_prepend_size` or raw).
// - was_compressed: indicates whether the data is expected to be in compressed format.
// Returns the decompressed (or original) data as a Vec<u8> on success,
// or an error if decompression fails (e.g., corrupted input).
pub fn decompress(data: &[u8], was_compressed: bool) -> Result<Vec<u8>, anyhow::Error> {
    if was_compressed {
        // The compressed format includes a 4-byte header with original length,
        // which `decompress_size_prepend` reads and uses to allocate the output.
        Ok(decompress_size_prepend(data)?)
    } else {
        // No compression: return a copy of the original data.
        Ok(data.to_vec())
    }
}