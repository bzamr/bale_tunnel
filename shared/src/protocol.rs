use thiserror::Error; 
//use serde::{Serialize, Deserialize};
use crate::types::{SessionId, Sequence, FileType };
use std::convert::TryInto;

// u_<session_id>_<seq>.bin
pub fn upstream_filename(session_id: SessionId, seq: Sequence) -> String {
    format!("u_{}_{}.bin", session_id, seq)
}

// d_<session_id>_<seq>.bin
pub fn downstream_filename(session_id: SessionId, seq: Sequence) -> String {
    format!("d_{}_{}.bin", session_id, seq)
}

// conn_<session_id>.bin
pub fn conn_filename(session_id: SessionId) -> String {
    format!("conn_{}.bin", session_id)
}

// ack_<session_id>.bin
pub fn ack_filename(session_id: SessionId) -> String {
    format!("ack_{}.bin", session_id)
}

// end_<session_id>.bin
pub fn end_filename(session_id: SessionId) -> String {
    format!("end_{}.bin", session_id)
}
    
#[derive(Error, Debug, PartialEq, Eq)]
pub enum ParseFilenameError {
    #[error("missing .bin suffix")]
    MissingBinSuffix,
    #[error("unknown file prefix")]
    UnknownPrefix,
    #[error("invalid UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
    #[error("missing sequence number")]
    MissingSequence,
    #[error("invalid sequence number: {0}")]
    InvalidSequence(#[from] std::num::ParseIntError),
    #[error("empty segment in filename")]
    EmptySegment,
}

pub fn parse_filename(name: &str) -> Result<(FileType, SessionId, Option<Sequence>), ParseFilenameError> {
    // delete .bin
    let stem = name.strip_suffix(".bin").ok_or(ParseFilenameError::MissingBinSuffix)?;
    // shouldn't happen 
    if stem.is_empty() {
        return Err(ParseFilenameError::EmptySegment);
    }
    // recognize and process the prefix
    if let Some(rest) = stem.strip_prefix("conn_") {
        if rest.is_empty() { return Err(ParseFilenameError::EmptySegment); }
        let id = SessionId::parse_str(rest)?;
        Ok((FileType::Conn, id, None))
    } 
    else if let Some(rest) = stem.strip_prefix("ack_") {
        if rest.is_empty() { return Err(ParseFilenameError::EmptySegment); }
        let id = SessionId::parse_str(rest)?;
        Ok((FileType::Ack, id, None))
    }
    else if let Some(rest) = stem.strip_prefix("end_") {
        if rest.is_empty() { return Err(ParseFilenameError::EmptySegment); }
        let id = SessionId::parse_str(rest)?;
        Ok((FileType::End, id, None))
    }
    else if let Some(rest) = stem.strip_prefix("u_") {
        parse_data_filename(rest, FileType::Upstream)
    }
    else if let Some(rest) = stem.strip_prefix("d_") {
        parse_data_filename(rest, FileType::Downstream)
    }
    else {
        Err(ParseFilenameError::UnknownPrefix)
    }
}

fn parse_data_filename(rest: &str, file_type: FileType) -> Result<(FileType, SessionId, Option<Sequence>), ParseFilenameError> {
    // rest = SessionID_sequence
    let last_underscore = rest.rfind('_').ok_or(ParseFilenameError::MissingSequence)?;
    let (uuid_part, seq_part) = rest.split_at(last_underscore);
    let seq_part = &seq_part[1..]; // remove '_'
    if uuid_part.is_empty() || seq_part.is_empty() {
        return Err(ParseFilenameError::EmptySegment);
    }
    let id = SessionId::parse_str(uuid_part)?;
    let seq: u32 = seq_part.parse()?;
    Ok((file_type, id, Some(seq)))
}

// Magic number for chunk header: "BLET" in little-endian ASCII
// to find out if chunk has head or not in the other side(server-client).
pub const HEADER_MAGIC: u32 = 0x424C4554;
// Total size of the header in bytes (36 bytes)
pub const HEADER_SIZE: usize = 36;

// Represents the header prepended to each compressed or uncompressed chunk.
// Layout is C‑compatible for potential FFI, but used here for safe serialization.
#[repr(C)]// correct order of fields
#[derive(Debug, Clone, Copy)]
pub struct ChunkHeader {
    // Fixed magic value to identify the header (0x424C4554)
    pub magic: u32,
    // Session ID encoded as a 128‑bit little‑endian integer (UUID)
    pub session_id: u128,
    // Sequence number of this chunk (separate for upstream/downstream)
    pub seq: u32,
    // Bit flags: bit LSB = 1 if payload is compressed with LZ4
    pub flags: u8,
    // Reserved for future use (must be zero)
    pub reserved: [u8; 3],
    // Length of the payload data that follows the header (in bytes)
    pub data_len: u32,
    // Original uncompressed length (equal to data_len if not compressed)
    pub original_len: u32,
}

impl ChunkHeader {
    pub fn new(session_id: u128, seq: u32, compressed: bool, data_len: u32, original_len: u32) -> Self {
        Self {
            magic: HEADER_MAGIC,
            session_id,
            seq,
            flags: if compressed { 1 } else { 0 },
            reserved: [0; 3],
            data_len,
            original_len,
        }
    }

    /// Returns true if the payload is compressed (LZ4).
    pub fn is_compressed(&self) -> bool {
        self.flags & 1 != 0
    }
}

/// Serializes a `ChunkHeader` into a fixed‑size byte array (little‑endian).
pub fn serialize_header(header: &ChunkHeader) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..4].copy_from_slice(&header.magic.to_le_bytes());
    buf[4..20].copy_from_slice(&header.session_id.to_le_bytes());
    buf[20..24].copy_from_slice(&header.seq.to_le_bytes());
    buf[24] = header.flags;
    buf[25..28].copy_from_slice(&header.reserved);
    buf[28..32].copy_from_slice(&header.data_len.to_le_bytes());
    buf[32..36].copy_from_slice(&header.original_len.to_le_bytes());
    buf
}

/// Deserializes a byte slice into a `ChunkHeader`.
/// Returns `None` if the slice is too short or conversion fails.
pub fn deserialize_header(buf: &[u8]) -> Option<ChunkHeader> {
    if buf.len() < HEADER_SIZE {
        return None;
    }
    // if any of the fields coudn't be converted, function return None.
    let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
    let session_id = u128::from_le_bytes(buf[4..20].try_into().ok()?);
    let seq = u32::from_le_bytes(buf[20..24].try_into().ok()?);
    let flags = buf[24];
    let reserved = [buf[25], buf[26], buf[27]];
    let data_len = u32::from_le_bytes(buf[28..32].try_into().ok()?);
    let original_len = u32::from_le_bytes(buf[32..36].try_into().ok()?);
    Some(ChunkHeader { magic, session_id, seq, flags, reserved, data_len, original_len })
}