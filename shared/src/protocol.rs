use thiserror::Error; 
//use serde::{Serialize, Deserialize};
use crate::types::{SessionId, Sequence, FileType };

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


