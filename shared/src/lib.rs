mod types;
mod protocol;
pub mod compression;
pub mod bot_api;

pub use compression::*;
pub use types::{SessionId, Sequence, FileType, ChunkPair};
pub use protocol::{
    upstream_filename, downstream_filename, conn_filename, ack_filename, end_filename,
    parse_filename, ParseFilenameError, serialize_header, deserialize_header,
    ChunkHeader, HEADER_SIZE, HEADER_MAGIC,
};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_valid_conn_filename() {
        let id = Uuid::new_v4();
        let conn = conn_filename(id);
        assert_eq!(parse_filename(&conn), Ok((FileType::Conn, id, None)));
    }

    #[test]
    fn test_valid_ack_filename() {
        let id = Uuid::new_v4();
        let ack = ack_filename(id);
        assert_eq!(parse_filename(&ack), Ok((FileType::Ack, id, None)));
    }

    #[test]
    fn test_valid_upstream_filename() {
        let id = Uuid::new_v4();
        let up_stream = upstream_filename(id, 47);
        assert_eq!(
            parse_filename(&up_stream),
            Ok((FileType::Upstream, id, Some(47)))
        );
    }

    #[test]
    fn test_valid_downstream_filename() {
        let id = Uuid::new_v4();
        let d = downstream_filename(id, 7);
        assert_eq!(
            parse_filename(&d),
            Ok((FileType::Downstream, id, Some(7)))
        );
    }

    #[test]
    fn test_valid_end_filename() {
        let id = Uuid::new_v4();
        let end = end_filename(id);
        assert_eq!(parse_filename(&end), Ok((FileType::End, id, None)));
    }

    #[test]
    fn test_deserialize_header_validates_magic() {
        let mut buf = [0u8; HEADER_SIZE];
        // Wrong magic
        assert!(deserialize_header(&buf).is_none());
        // Correct magic, zero fields
        buf[0..4].copy_from_slice(&HEADER_MAGIC.to_le_bytes());
        let header = deserialize_header(&buf);
        assert!(header.is_some());
        assert_eq!(header.unwrap().magic, HEADER_MAGIC);
    }

    #[test]
    fn test_header_round_trip() {
        let original = ChunkHeader::new(0xDEAD_BEEF_CAFE_BABE_1234_5678_9ABC_DEF0, 42, true, 128, 256);
        let bytes = serialize_header(&original);
        let restored = deserialize_header(&bytes).expect("valid header");
        assert_eq!(restored.session_id, original.session_id);
        assert_eq!(restored.seq, original.seq);
        assert!(restored.is_compressed());
        assert_eq!(restored.data_len, 128);
        assert_eq!(restored.original_len, 256);
    }
}
