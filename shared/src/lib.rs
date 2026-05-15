
mod types;
mod protocol;
pub mod compression;

pub use compression::*;
pub use types::{SessionId, Sequence, FileType};
pub use protocol::{
    upstream_filename, downstream_filename, conn_filename, ack_filename, end_filename,
    parse_filename, ParseFilenameError,
};

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    
    #[test]
    fn test_valid_conn_filename(){
        let id = Uuid::new_v4();
        let conn = conn_filename(id);
        assert_eq!(parse_filename(&conn), Ok((FileType::Conn, id, None)));
    }
    #[test]
    fn test_valid_ack_filename(){
        let id = Uuid::new_v4();
        let ack = ack_filename(id);
        assert_eq!(parse_filename(&ack), Ok((FileType::Ack, id, None)));
    }
    #[test]
    fn test_valid_upstream_filename(){
        let id = Uuid::new_v4();
        let up_sream = upstream_filename(id,47);
        assert_eq!(parse_filename(&up_sream), Ok((FileType::Upstream, id, Some(47))));
    }
    #[test]
    fn test_valid_downstream_filename(){
        let id = Uuid::new_v4();
        let d = downstream_filename(id, 7);
        assert_eq!(parse_filename(&d), Ok((FileType::Downstream, id, Some(7))));
    }
    #[test]
    fn test_valid_end_filename(){
        let id = Uuid::new_v4();
        let end = end_filename(id);
        assert_eq!(parse_filename(&end), Ok((FileType::End, id, None)));
    }
}

