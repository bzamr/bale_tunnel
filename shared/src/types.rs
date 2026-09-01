use uuid::Uuid;
use std::fmt;

pub type SessionId = Uuid;
pub type Sequence = u32;

/// Pairs that travel across the Bale channel: `(seq, raw_chunk_data)`.
pub type ChunkPair = (u32, Vec<u8>);

#[derive(Debug,PartialEq)]
pub enum FileType {
   Conn,   // connection request, client to server : {Host:Port}
   Ack,    // connection accepted, server to client :{Ok|Err}
   Upstream,   // chunk,client to server
   Downstream, // chunk, server to client
   End,    // session end: empty file ( may not be safe ) 
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            FileType::Conn => "conn",
            FileType::Ack => "ack",
            FileType::Upstream => "u",
            FileType::Downstream => "d",
            FileType::End => "end",
        };
        write!(f, "{s}")
    }
}
