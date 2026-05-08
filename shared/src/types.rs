use uuid::Uuid;
use std::fmt;

pub type SessionId = Uuid;// a random unique ID for each socks5 connection
pub type Sequence = u32;// keep the order of chunks, seperated for upstream and downstream 

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
        write!(f, "{}", s)
    }
}
