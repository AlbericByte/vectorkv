use std::io;

#[derive(Debug)]
pub enum DBError {
    Io(String),
    Corruption(String),
    InvalidArgument(String),
    UnknownColumnFamily(String),
    NotFound(String),
    Other(String),
}
