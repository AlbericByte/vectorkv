use std::io;

#[derive(Debug)]
pub enum DBError {
    Io(io::Error),
    Corruption(String),
    InvalidArgument(String),
    UnknownColumnFamily(String),
    NotFound(String),
}