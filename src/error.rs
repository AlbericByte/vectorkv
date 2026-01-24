use std::io;
use config::ConfigError;

#[derive(Debug)]
pub enum DBError {
    Io(io::Error),
    Config(ConfigError),
    InvalidKeyOrder(String),
    EmptyTable(String),
    Corruption(String),
    InvalidArgument(String),
    UnknownColumnFamily(String),
    NotFound(String),
    InvalidColumnFamily(String),
    Other(String),
}

impl From<std::io::Error> for DBError {
    fn from(e: std::io::Error) -> Self {
        DBError::Io(e)
    }
}

impl From<config::ConfigError> for DBError {
    fn from(e: config::ConfigError) -> Self {
        DBError::Config(e)
    }
}
