use thiserror::Error;

#[derive(Error, Debug)]
pub enum LeaseError {
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Lease not found")]
    NotFound,
    #[error("Lease already acquired")]
    Conflict,
    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, LeaseError>;
