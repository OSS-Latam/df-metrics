use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum StorageBackend {
    Stdout,
    LocalDisk,
    S3,
}

impl Display for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            StorageBackend::Stdout => "stdout".to_string(),
            StorageBackend::LocalDisk => "local_disk".to_string(),
            StorageBackend::S3 => "s3".to_string(),
        };
        write!(f, "{}", str)
    }
}
