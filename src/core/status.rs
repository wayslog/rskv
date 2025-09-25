/// Represents the status of an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    Pending = 1,
    NotFound = 2,
    OutOfMemory = 3,
    IoError = 4,
    Corruption = 5,
    Aborted = 6,

    // Memory allocation errors
    AllocationFailed = 7,
    InvalidAlignment = 8,
    BufferTooSmall = 9,

    // Concurrency errors
    LockContentionTimeout = 10,
    EpochProtectionFailed = 11,
    DeadlockDetected = 12,

    // Data integrity errors
    ChecksumMismatch = 13,
    InvalidDataFormat = 14,
    VersionMismatch = 15,

    // File system errors
    FileNotFound = 16,
    PermissionDenied = 17,
    DiskFull = 18,

    // Configuration errors
    InvalidConfiguration = 19,
    FeatureNotSupported = 20,

    // Internal errors
    InternalError = 21,
    UnexpectedState = 22,
}

impl Status {
    /// Returns a string representation of the status.
    pub fn as_str(&self) -> &str {
        match self {
            Status::Ok => "Ok",
            Status::Pending => "Pending",
            Status::NotFound => "NotFound",
            Status::OutOfMemory => "OutOfMemory",
            Status::IoError => "IoError",
            Status::Corruption => "Corruption",
            Status::Aborted => "Aborted",

            // Memory allocation errors
            Status::AllocationFailed => "AllocationFailed",
            Status::InvalidAlignment => "InvalidAlignment",
            Status::BufferTooSmall => "BufferTooSmall",

            // Concurrency errors
            Status::LockContentionTimeout => "LockContentionTimeout",
            Status::EpochProtectionFailed => "EpochProtectionFailed",
            Status::DeadlockDetected => "DeadlockDetected",

            // Data integrity errors
            Status::ChecksumMismatch => "ChecksumMismatch",
            Status::InvalidDataFormat => "InvalidDataFormat",
            Status::VersionMismatch => "VersionMismatch",

            // File system errors
            Status::FileNotFound => "FileNotFound",
            Status::PermissionDenied => "PermissionDenied",
            Status::DiskFull => "DiskFull",

            // Configuration errors
            Status::InvalidConfiguration => "InvalidConfiguration",
            Status::FeatureNotSupported => "FeatureNotSupported",

            // Internal errors
            Status::InternalError => "InternalError",
            Status::UnexpectedState => "UnexpectedState",
        }
    }

    /// Returns a detailed description of the error
    pub fn description(&self) -> &str {
        match self {
            Status::Ok => "Operation completed successfully",
            Status::Pending => "Operation is pending completion",
            Status::NotFound => "Requested item was not found",
            Status::OutOfMemory => "Insufficient memory to complete operation",
            Status::IoError => "Input/output operation failed",
            Status::Corruption => "Data corruption detected",
            Status::Aborted => "Operation was aborted",

            // Memory allocation errors
            Status::AllocationFailed => "Memory allocation failed",
            Status::InvalidAlignment => "Invalid memory alignment specified",
            Status::BufferTooSmall => "Buffer size insufficient for operation",

            // Concurrency errors
            Status::LockContentionTimeout => "Lock acquisition timed out due to contention",
            Status::EpochProtectionFailed => "Epoch protection mechanism failed",
            Status::DeadlockDetected => "Potential deadlock detected",

            // Data integrity errors
            Status::ChecksumMismatch => "Data checksum does not match expected value",
            Status::InvalidDataFormat => "Data format is invalid or corrupted",
            Status::VersionMismatch => "Version compatibility check failed",

            // File system errors
            Status::FileNotFound => "Required file does not exist",
            Status::PermissionDenied => "Insufficient permissions for file operation",
            Status::DiskFull => "Insufficient disk space available",

            // Configuration errors
            Status::InvalidConfiguration => "Configuration parameters are invalid",
            Status::FeatureNotSupported => "Requested feature is not supported",

            // Internal errors
            Status::InternalError => "Internal system error occurred",
            Status::UnexpectedState => "System is in an unexpected state",
        }
    }

    /// Returns true if this is an error status
    pub fn is_error(&self) -> bool {
        !matches!(self, Status::Ok | Status::Pending)
    }

    /// Returns true if this is a recoverable error
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Status::Pending
                | Status::NotFound
                | Status::LockContentionTimeout
                | Status::BufferTooSmall
        )
    }

    /// Returns true if this is a memory-related error
    pub fn is_memory_error(&self) -> bool {
        matches!(
            self,
            Status::OutOfMemory
                | Status::AllocationFailed
                | Status::InvalidAlignment
                | Status::BufferTooSmall
        )
    }

    /// Returns true if this is a concurrency-related error
    pub fn is_concurrency_error(&self) -> bool {
        matches!(
            self,
            Status::LockContentionTimeout
                | Status::EpochProtectionFailed
                | Status::DeadlockDetected
        )
    }
}

impl std::error::Error for Status {
    fn description(&self) -> &str {
        self.description()
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.as_str(), self.description())
    }
}

/// Result type for operations that can fail with a Status
pub type Result<T> = std::result::Result<T, Status>;
