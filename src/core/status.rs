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

/// Error with context and chaining support
#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub status: Status,
    pub context: String,
    pub source: Option<Box<ErrorContext>>,
    pub location: Option<String>,
}

impl ErrorContext {
    /// Create a new error context
    pub fn new(status: Status) -> Self {
        Self {
            status,
            context: String::new(),
            source: None,
            location: None,
        }
    }

    /// Add context to the error
    pub fn with_context<S: Into<String>>(mut self, context: S) -> Self {
        self.context = context.into();
        self
    }

    /// Add source error
    pub fn with_source(mut self, source: ErrorContext) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// Add location information
    pub fn with_location<S: Into<String>>(mut self, location: S) -> Self {
        self.location = location.into().into();
        self
    }

    /// Get the root cause status
    pub fn root_cause(&self) -> Status {
        match &self.source {
            Some(source) => source.root_cause(),
            None => self.status,
        }
    }

    /// Get error chain as a string
    pub fn error_chain(&self) -> String {
        let mut chain = Vec::new();
        let mut current = Some(self);

        while let Some(error) = current {
            let mut msg = format!("{}: {}", error.status.as_str(), error.status.description());
            if !error.context.is_empty() {
                msg.push_str(&format!(" ({})", error.context));
            }
            if let Some(ref location) = error.location {
                msg.push_str(&format!(" at {}", location));
            }
            chain.push(msg);
            current = error.source.as_ref().map(|s| s.as_ref());
        }

        chain.join(" -> ")
    }

    /// Check if error chain contains a specific status
    pub fn contains_status(&self, status: Status) -> bool {
        if self.status == status {
            return true;
        }
        match &self.source {
            Some(source) => source.contains_status(status),
            None => false,
        }
    }
}

impl std::fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error_chain())
    }
}

impl std::error::Error for ErrorContext {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|s| s.as_ref() as &dyn std::error::Error)
    }
}

impl From<Status> for ErrorContext {
    fn from(status: Status) -> Self {
        Self::new(status)
    }
}

/// Enhanced result type with error context
pub type ContextResult<T> = std::result::Result<T, ErrorContext>;

/// Macro for creating error context with location
#[macro_export]
macro_rules! error_context {
    ($status:expr) => {
        $crate::core::status::ErrorContext::new($status)
            .with_location(format!("{}:{}", file!(), line!()))
    };
    ($status:expr, $context:expr) => {
        $crate::core::status::ErrorContext::new($status)
            .with_context($context)
            .with_location(format!("{}:{}", file!(), line!()))
    };
}

/// Macro for adding context to existing result
#[macro_export]
macro_rules! with_context {
    ($result:expr, $context:expr) => {
        $result.map_err(|e| match e {
            err if err.is_error() => $crate::core::status::ErrorContext::new(err)
                .with_context($context)
                .with_location(format!("{}:{}", file!(), line!())),
            _ => $crate::core::status::ErrorContext::new(err)
                .with_location(format!("{}:{}", file!(), line!())),
        })
    };
}

/// Trait for converting results to context results
pub trait ResultExt<T> {
    fn with_context<S: Into<String>>(self, context: S) -> ContextResult<T>;
    fn with_location(self) -> ContextResult<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn with_context<S: Into<String>>(self, context: S) -> ContextResult<T> {
        self.map_err(|status| {
            ErrorContext::new(status)
                .with_context(context)
        })
    }

    fn with_location(self) -> ContextResult<T> {
        self.map_err(|status| {
            ErrorContext::new(status)
        })
    }
}

impl<T> ResultExt<T> for ContextResult<T> {
    fn with_context<S: Into<String>>(self, context: S) -> ContextResult<T> {
        self.map_err(|error| {
            ErrorContext::new(error.status)
                .with_context(context)
                .with_source(error)
        })
    }

    fn with_location(self) -> ContextResult<T> {
        self
    }
}
