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
        }
    }
}

impl std::error::Error for Status {
    fn description(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
