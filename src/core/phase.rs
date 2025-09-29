/// Phases, used internally by RsKv to keep track of how far along RsKv has gotten during
/// checkpoint, gc, and grow actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Phase {
    /// Checkpoint phases.
    PrepIndexChkpt = 0,
    IndexChkpt = 1,
    Prepare = 2,
    InProgress = 3,
    WaitPending = 4,
    WaitFlush = 5,
    Rest = 6,
    PersistenceCallback = 7,
    /// Garbage-collection phases.
    GcIoPending = 8,
    GcInProgress = 9,
    /// Grow-index phases.
    GrowPrepare = 10,
    GrowInProgress = 11,
    Invalid = 12,
}

impl Phase {
    /// Returns a string representation of the phase.
    pub fn as_str(&self) -> &str {
        match self {
            Phase::PrepIndexChkpt => "PREP_INDEX_CHKPT",
            Phase::IndexChkpt => "INDEX_CHKPT",
            Phase::Prepare => "PREPARE",
            Phase::InProgress => "IN_PROGRESS",
            Phase::WaitPending => "WAIT_PENDING",
            Phase::WaitFlush => "WAIT_FLUSH",
            Phase::Rest => "REST",
            Phase::PersistenceCallback => "PERSISTENCE_CALLBACK",
            Phase::GcIoPending => "GC_IO_PENDING",
            Phase::GcInProgress => "GC_IN_PROGRESS",
            Phase::GrowPrepare => "GROW_PREPARE",
            Phase::GrowInProgress => "GROW_IN_PROGRESS",
            Phase::Invalid => "INVALID",
        }
    }
}
