use crossbeam_epoch::{self as epoch, Guard as CrossbeamGuard};

/// A light-weight epoch management system, wrapping `crossbeam-epoch`.
/// Re-exporting Guard for convenience.
pub type Guard = CrossbeamGuard;
///
/// This struct provides the core functionality for epoch-based memory reclamation,
/// allowing threads to "protect" themselves while accessing shared data and to
/// defer cleanup operations until no thread is observing a particular epoch.
pub struct LightEpoch {
    // crossbeam-epoch manages global state, so this struct is a lightweight handle.
}

impl LightEpoch {
    /// Creates a new `LightEpoch` instance.
    pub fn new() -> Self {
        LightEpoch {}
    }

    /// Protects the current thread, returning a `Guard`.
    ///
    /// The returned `Guard` ensures that any memory reclaimed while it is active
    /// will not be freed until the guard is dropped. This is equivalent to
    /// entering a critical section for epoch-based reclamation.
    ///
    /// Corresponds to `Protect()` in the C++ version.
    #[inline]
    pub fn protect(&self) -> Guard {
        epoch::pin()
    }

    /// Bumps the current epoch and runs pending deferred functions if possible.
    ///
    /// In `crossbeam-epoch`, epoch advancement and garbage collection are handled
    /// automatically. This function can be used to manually trigger a collection attempt,
    /// which can be useful in scenarios where threads might not be unpinned frequently.
    ///
    /// Corresponds to `BumpCurrentEpoch()` and the draining part of `ProtectAndDrain()`.
    pub fn bump_and_drain(&self) {
        let guard = self.protect();
        guard.flush(); // Attempts to advance the global epoch and collect garbage.
    }
}

impl Default for LightEpoch {
    fn default() -> Self {
        Self::new()
    }
}
