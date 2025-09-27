use crate::core::status::{Status, Result, ErrorContext, ContextResult};
use std::time::{Duration, Instant};
use std::thread;

/// Recovery strategy for different types of operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStrategy {
    /// Do not attempt recovery
    None,
    /// Retry the operation with exponential backoff
    ExponentialBackoff { max_retries: u32, base_delay: Duration },
    /// Retry the operation with fixed intervals
    FixedDelay { max_retries: u32, delay: Duration },
    /// Custom recovery strategy
    Custom,
}

impl Default for RecoveryStrategy {
    fn default() -> Self {
        RecoveryStrategy::ExponentialBackoff {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
        }
    }
}

/// Recovery configuration for specific error types
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Strategy to use for lock contention errors
    pub lock_contention: RecoveryStrategy,
    /// Strategy to use for memory allocation errors
    pub memory_allocation: RecoveryStrategy,
    /// Strategy to use for I/O errors
    pub io_errors: RecoveryStrategy,
    /// Strategy to use for temporary failures
    pub temporary_failures: RecoveryStrategy,
    /// Maximum total recovery time
    pub max_recovery_time: Duration,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            lock_contention: RecoveryStrategy::ExponentialBackoff {
                max_retries: 5,
                base_delay: Duration::from_millis(50),
            },
            memory_allocation: RecoveryStrategy::FixedDelay {
                max_retries: 3,
                delay: Duration::from_millis(200),
            },
            io_errors: RecoveryStrategy::ExponentialBackoff {
                max_retries: 3,
                base_delay: Duration::from_millis(100),
            },
            temporary_failures: RecoveryStrategy::ExponentialBackoff {
                max_retries: 3,
                base_delay: Duration::from_millis(100),
            },
            max_recovery_time: Duration::from_secs(30),
        }
    }
}

/// Recovery context tracking retry attempts and timing
#[derive(Debug)]
pub struct RecoveryContext {
    pub attempt: u32,
    pub start_time: Instant,
    pub last_error: Option<ErrorContext>,
    pub config: RecoveryConfig,
}

impl RecoveryContext {
    /// Create a new recovery context
    pub fn new(config: RecoveryConfig) -> Self {
        Self {
            attempt: 0,
            start_time: Instant::now(),
            last_error: None,
            config,
        }
    }

    /// Record a failed attempt
    pub fn record_failure(&mut self, error: ErrorContext) {
        self.attempt += 1;
        self.last_error = Some(error);
    }

    /// Check if recovery should continue
    pub fn should_continue(&self, status: Status) -> bool {
        // Check if we've exceeded the maximum recovery time
        if self.start_time.elapsed() > self.config.max_recovery_time {
            return false;
        }

        let strategy = self.get_strategy_for_status(status);
        match strategy {
            RecoveryStrategy::None => false,
            RecoveryStrategy::ExponentialBackoff { max_retries, .. } |
            RecoveryStrategy::FixedDelay { max_retries, .. } => {
                self.attempt < max_retries
            }
            RecoveryStrategy::Custom => true, // Let custom logic decide
        }
    }

    /// Get the recovery strategy for a specific status
    fn get_strategy_for_status(&self, status: Status) -> RecoveryStrategy {
        match status {
            Status::LockContentionTimeout => self.config.lock_contention,
            Status::OutOfMemory | Status::AllocationFailed => self.config.memory_allocation,
            Status::IoError | Status::FileNotFound | Status::PermissionDenied => self.config.io_errors,
            Status::Pending | Status::BufferTooSmall => self.config.temporary_failures,
            _ => RecoveryStrategy::None,
        }
    }

    /// Calculate the delay before next retry
    pub fn get_retry_delay(&self, status: Status) -> Option<Duration> {
        let strategy = self.get_strategy_for_status(status);
        match strategy {
            RecoveryStrategy::None => None,
            RecoveryStrategy::FixedDelay { delay, .. } => Some(delay),
            RecoveryStrategy::ExponentialBackoff { base_delay, .. } => {
                let multiplier = 2_u64.pow(self.attempt.min(10)); // Cap to prevent overflow
                Some(base_delay * multiplier as u32)
            }
            RecoveryStrategy::Custom => Some(Duration::from_millis(100)), // Default for custom
        }
    }

    /// Perform the retry delay
    pub fn delay_for_retry(&self, status: Status) {
        if let Some(delay) = self.get_retry_delay(status) {
            thread::sleep(delay);
        }
    }
}

/// Recovery manager for handling automatic error recovery
pub struct RecoveryManager {
    config: RecoveryConfig,
}

impl RecoveryManager {
    /// Create a new recovery manager with default configuration
    pub fn new() -> Self {
        Self {
            config: RecoveryConfig::default(),
        }
    }

    /// Create a new recovery manager with custom configuration
    pub fn with_config(config: RecoveryConfig) -> Self {
        Self { config }
    }

    /// Execute an operation with automatic recovery
    pub fn execute_with_recovery<F, T>(&self, mut operation: F) -> ContextResult<T>
    where
        F: FnMut() -> ContextResult<T>,
    {
        let mut context = RecoveryContext::new(self.config.clone());

        loop {
            match operation() {
                Ok(result) => return Ok(result),
                Err(error) => {
                    let status = error.root_cause();

                    // If this is not a recoverable error, return immediately
                    if !status.is_recoverable() {
                        return Err(error);
                    }

                    context.record_failure(error.clone());

                    // Check if we should attempt recovery
                    if !context.should_continue(status) {
                        return Err(error.with_context(format!(
                            "Recovery failed after {} attempts in {:?}",
                            context.attempt,
                            context.start_time.elapsed()
                        )));
                    }

                    log::debug!(
                        "Attempting recovery for error: {} (attempt {}/{})",
                        error,
                        context.attempt,
                        match context.get_strategy_for_status(status) {
                            RecoveryStrategy::ExponentialBackoff { max_retries, .. } |
                            RecoveryStrategy::FixedDelay { max_retries, .. } => max_retries,
                            _ => 0,
                        }
                    );

                    // Perform recovery delay
                    context.delay_for_retry(status);
                }
            }
        }
    }

    /// Execute an operation with recovery for simple Status results
    pub fn execute_with_recovery_simple<F>(&self, mut operation: F) -> Result<()>
    where
        F: FnMut() -> Result<()>,
    {
        let result = self.execute_with_recovery(|| {
            operation().map_err(|status| ErrorContext::new(status))
        });

        result.map_err(|error| error.root_cause())
    }
}

impl Default for RecoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for adding recovery capabilities to results
pub trait RecoveryExt<T> {
    /// Execute with recovery using default configuration
    fn with_recovery<F>(self, operation: F) -> ContextResult<T>
    where
        F: FnMut() -> ContextResult<T>;

    /// Execute with recovery using custom configuration
    fn with_recovery_config<F>(self, config: RecoveryConfig, operation: F) -> ContextResult<T>
    where
        F: FnMut() -> ContextResult<T>;
}

impl<T, F> RecoveryExt<T> for F
where
    F: FnMut() -> ContextResult<T>,
{
    fn with_recovery<G>(mut self, mut _operation: G) -> ContextResult<T>
    where
        G: FnMut() -> ContextResult<T>,
    {
        let manager = RecoveryManager::new();
        manager.execute_with_recovery(|| self())
    }

    fn with_recovery_config<G>(mut self, config: RecoveryConfig, mut _operation: G) -> ContextResult<T>
    where
        G: FnMut() -> ContextResult<T>,
    {
        let manager = RecoveryManager::with_config(config);
        manager.execute_with_recovery(|| self())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_recovery_success_after_retries() {
        let manager = RecoveryManager::new();
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let result = manager.execute_with_recovery(|| {
            let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                Err(ErrorContext::new(Status::LockContentionTimeout))
            } else {
                Ok(42)
            }
        });

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_recovery_failure_after_max_retries() {
        let manager = RecoveryManager::new();
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let result: ContextResult<()> = manager.execute_with_recovery(|| {
            attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            Err(ErrorContext::new(Status::LockContentionTimeout))
        });

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.contains_status(Status::LockContentionTimeout));
        assert!(attempt_count.load(Ordering::SeqCst) > 1);
    }

    #[test]
    fn test_non_recoverable_error_no_retry() {
        let manager = RecoveryManager::new();
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let result: ContextResult<()> = manager.execute_with_recovery(|| {
            attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            Err(ErrorContext::new(Status::Corruption))
        });

        assert!(result.is_err());
        assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
    }
}