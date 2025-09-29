use super::status::*;
use std::error::Error;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_display() {
        // Test all status variants have non-empty display messages
        assert!(!Status::Ok.to_string().is_empty());
        assert!(!Status::NotFound.to_string().is_empty());
        assert!(!Status::Pending.to_string().is_empty());
        assert!(!Status::Corruption.to_string().is_empty());
        assert!(!Status::BufferTooSmall.to_string().is_empty());
        assert!(!Status::OutOfMemory.to_string().is_empty());
        assert!(!Status::AllocationFailed.to_string().is_empty());
        assert!(!Status::IoError.to_string().is_empty());
        assert!(!Status::LockContentionTimeout.to_string().is_empty());
        assert!(!Status::EpochProtectionFailed.to_string().is_empty());
        assert!(!Status::DeadlockDetected.to_string().is_empty());
        assert!(!Status::ChecksumMismatch.to_string().is_empty());
        assert!(!Status::InvalidDataFormat.to_string().is_empty());
        assert!(!Status::VersionMismatch.to_string().is_empty());
        assert!(!Status::FileNotFound.to_string().is_empty());
        assert!(!Status::PermissionDenied.to_string().is_empty());
        assert!(!Status::DiskFull.to_string().is_empty());
        assert!(!Status::InvalidConfiguration.to_string().is_empty());
        assert!(!Status::FeatureNotSupported.to_string().is_empty());
        assert!(!Status::InternalError.to_string().is_empty());
        assert!(!Status::UnexpectedState.to_string().is_empty());
    }

    #[test]
    fn test_status_is_error() {
        // Ok and Pending are not errors
        assert!(!Status::Ok.is_error());
        assert!(!Status::Pending.is_error());

        // All other statuses are errors
        assert!(Status::NotFound.is_error());
        assert!(Status::Corruption.is_error());
        assert!(Status::BufferTooSmall.is_error());
        assert!(Status::OutOfMemory.is_error());
        assert!(Status::AllocationFailed.is_error());
        assert!(Status::IoError.is_error());
        assert!(Status::LockContentionTimeout.is_error());
        assert!(Status::EpochProtectionFailed.is_error());
        assert!(Status::DeadlockDetected.is_error());
        assert!(Status::ChecksumMismatch.is_error());
        assert!(Status::InvalidDataFormat.is_error());
        assert!(Status::VersionMismatch.is_error());
        assert!(Status::FileNotFound.is_error());
        assert!(Status::PermissionDenied.is_error());
        assert!(Status::DiskFull.is_error());
        assert!(Status::InvalidConfiguration.is_error());
        assert!(Status::FeatureNotSupported.is_error());
        assert!(Status::InternalError.is_error());
        assert!(Status::UnexpectedState.is_error());
    }

    #[test]
    fn test_status_is_recoverable() {
        // Recoverable errors
        assert!(Status::LockContentionTimeout.is_recoverable());
        assert!(Status::OutOfMemory.is_recoverable());
        assert!(Status::AllocationFailed.is_recoverable());
        assert!(Status::IoError.is_recoverable());
        assert!(Status::Pending.is_recoverable());
        assert!(Status::BufferTooSmall.is_recoverable());

        // Non-recoverable errors
        assert!(!Status::Corruption.is_recoverable());
        assert!(!Status::ChecksumMismatch.is_recoverable());
        assert!(!Status::InvalidDataFormat.is_recoverable());
        assert!(!Status::VersionMismatch.is_recoverable());
        assert!(!Status::FileNotFound.is_recoverable());
        assert!(!Status::PermissionDenied.is_recoverable());
        assert!(!Status::DiskFull.is_recoverable());
        assert!(!Status::InvalidConfiguration.is_recoverable());
        assert!(!Status::FeatureNotSupported.is_recoverable());
        assert!(!Status::InternalError.is_recoverable());
        assert!(!Status::UnexpectedState.is_recoverable());
        assert!(!Status::DeadlockDetected.is_recoverable());
        assert!(!Status::EpochProtectionFailed.is_recoverable());
        assert!(!Status::NotFound.is_recoverable());
        assert!(!Status::Ok.is_recoverable()); // Ok is not an error to recover from
    }

    #[test]
    fn test_error_context_creation() {
        let context = ErrorContext::new(Status::OutOfMemory);
        assert_eq!(context.status, Status::OutOfMemory);
        assert!(context.context.is_empty());
        assert!(context.source.is_none());
        assert!(context.location.is_none());
    }

    #[test]
    fn test_error_context_with_context() {
        let context = ErrorContext::new(Status::IoError)
            .with_context("Failed to read file");

        assert_eq!(context.status, Status::IoError);
        assert_eq!(context.context, "Failed to read file");
        assert!(context.source.is_none());
        assert!(context.location.is_none());
    }

    #[test]
    fn test_error_context_with_location() {
        let context = ErrorContext::new(Status::InternalError)
            .with_location("file.rs:123");

        assert_eq!(context.status, Status::InternalError);
        assert!(context.context.is_empty());
        assert!(context.source.is_none());
        assert_eq!(context.location, Some("file.rs:123".to_string()));
    }

    #[test]
    fn test_error_context_chaining() {
        let source = ErrorContext::new(Status::IoError)
            .with_context("Disk read failed");

        let context = ErrorContext::new(Status::InternalError)
            .with_context("Operation failed")
            .with_source(source);

        assert_eq!(context.status, Status::InternalError);
        assert_eq!(context.context, "Operation failed");
        assert!(context.source.is_some());

        let source_ref = context.source.as_ref().unwrap();
        assert_eq!(source_ref.status, Status::IoError);
        assert_eq!(source_ref.context, "Disk read failed");
    }

    #[test]
    fn test_error_context_contains_status() {
        let source = ErrorContext::new(Status::OutOfMemory);
        let context = ErrorContext::new(Status::InternalError)
            .with_source(source);

        assert!(context.contains_status(Status::InternalError));
        assert!(context.contains_status(Status::OutOfMemory));
        assert!(!context.contains_status(Status::IoError));
    }

    #[test]
    fn test_error_context_root_cause() {
        let root = ErrorContext::new(Status::DiskFull);
        let middle = ErrorContext::new(Status::IoError).with_source(root);
        let top = ErrorContext::new(Status::InternalError).with_source(middle);

        assert_eq!(top.root_cause(), Status::DiskFull);
    }

    #[test]
    fn test_error_context_display() {
        let context = ErrorContext::new(Status::OutOfMemory)
            .with_context("Memory allocation failed")
            .with_location("allocator.rs:45");

        let display_str = format!("{}", context);
        assert!(display_str.contains("OutOfMemory"));
        assert!(display_str.contains("Memory allocation failed"));
        assert!(display_str.contains("allocator.rs:45"));
    }

    #[test]
    fn test_error_context_debug() {
        let context = ErrorContext::new(Status::Corruption)
            .with_context("Data verification failed");

        let debug_str = format!("{:?}", context);
        assert!(debug_str.contains("Corruption"));
        assert!(debug_str.contains("Data verification failed"));
    }

    #[test]
    fn test_error_context_error_trait() {
        let source = ErrorContext::new(Status::FileNotFound)
            .with_context("Config file missing");
        let context = ErrorContext::new(Status::InvalidConfiguration)
            .with_context("Failed to load configuration")
            .with_source(source);

        // Test Error trait implementation
        let error_description = context.to_string();
        assert!(!error_description.is_empty());

        // Test source chain
        let source_error = context.source();
        assert!(source_error.is_some());

        if let Some(source) = source_error {
            let source_context = source.downcast_ref::<ErrorContext>();
            assert!(source_context.is_some());
        }
    }

    #[test]
    fn test_result_ext_with_context() {
        let result: Result<()> = Err(Status::IoError);
        let context_result = result.with_context("Test context");

        assert!(context_result.is_err());
        let error = context_result.unwrap_err();
        assert_eq!(error.status, Status::IoError);
        assert_eq!(error.context, "Test context");
    }

    #[test]
    fn test_result_ext_with_location() {
        let result: Result<()> = Err(Status::OutOfMemory);
        let context_result = result.with_location();

        assert!(context_result.is_err());
        let error = context_result.unwrap_err();
        assert_eq!(error.status, Status::OutOfMemory);
        assert_eq!(error.location, Some("src/core/status.rs:324".to_string()));
    }

    #[test]
    fn test_complex_error_chain() {
        // Create a complex error chain to test deep nesting
        let level1 = ErrorContext::new(Status::DiskFull)
            .with_context("Disk space exhausted")
            .with_location("storage.rs:200");

        let level2 = ErrorContext::new(Status::IoError)
            .with_context("Write operation failed")
            .with_location("writer.rs:150")
            .with_source(level1);

        let level3 = ErrorContext::new(Status::InternalError)
            .with_context("Transaction commit failed")
            .with_location("transaction.rs:100")
            .with_source(level2);

        // Test traversal
        assert_eq!(level3.root_cause(), Status::DiskFull);
        assert!(level3.contains_status(Status::DiskFull));
        assert!(level3.contains_status(Status::IoError));
        assert!(level3.contains_status(Status::InternalError));
        assert!(!level3.contains_status(Status::Corruption));

        // Test display contains all contexts
        let display = format!("{}", level3);
        assert!(display.contains("Transaction commit failed"));
        assert!(display.contains("Write operation failed"));
        assert!(display.contains("Disk space exhausted"));
    }

    #[test]
    fn test_memory_safety_error_contexts() {
        // Test that error contexts don't cause memory leaks with deep chains
        for _ in 0..1000 {
            let mut current = ErrorContext::new(Status::OutOfMemory);

            for i in 0..10 {
                let next = ErrorContext::new(Status::InternalError)
                    .with_context(format!("Level {}", i))
                    .with_source(current);
                current = next;
            }

            // Force deep traversal
            let _ = current.root_cause();
            let _ = format!("{}", current);
        }
    }

    #[test]
    fn test_concurrent_error_creation() {
        // Removed unused Arc import
        use std::thread;

        let handles: Vec<_> = (0..10).map(|i| {
            thread::spawn(move || {
                for j in 0..100 {
                    let context = ErrorContext::new(
                        if (i + j) % 2 == 0 { Status::OutOfMemory } else { Status::IoError }
                    )
                    .with_context(format!("Thread {} iteration {}", i, j))
                    .with_location(format!("thread_{}.rs:{}", i, j));

                    assert!(!context.to_string().is_empty());
                }
            })
        }).collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}