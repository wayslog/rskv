use super::light_epoch::*;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::thread;
use std::time::Duration;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_light_epoch_creation() {
        let epoch = LightEpoch::new();
        // Epoch creation should succeed
        assert_eq!(std::mem::size_of_val(&epoch), 0); // Zero-sized type
    }

    #[test]
    fn test_light_epoch_default() {
        let epoch = LightEpoch::default();
        // Default should work the same as new()
        assert_eq!(std::mem::size_of_val(&epoch), 0);
    }

    #[test]
    fn test_epoch_protect() {
        let epoch = LightEpoch::new();
        let guard = epoch.protect();
        // Guard should be created successfully
        // The guard is a crossbeam Guard, we can't test much about its internal state
        drop(guard); // Explicitly drop to end protection
    }

    #[test]
    fn test_epoch_protect_multiple() {
        let epoch = LightEpoch::new();

        // Multiple protections should work
        let guard1 = epoch.protect();
        let guard2 = epoch.protect();
        let guard3 = epoch.protect();

        // All guards should be independent
        drop(guard1);
        drop(guard2);
        drop(guard3);
    }

    #[test]
    fn test_epoch_bump_and_drain() {
        let epoch = LightEpoch::new();

        // Should not panic
        epoch.bump_and_drain();
        epoch.bump_and_drain();
    }

    #[test]
    fn test_epoch_concurrent_access() {
        let epoch = Arc::new(LightEpoch::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Spawn multiple threads to test concurrent access
        for _ in 0..10 {
            let epoch_clone = epoch.clone();
            let counter_clone = counter.clone();

            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let _guard = epoch_clone.protect();
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                    // Small delay to increase chance of race conditions
                    thread::sleep(Duration::from_micros(1));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn test_epoch_guard_scope() {
        let epoch = LightEpoch::new();

        // Test that guards work within different scopes
        {
            let _guard1 = epoch.protect();
            {
                let _guard2 = epoch.protect();
                // Nested protection should work
            }
            // guard2 dropped here
        }
        // guard1 dropped here

        // Should still be able to create new guards
        let _guard3 = epoch.protect();
    }

    #[test]
    fn test_epoch_memory_safety() {
        let epoch = LightEpoch::new();

        // Test potential memory safety issues
        for _ in 0..1000 {
            let guard = epoch.protect();
            epoch.bump_and_drain();
            drop(guard);
        }
    }

    #[test]
    fn test_epoch_stress_concurrent_protect_and_drain() {
        let epoch = Arc::new(LightEpoch::new());
        let mut handles = vec![];

        // Thread that constantly protects and unprotects
        for i in 0..5 {
            let epoch_clone = epoch.clone();
            let handle = thread::spawn(move || {
                for j in 0..200 {
                    let _guard = epoch_clone.protect();
                    if (i + j) % 10 == 0 {
                        epoch_clone.bump_and_drain();
                    }
                    thread::sleep(Duration::from_micros(10));
                }
            });
            handles.push(handle);
        }

        // Thread that constantly drains
        let epoch_drain = epoch.clone();
        let drain_handle = thread::spawn(move || {
            for _ in 0..100 {
                epoch_drain.bump_and_drain();
                thread::sleep(Duration::from_micros(50));
            }
        });
        handles.push(drain_handle);

        for handle in handles {
            handle.join().unwrap();
        }
    }
}