use super::phase::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phase_creation() {
        // Test all phase variants can be created
        let _prep = Phase::PrepIndexChkpt;
        let _index = Phase::IndexChkpt;
        let _prepare = Phase::Prepare;
        let _in_progress = Phase::InProgress;
        let _wait_pending = Phase::WaitPending;
        let _wait_flush = Phase::WaitFlush;
        let _rest = Phase::Rest;
        let _persistence = Phase::PersistenceCallback;
        let _gc_io = Phase::GcIoPending;
        let _gc_progress = Phase::GcInProgress;
        let _grow_prep = Phase::GrowPrepare;
        let _grow_progress = Phase::GrowInProgress;
        let _invalid = Phase::Invalid;
    }

    #[test]
    fn test_phase_as_str() {
        // Test string representations
        assert_eq!(Phase::PrepIndexChkpt.as_str(), "PREP_INDEX_CHKPT");
        assert_eq!(Phase::IndexChkpt.as_str(), "INDEX_CHKPT");
        assert_eq!(Phase::Prepare.as_str(), "PREPARE");
        assert_eq!(Phase::InProgress.as_str(), "IN_PROGRESS");
        assert_eq!(Phase::WaitPending.as_str(), "WAIT_PENDING");
        assert_eq!(Phase::WaitFlush.as_str(), "WAIT_FLUSH");
        assert_eq!(Phase::Rest.as_str(), "REST");
        assert_eq!(Phase::PersistenceCallback.as_str(), "PERSISTENCE_CALLBACK");
        assert_eq!(Phase::GcIoPending.as_str(), "GC_IO_PENDING");
        assert_eq!(Phase::GcInProgress.as_str(), "GC_IN_PROGRESS");
        assert_eq!(Phase::GrowPrepare.as_str(), "GROW_PREPARE");
        assert_eq!(Phase::GrowInProgress.as_str(), "GROW_IN_PROGRESS");
        assert_eq!(Phase::Invalid.as_str(), "INVALID");
    }

    #[test]
    fn test_phase_equality() {
        // Test equality comparisons
        assert_eq!(Phase::PrepIndexChkpt, Phase::PrepIndexChkpt);
        assert_eq!(Phase::InProgress, Phase::InProgress);
        assert_ne!(Phase::PrepIndexChkpt, Phase::IndexChkpt);
        assert_ne!(Phase::InProgress, Phase::WaitPending);
    }

    #[test]
    fn test_phase_debug() {
        // Test debug formatting
        let phase = Phase::InProgress;
        let debug_str = format!("{:?}", phase);
        assert!(debug_str.contains("InProgress"));
    }

    #[test]
    fn test_phase_clone_copy() {
        // Test Clone and Copy traits
        let original = Phase::GcInProgress;
        let cloned = original.clone();
        let copied = original;

        assert_eq!(original, cloned);
        assert_eq!(original, copied);
        assert_eq!(cloned, copied);
    }

    #[test]
    fn test_phase_repr_values() {
        // Test that enum values match expected representation
        assert_eq!(Phase::PrepIndexChkpt as u8, 0);
        assert_eq!(Phase::IndexChkpt as u8, 1);
        assert_eq!(Phase::Prepare as u8, 2);
        assert_eq!(Phase::InProgress as u8, 3);
        assert_eq!(Phase::WaitPending as u8, 4);
        assert_eq!(Phase::WaitFlush as u8, 5);
        assert_eq!(Phase::Rest as u8, 6);
        assert_eq!(Phase::PersistenceCallback as u8, 7);
        assert_eq!(Phase::GcIoPending as u8, 8);
        assert_eq!(Phase::GcInProgress as u8, 9);
        assert_eq!(Phase::GrowPrepare as u8, 10);
        assert_eq!(Phase::GrowInProgress as u8, 11);
        assert_eq!(Phase::Invalid as u8, 12);
    }

    #[test]
    fn test_phase_ordering() {
        // Test ordering of phases
        assert!(Phase::PrepIndexChkpt < Phase::IndexChkpt);
        assert!(Phase::IndexChkpt < Phase::Prepare);
        assert!(Phase::Prepare < Phase::InProgress);
        assert!(Phase::InProgress < Phase::WaitPending);
        assert!(Phase::WaitPending < Phase::WaitFlush);
        assert!(Phase::WaitFlush < Phase::Rest);
        assert!(Phase::Rest < Phase::PersistenceCallback);
        assert!(Phase::PersistenceCallback < Phase::GcIoPending);
        assert!(Phase::GcIoPending < Phase::GcInProgress);
        assert!(Phase::GcInProgress < Phase::GrowPrepare);
        assert!(Phase::GrowPrepare < Phase::GrowInProgress);
        assert!(Phase::GrowInProgress < Phase::Invalid);
    }

    #[test]
    fn test_phase_string_not_empty() {
        // Ensure all phases have non-empty string representations
        let all_phases = [
            Phase::PrepIndexChkpt,
            Phase::IndexChkpt,
            Phase::Prepare,
            Phase::InProgress,
            Phase::WaitPending,
            Phase::WaitFlush,
            Phase::Rest,
            Phase::PersistenceCallback,
            Phase::GcIoPending,
            Phase::GcInProgress,
            Phase::GrowPrepare,
            Phase::GrowInProgress,
            Phase::Invalid,
        ];

        for phase in &all_phases {
            assert!(!phase.as_str().is_empty(), "Phase {:?} has empty string representation", phase);
            assert!(phase.as_str().len() > 0, "Phase {:?} string length is 0", phase);
        }
    }

    #[test]
    fn test_phase_checkpoint_sequence() {
        // Test logical sequence of checkpoint phases
        let checkpoint_phases = [
            Phase::PrepIndexChkpt,
            Phase::IndexChkpt,
            Phase::Prepare,
            Phase::InProgress,
            Phase::WaitPending,
            Phase::WaitFlush,
            Phase::Rest,
            Phase::PersistenceCallback,
        ];

        // Verify they are in ascending order
        for i in 1..checkpoint_phases.len() {
            assert!(
                checkpoint_phases[i-1] < checkpoint_phases[i],
                "Checkpoint phases not in order: {:?} should be < {:?}",
                checkpoint_phases[i-1], checkpoint_phases[i]
            );
        }
    }

    #[test]
    fn test_phase_gc_sequence() {
        // Test GC phases are ordered correctly
        assert!(Phase::GcIoPending < Phase::GcInProgress);
        assert!(Phase::PersistenceCallback < Phase::GcIoPending);
    }

    #[test]
    fn test_phase_grow_sequence() {
        // Test grow phases are ordered correctly
        assert!(Phase::GrowPrepare < Phase::GrowInProgress);
        assert!(Phase::GcInProgress < Phase::GrowPrepare);
    }

    #[test]
    fn test_phase_memory_size() {
        // Verify Phase is a single byte (u8)
        assert_eq!(std::mem::size_of::<Phase>(), 1);
        assert_eq!(std::mem::align_of::<Phase>(), 1);
    }

    #[test]
    fn test_phase_match_patterns() {
        // Test pattern matching works correctly
        let phase = Phase::InProgress;

        let result = match phase {
            Phase::PrepIndexChkpt => "checkpoint_prep",
            Phase::IndexChkpt => "checkpoint_index",
            Phase::Prepare => "prepare",
            Phase::InProgress => "in_progress",
            Phase::WaitPending => "wait_pending",
            Phase::WaitFlush => "wait_flush",
            Phase::Rest => "rest",
            Phase::PersistenceCallback => "persistence",
            Phase::GcIoPending => "gc_io",
            Phase::GcInProgress => "gc_progress",
            Phase::GrowPrepare => "grow_prep",
            Phase::GrowInProgress => "grow_progress",
            Phase::Invalid => "invalid",
        };

        assert_eq!(result, "in_progress");
    }

    #[test]
    fn test_phase_collection() {
        // Test phases can be used in collections
        use std::collections::{HashMap, HashSet};

        let mut phase_set = HashSet::new();
        phase_set.insert(Phase::InProgress);
        phase_set.insert(Phase::WaitPending);
        phase_set.insert(Phase::InProgress); // Duplicate

        assert_eq!(phase_set.len(), 2); // Should only contain 2 unique phases

        let mut phase_map = HashMap::new();
        phase_map.insert(Phase::InProgress, "active");
        phase_map.insert(Phase::Rest, "idle");

        assert_eq!(phase_map.get(&Phase::InProgress), Some(&"active"));
        assert_eq!(phase_map.get(&Phase::Rest), Some(&"idle"));
        assert_eq!(phase_map.get(&Phase::Invalid), None);
    }

    #[test]
    fn test_phase_functional_categories() {
        // Test categorization of phases by function
        let checkpoint_phases = [
            Phase::PrepIndexChkpt,
            Phase::IndexChkpt,
            Phase::Prepare,
            Phase::InProgress,
            Phase::WaitPending,
            Phase::WaitFlush,
            Phase::Rest,
            Phase::PersistenceCallback,
        ];

        let gc_phases = [Phase::GcIoPending, Phase::GcInProgress];
        let grow_phases = [Phase::GrowPrepare, Phase::GrowInProgress];

        // All checkpoint phases should be less than all GC phases
        for &cp in &checkpoint_phases {
            for &gc in &gc_phases {
                assert!(cp < gc, "Checkpoint phase {:?} should be < GC phase {:?}", cp, gc);
            }
        }

        // All GC phases should be less than all grow phases
        for &gc in &gc_phases {
            for &grow in &grow_phases {
                assert!(gc < grow, "GC phase {:?} should be < Grow phase {:?}", gc, grow);
            }
        }

        // Invalid should be the highest
        for &phase in checkpoint_phases.iter().chain(&gc_phases).chain(&grow_phases) {
            assert!(phase < Phase::Invalid, "Phase {:?} should be < Invalid", phase);
        }
    }
}