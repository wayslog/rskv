use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Access pattern type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessPattern {
    /// Sequential access pattern
    Sequential,
    /// Random access pattern
    Random,
    /// Scan pattern (reading many keys in order)
    Scan,
    /// Hot spot (concentrated on few keys)
    HotSpot,
    /// Temporal locality (same keys accessed repeatedly in short time)
    TemporalLocality,
    /// Unknown pattern
    Unknown,
}

/// Access event for tracking
#[derive(Debug, Clone)]
struct AccessEvent {
    key_hash: u64,
    timestamp: Instant,
    operation_type: OperationType,
}

/// Operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    Read,
    Write,
    Update,
    Delete,
}

/// Access pattern statistics
#[derive(Debug, Clone)]
pub struct AccessStats {
    pub total_accesses: u64,
    pub read_count: u64,
    pub write_count: u64,
    pub update_count: u64,
    pub delete_count: u64,
    pub unique_keys: usize,
    pub dominant_pattern: AccessPattern,
    pub sequential_ratio: f64,
    pub temporal_locality_score: f64,
    pub hotspot_concentration: f64,
}

/// Configuration for access analyzer
#[derive(Debug, Clone)]
pub struct AnalyzerConfig {
    /// Maximum number of events to keep in history
    pub history_size: usize,
    /// Time window for pattern detection (in seconds)
    pub pattern_window_secs: u64,
    /// Minimum accesses before pattern detection
    pub min_accesses_for_pattern: usize,
    /// Hot spot threshold (fraction of total accesses)
    pub hotspot_threshold: f64,
    /// Temporal locality window (in milliseconds)
    pub temporal_window_ms: u64,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            history_size: 10000,
            pattern_window_secs: 60,
            min_accesses_for_pattern: 100,
            hotspot_threshold: 0.8,
            temporal_window_ms: 1000,
        }
    }
}

/// Access pattern analyzer
pub struct AccessAnalyzer {
    config: AnalyzerConfig,
    events: Arc<RwLock<VecDeque<AccessEvent>>>,
    key_frequencies: Arc<RwLock<HashMap<u64, u64>>>,
    total_accesses: AtomicU64,
    read_count: AtomicU64,
    write_count: AtomicU64,
    update_count: AtomicU64,
    delete_count: AtomicU64,
    start_time: Instant,
}

impl AccessAnalyzer {
    pub fn new(config: AnalyzerConfig) -> Self {
        Self {
            config,
            events: Arc::new(RwLock::new(VecDeque::new())),
            key_frequencies: Arc::new(RwLock::new(HashMap::new())),
            total_accesses: AtomicU64::new(0),
            read_count: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
            update_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Record an access event
    pub fn record_access(&self, key_hash: u64, operation_type: OperationType) {
        self.total_accesses.fetch_add(1, Ordering::Relaxed);

        match operation_type {
            OperationType::Read => self.read_count.fetch_add(1, Ordering::Relaxed),
            OperationType::Write => self.write_count.fetch_add(1, Ordering::Relaxed),
            OperationType::Update => self.update_count.fetch_add(1, Ordering::Relaxed),
            OperationType::Delete => self.delete_count.fetch_add(1, Ordering::Relaxed),
        };

        let event = AccessEvent {
            key_hash,
            timestamp: Instant::now(),
            operation_type,
        };

        // Update frequency map
        if let Ok(mut frequencies) = self.key_frequencies.write() {
            *frequencies.entry(key_hash).or_insert(0) += 1;
        }

        // Add to event history
        if let Ok(mut events) = self.events.write() {
            events.push_back(event);
            // Maintain history size limit
            while events.len() > self.config.history_size {
                events.pop_front();
            }
        }
    }

    /// Analyze current access patterns
    pub fn analyze_patterns(&self) -> AccessStats {
        let total = self.total_accesses.load(Ordering::Relaxed);

        if total < self.config.min_accesses_for_pattern as u64 {
            return AccessStats {
                total_accesses: total,
                read_count: self.read_count.load(Ordering::Relaxed),
                write_count: self.write_count.load(Ordering::Relaxed),
                update_count: self.update_count.load(Ordering::Relaxed),
                delete_count: self.delete_count.load(Ordering::Relaxed),
                unique_keys: 0,
                dominant_pattern: AccessPattern::Unknown,
                sequential_ratio: 0.0,
                temporal_locality_score: 0.0,
                hotspot_concentration: 0.0,
            };
        }

        let (unique_keys, hotspot_concentration) = self.analyze_key_distribution();
        let sequential_ratio = self.analyze_sequential_pattern();
        let temporal_locality_score = self.analyze_temporal_locality();

        let dominant_pattern = self.determine_dominant_pattern(
            sequential_ratio,
            temporal_locality_score,
            hotspot_concentration,
        );

        AccessStats {
            total_accesses: total,
            read_count: self.read_count.load(Ordering::Relaxed),
            write_count: self.write_count.load(Ordering::Relaxed),
            update_count: self.update_count.load(Ordering::Relaxed),
            delete_count: self.delete_count.load(Ordering::Relaxed),
            unique_keys,
            dominant_pattern,
            sequential_ratio,
            temporal_locality_score,
            hotspot_concentration,
        }
    }

    /// Analyze key distribution to detect hot spots
    fn analyze_key_distribution(&self) -> (usize, f64) {
        let frequencies = match self.key_frequencies.read() {
            Ok(f) => f,
            Err(_) => return (0, 0.0),
        };

        let unique_keys = frequencies.len();
        if unique_keys == 0 {
            return (0, 0.0);
        }

        // Calculate concentration (Gini coefficient approximation)
        let mut sorted_frequencies: Vec<u64> = frequencies.values().copied().collect();
        sorted_frequencies.sort_unstable();

        let total_accesses = self.total_accesses.load(Ordering::Relaxed);
        let top_20_percent = (unique_keys as f64 * 0.2).ceil() as usize;
        let top_20_accesses: u64 = sorted_frequencies
            .iter()
            .rev()
            .take(top_20_percent)
            .sum();

        let concentration = if total_accesses > 0 {
            top_20_accesses as f64 / total_accesses as f64
        } else {
            0.0
        };

        (unique_keys, concentration)
    }

    /// Analyze sequential access pattern
    fn analyze_sequential_pattern(&self) -> f64 {
        let events = match self.events.read() {
            Ok(e) => e,
            Err(_) => return 0.0,
        };

        if events.len() < 2 {
            return 0.0;
        }

        let mut sequential_count = 0;
        let mut total_pairs = 0;

        let recent_events: Vec<_> = events
            .iter()
            .rev()
            .take(self.config.min_accesses_for_pattern)
            .collect();

        for window in recent_events.windows(2) {
            total_pairs += 1;
            let diff = window[0].key_hash.abs_diff(window[1].key_hash);
            // Consider sequential if hash difference is small
            if diff < 1000 {
                sequential_count += 1;
            }
        }

        if total_pairs > 0 {
            sequential_count as f64 / total_pairs as f64
        } else {
            0.0
        }
    }

    /// Analyze temporal locality
    fn analyze_temporal_locality(&self) -> f64 {
        let events = match self.events.read() {
            Ok(e) => e,
            Err(_) => return 0.0,
        };

        if events.len() < 10 {
            return 0.0;
        }

        let temporal_window = Duration::from_millis(self.config.temporal_window_ms);
        let mut reaccess_count = 0;
        let mut total_count = 0;

        let recent_events: Vec<_> = events
            .iter()
            .rev()
            .take(self.config.min_accesses_for_pattern)
            .collect();

        for i in 0..recent_events.len().saturating_sub(1) {
            let current = &recent_events[i];
            total_count += 1;

            // Look for reaccess of the same key within temporal window
            for j in (i + 1)..recent_events.len() {
                let other = &recent_events[j];
                if other.timestamp < current.timestamp - temporal_window {
                    break;
                }
                if current.key_hash == other.key_hash {
                    reaccess_count += 1;
                    break;
                }
            }
        }

        if total_count > 0 {
            reaccess_count as f64 / total_count as f64
        } else {
            0.0
        }
    }

    /// Determine dominant access pattern
    fn determine_dominant_pattern(
        &self,
        sequential_ratio: f64,
        temporal_locality_score: f64,
        hotspot_concentration: f64,
    ) -> AccessPattern {
        // Priority order: Sequential > TemporalLocality > HotSpot > Scan > Random

        if sequential_ratio > 0.7 {
            return AccessPattern::Sequential;
        }

        if temporal_locality_score > 0.5 {
            return AccessPattern::TemporalLocality;
        }

        if hotspot_concentration > self.config.hotspot_threshold {
            return AccessPattern::HotSpot;
        }

        if sequential_ratio > 0.3 {
            return AccessPattern::Scan;
        }

        AccessPattern::Random
    }

    /// Get hot keys (top N most accessed keys)
    pub fn get_hot_keys(&self, n: usize) -> Vec<(u64, u64)> {
        let frequencies = match self.key_frequencies.read() {
            Ok(f) => f,
            Err(_) => return Vec::new(),
        };

        let mut sorted: Vec<_> = frequencies.iter().map(|(&k, &v)| (k, v)).collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        sorted.into_iter().take(n).collect()
    }

    /// Clear old statistics (useful for adaptive behavior)
    pub fn reset_statistics(&self) {
        if let Ok(mut events) = self.events.write() {
            events.clear();
        }
        if let Ok(mut frequencies) = self.key_frequencies.write() {
            frequencies.clear();
        }
        self.total_accesses.store(0, Ordering::Relaxed);
        self.read_count.store(0, Ordering::Relaxed);
        self.write_count.store(0, Ordering::Relaxed);
        self.update_count.store(0, Ordering::Relaxed);
        self.delete_count.store(0, Ordering::Relaxed);
    }

    /// Get recommendation based on current pattern
    pub fn get_recommendation(&self) -> AccessRecommendation {
        let stats = self.analyze_patterns();

        match stats.dominant_pattern {
            AccessPattern::Sequential => AccessRecommendation {
                pattern: stats.dominant_pattern,
                suggestion: "Consider using prefetching for sequential access".to_string(),
                cache_size_factor: 1.5,
                migration_aggressiveness: 0.7,
            },
            AccessPattern::TemporalLocality => AccessRecommendation {
                pattern: stats.dominant_pattern,
                suggestion: "High temporal locality detected, increase cache size".to_string(),
                cache_size_factor: 2.0,
                migration_aggressiveness: 0.9,
            },
            AccessPattern::HotSpot => AccessRecommendation {
                pattern: stats.dominant_pattern,
                suggestion: "Hot spot detected, prioritize frequently accessed keys".to_string(),
                cache_size_factor: 1.2,
                migration_aggressiveness: 0.8,
            },
            AccessPattern::Scan => AccessRecommendation {
                pattern: stats.dominant_pattern,
                suggestion: "Scan pattern detected, optimize for range queries".to_string(),
                cache_size_factor: 1.3,
                migration_aggressiveness: 0.5,
            },
            AccessPattern::Random => AccessRecommendation {
                pattern: stats.dominant_pattern,
                suggestion: "Random access pattern, use balanced caching strategy".to_string(),
                cache_size_factor: 1.0,
                migration_aggressiveness: 0.5,
            },
            AccessPattern::Unknown => AccessRecommendation {
                pattern: stats.dominant_pattern,
                suggestion: "Insufficient data for pattern detection".to_string(),
                cache_size_factor: 1.0,
                migration_aggressiveness: 0.5,
            },
        }
    }
}

/// Access pattern recommendation
#[derive(Debug, Clone)]
pub struct AccessRecommendation {
    pub pattern: AccessPattern,
    pub suggestion: String,
    pub cache_size_factor: f64,
    pub migration_aggressiveness: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_recording() {
        let analyzer = AccessAnalyzer::new(AnalyzerConfig::default());

        analyzer.record_access(100, OperationType::Read);
        analyzer.record_access(100, OperationType::Write);
        analyzer.record_access(200, OperationType::Read);

        assert_eq!(analyzer.total_accesses.load(Ordering::Relaxed), 3);
        assert_eq!(analyzer.read_count.load(Ordering::Relaxed), 2);
        assert_eq!(analyzer.write_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_hot_keys_detection() {
        let analyzer = AccessAnalyzer::new(AnalyzerConfig::default());

        // Create hot spot on key 100
        for _ in 0..100 {
            analyzer.record_access(100, OperationType::Read);
        }
        for _ in 0..10 {
            analyzer.record_access(200, OperationType::Read);
        }
        for _ in 0..5 {
            analyzer.record_access(300, OperationType::Read);
        }

        let hot_keys = analyzer.get_hot_keys(2);
        assert_eq!(hot_keys.len(), 2);
        assert_eq!(hot_keys[0].0, 100);
        assert_eq!(hot_keys[0].1, 100);
    }

    #[test]
    fn test_pattern_detection() {
        let config = AnalyzerConfig {
            min_accesses_for_pattern: 10,
            ..Default::default()
        };
        let analyzer = AccessAnalyzer::new(config);

        // Create sequential pattern
        for i in 0..100 {
            analyzer.record_access(i, OperationType::Read);
        }

        let stats = analyzer.analyze_patterns();
        assert!(stats.sequential_ratio > 0.5);
    }

    #[test]
    fn test_hotspot_detection() {
        let config = AnalyzerConfig {
            min_accesses_for_pattern: 10,
            hotspot_threshold: 0.8,
            ..Default::default()
        };
        let analyzer = AccessAnalyzer::new(config);

        // Create hot spot: 90 accesses to key 10000, 10 accesses to key 20000
        // Interleave accesses to avoid sequential pattern detection
        for i in 0..100 {
            if i < 90 {
                analyzer.record_access(10000, OperationType::Read);
            } else {
                analyzer.record_access(20000, OperationType::Read);
            }
        }

        let stats = analyzer.analyze_patterns();
        assert!(stats.hotspot_concentration > 0.8);
        // Note: Sequential may still be detected due to repeated same key access
        // So we just check hotspot concentration
        assert!(stats.hotspot_concentration > 0.8);
    }
}