use crate::model::{Backend, DEFAULT_READ_FILES_PER_WORKER, Operation, ScenarioSpec};

pub const KIB: u64 = 1024;
pub const MIB: u64 = 1024 * KIB;
pub const DEFAULT_READ_GRANULARITY: u32 = 128 * 1024;
pub const LARGE_READ_GRANULARITY: u32 = 1024 * 1024;
pub const DEFAULT_SHARDS: usize = 64;
const LATENCY_OBJECT_SIZES: [u64; 4] = [32 * KIB, 128 * KIB, 512 * KIB, 2 * MIB];
const STANDARD_DURATION_MS: u64 = 3_000;
const STANDARD_SCALE_DURATION_MS: u64 = 4_000;
const STANDARD_WARMUP_MS: u64 = 750;

pub fn closed_loop_suite(
    quick: bool,
    seed: u64,
    requested_repetitions: u32,
    diagnostics: bool,
) -> Vec<ScenarioSpec> {
    let repetitions = if quick { 1 } else { requested_repetitions };
    let duration_ms = if quick { 350 } else { STANDARD_DURATION_MS };
    let scale_duration_ms = if quick {
        450
    } else {
        STANDARD_SCALE_DURATION_MS
    };
    let warmup_ms = if quick { 100 } else { STANDARD_WARMUP_MS };
    let mut specs = Vec::new();

    for repetition in 1..=repetitions {
        for &size in &LATENCY_OBJECT_SIZES {
            for operation in [
                Operation::Get,
                Operation::GetKnownSize,
                Operation::PutCreateNew,
                Operation::PutOverwrite,
            ] {
                for backend in [Backend::NfsCrust, Backend::LinuxRemote] {
                    specs.push(ScenarioSpec::closed_loop(
                        "latency-profile",
                        backend,
                        operation,
                        size,
                        1,
                        1,
                        DEFAULT_READ_GRANULARITY,
                        DEFAULT_SHARDS,
                        repetition,
                        warmup_ms,
                        duration_ms,
                    ));
                }
            }
        }

        for &size in &[128 * KIB, MIB] {
            for concurrency in [1, 8, 32] {
                for operation in [Operation::GetKnownSize, Operation::PutCreateNew] {
                    for backend in [Backend::NfsCrust, Backend::LinuxRemote] {
                        specs.push(ScenarioSpec::closed_loop(
                            "concurrency-scaling",
                            backend,
                            operation,
                            size,
                            concurrency,
                            1,
                            DEFAULT_READ_GRANULARITY,
                            DEFAULT_SHARDS,
                            repetition,
                            warmup_ms,
                            scale_duration_ms,
                        ));
                    }
                }
            }
        }

        if diagnostics {
            for backend in [Backend::NfsCrust, Backend::LinuxRemote] {
                for files_per_worker in [DEFAULT_READ_FILES_PER_WORKER, 64] {
                    specs.push(
                        ScenarioSpec::closed_loop(
                            "read-working-set",
                            backend,
                            Operation::GetKnownSize,
                            128 * KIB,
                            32,
                            1,
                            DEFAULT_READ_GRANULARITY,
                            DEFAULT_SHARDS,
                            repetition,
                            warmup_ms,
                            scale_duration_ms,
                        )
                        .with_read_files_per_worker(files_per_worker),
                    );
                }
            }

            for clients in [1, 4] {
                for operation in [Operation::GetKnownSize, Operation::PutCreateNew] {
                    specs.push(ScenarioSpec::closed_loop(
                        "connection-pool",
                        Backend::NfsCrust,
                        operation,
                        MIB,
                        32,
                        clients,
                        DEFAULT_READ_GRANULARITY,
                        DEFAULT_SHARDS,
                        repetition,
                        warmup_ms,
                        scale_duration_ms,
                    ));
                }
            }

            for shards in [1, DEFAULT_SHARDS] {
                specs.push(ScenarioSpec::closed_loop(
                    "directory-sharding",
                    Backend::NfsCrust,
                    Operation::PutCreateNew,
                    128 * KIB,
                    32,
                    1,
                    DEFAULT_READ_GRANULARITY,
                    shards,
                    repetition,
                    warmup_ms,
                    scale_duration_ms,
                ));
            }

            for granularity in [DEFAULT_READ_GRANULARITY, LARGE_READ_GRANULARITY] {
                specs.push(ScenarioSpec::closed_loop(
                    "read-granularity",
                    Backend::NfsCrust,
                    Operation::Get,
                    4 * KIB,
                    1,
                    1,
                    granularity,
                    DEFAULT_SHARDS,
                    repetition,
                    warmup_ms,
                    duration_ms,
                ));
                for concurrency in [1, 32] {
                    specs.push(ScenarioSpec::closed_loop(
                        "read-granularity",
                        Backend::NfsCrust,
                        Operation::GetKnownSize,
                        MIB,
                        concurrency,
                        1,
                        granularity,
                        DEFAULT_SHARDS,
                        repetition,
                        warmup_ms,
                        scale_duration_ms,
                    ));
                }
            }

            for &size in &[4 * KIB, MIB] {
                for concurrency in [1, 32] {
                    specs.push(ScenarioSpec::closed_loop(
                        "linux-page-cache-context",
                        Backend::LinuxCached,
                        Operation::GetKnownSize,
                        size,
                        concurrency,
                        1,
                        DEFAULT_READ_GRANULARITY,
                        DEFAULT_SHARDS,
                        repetition,
                        warmup_ms,
                        if quick { 300 } else { 2_000 },
                    ));
                }
            }

            for &size in &[128 * KIB, MIB] {
                for concurrency in [1, 8, 32] {
                    specs.push(ScenarioSpec::closed_loop(
                        "known-size-reference",
                        Backend::LinuxRemoteTrustedSize,
                        Operation::GetKnownSize,
                        size,
                        concurrency,
                        1,
                        DEFAULT_READ_GRANULARITY,
                        DEFAULT_SHARDS,
                        repetition,
                        warmup_ms,
                        scale_duration_ms,
                    ));
                }
            }

            for backend in [Backend::NfsCrust, Backend::LinuxRemote] {
                specs.push(ScenarioSpec::closed_loop(
                    "operation-detail",
                    backend,
                    Operation::GetRange4k,
                    MIB,
                    1,
                    1,
                    DEFAULT_READ_GRANULARITY,
                    DEFAULT_SHARDS,
                    repetition,
                    warmup_ms,
                    duration_ms,
                ));
                specs.push(
                    ScenarioSpec::closed_loop(
                        "operation-detail",
                        backend,
                        Operation::EntryInfo,
                        128 * KIB,
                        1,
                        1,
                        DEFAULT_READ_GRANULARITY,
                        1,
                        repetition,
                        warmup_ms,
                        0,
                    )
                    .with_fixed_operations(if quick { 20 } else { 1_000 }),
                );
                specs.push(
                    ScenarioSpec::closed_loop(
                        "operation-detail",
                        backend,
                        Operation::Delete,
                        4 * KIB,
                        1,
                        1,
                        DEFAULT_READ_GRANULARITY,
                        DEFAULT_SHARDS,
                        repetition,
                        0,
                        0,
                    )
                    .with_fixed_operations(if quick { 10 } else { 250 }),
                );
                specs.push(
                    ScenarioSpec::closed_loop(
                        "operation-detail",
                        backend,
                        Operation::List100,
                        4 * KIB,
                        1,
                        1,
                        DEFAULT_READ_GRANULARITY,
                        1,
                        repetition,
                        warmup_ms,
                        0,
                    )
                    .with_fixed_operations(if quick { 10 } else { 100 }),
                );
            }
        }
    }

    deterministic_shuffle(&mut specs, seed);
    specs
}

fn deterministic_shuffle<T>(values: &mut [T], mut state: u64) {
    if values.len() < 2 {
        return;
    }
    for index in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let swap_with = (state as usize) % (index + 1);
        values.swap(index, swap_with);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn scenario_ids_are_unique() {
        let suite = closed_loop_suite(false, 42, 5, true);
        let mut ids = HashSet::new();
        for spec in &suite {
            assert!(ids.insert(&spec.id), "duplicate scenario id: {}", spec.id);
        }
    }

    #[test]
    fn diagnostics_are_opt_in() {
        let suite = closed_loop_suite(true, 42, 5, false);
        assert_eq!(suite.len(), 56);
        let families: HashSet<_> = suite.iter().map(|spec| spec.family.as_str()).collect();
        assert_eq!(
            families,
            HashSet::from(["latency-profile", "concurrency-scaling"])
        );

        let suite = closed_loop_suite(true, 42, 5, true);
        assert_eq!(suite.len(), 90);
        let families: HashSet<_> = suite.iter().map(|spec| spec.family.as_str()).collect();
        assert!(families.contains("connection-pool"));
        assert!(families.contains("directory-sharding"));
        assert!(families.contains("read-granularity"));
        assert!(families.contains("linux-page-cache-context"));
        assert!(families.contains("operation-detail"));
        assert!(families.contains("known-size-reference"));
        assert!(families.contains("read-working-set"));
    }

    #[test]
    fn latency_profile_uses_one_object_size_grid() {
        let suite = closed_loop_suite(true, 42, 1, false);
        let sizes_for = |operations: &[Operation]| {
            suite
                .iter()
                .filter(|spec| {
                    spec.family == "latency-profile" && operations.contains(&spec.operation)
                })
                .map(|spec| spec.object_size_bytes)
                .collect::<std::collections::BTreeSet<_>>()
        };

        assert_eq!(
            sizes_for(&[Operation::Get, Operation::GetKnownSize]),
            LATENCY_OBJECT_SIZES.into_iter().collect()
        );
        assert_eq!(
            sizes_for(&[Operation::PutCreateNew, Operation::PutOverwrite]),
            LATENCY_OBJECT_SIZES.into_iter().collect()
        );
    }

    #[test]
    fn standard_timed_windows_stay_bounded() {
        let suite = closed_loop_suite(false, 42, 1, true);
        assert!(
            suite
                .iter()
                .all(|spec| spec.warmup_ms <= STANDARD_WARMUP_MS)
        );
        assert!(
            suite
                .iter()
                .all(|spec| spec.duration_ms <= STANDARD_SCALE_DURATION_MS)
        );
    }

    #[test]
    fn publishable_suite_honors_requested_repetitions() {
        let suite = closed_loop_suite(false, 42, 5, false);
        assert_eq!(suite.iter().map(|spec| spec.repetition).max(), Some(5));
    }
}
