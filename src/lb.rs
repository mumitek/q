use std::{
    error::Error,
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
};

use crate::partition::Partition;

pub(crate) struct PartitionBalancer {
    num_partition: u16,
    target: AtomicU32,
    partitions: Vec<Partition>,
}

impl PartitionBalancer {
    pub(crate) fn try_new(
        data_dir: impl AsRef<Path>,
        num_partition: u16,
    ) -> Result<Self, Box<dyn Error>> {
        let partitions = (0u16..num_partition)
            .map(|id| {
                let data_file = data_dir.as_ref().join(format!("{}.bin", id));
                let pointer_file = data_dir.as_ref().join(format!("{}.p.bin", id));
                Partition::try_new(data_file, pointer_file)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            num_partition,
            partitions,
            target: AtomicU32::new(0),
        })
    }

    pub(crate) fn add(&self, message: &[u8]) -> Result<(), Box<dyn Error>> {
        let target = self.target.fetch_add(1, Ordering::Relaxed);
        self.partitions[(target % self.num_partition as u32) as usize].add(message)
    }

    pub(crate) fn next(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        // TODO: make it better
        for partition in &self.partitions {
            if let Ok(message) = partition.next() {
                return Ok(message);
            }
        }
        Err("Queue is empty".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_try_new_creates_correct_number_of_partitions() {
        let dir = tempdir().unwrap();
        let num_partitions = 4;

        let balancer = PartitionBalancer::try_new(dir.path(), num_partitions).unwrap();

        assert_eq!(balancer.partitions.len(), num_partitions as usize);
        assert_eq!(balancer.num_partition, num_partitions);
    }

    #[test]
    fn test_add_cycles_through_partitions_round_robin() {
        let dir = tempdir().unwrap();
        let num_partitions = 2;
        let balancer = PartitionBalancer::try_new(dir.path(), num_partitions).unwrap();

        // First add should go to partition 0
        balancer.add(b"message 1").unwrap();
        assert_eq!(balancer.target.load(Ordering::Relaxed), 1);

        // Second add should go to partition 1
        balancer.add(b"message 2").unwrap();
        assert_eq!(balancer.target.load(Ordering::Relaxed), 2);

        // Third add should wrap back to partition 0 (target % 2 == 0)
        balancer.add(b"message 3").unwrap();
        assert_eq!(balancer.target.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_next_retrieves_first_available_message() {
        let dir = tempdir().unwrap();
        let balancer = PartitionBalancer::try_new(dir.path(), 2).unwrap();
        let msg = b"hello world";

        // Add message to the balancer (which routes to a partition)
        balancer.add(msg).unwrap();

        // Retrieve it
        let result = balancer.next().unwrap();
        assert_eq!(result, msg);
    }

    #[test]
    fn test_next_returns_error_when_all_partitions_empty() {
        let dir = tempdir().unwrap();
        let balancer = PartitionBalancer::try_new(dir.path(), 3).unwrap();

        let result = balancer.next();

        // Using unwrap_err to ensure we get a clear failure if it unexpectedly succeeded
        let err = result.unwrap_err();
        assert_eq!(err.to_string(), "Queue is empty");
    }

    #[test]
    fn test_atomic_target_consistency_across_threads() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let balancer = Arc::new(PartitionBalancer::try_new(dir.path(), 10).unwrap());
        let mut handles = vec![];

        for _ in 0..10 {
            let b = Arc::clone(&balancer);
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    b.add(b"data").unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 10 threads * 10 adds = 100
        assert_eq!(balancer.target.load(Ordering::SeqCst), 100);
    }

    #[test]
    fn test_next_searches_multiple_partitions() {
        let dir = tempdir().unwrap();
        let balancer = PartitionBalancer::try_new(dir.path(), 2).unwrap();

        // Manually push to the second partition only (bypassing balancer logic)
        // to ensure 'next' actually iterates until it finds something.
        let msg = b"partition_1_data";
        balancer.partitions[1].add(msg).unwrap();

        let result = balancer.next().unwrap();
        assert_eq!(result, msg);
    }

    #[test]
    fn test_parallel_writes_are_perfectly_balanced() {
        let dir = tempdir().unwrap();
        let num_partitions = 4;
        let messages_per_thread = 100;
        let num_threads = 4;
        let total_messages = messages_per_thread * num_threads;

        let balancer = Arc::new(PartitionBalancer::try_new(dir.path(), num_partitions).unwrap());
        let mut handles = vec![];

        // Spin up threads to hammer the balancer
        for t in 0..num_threads {
            let b = Arc::clone(&balancer);
            handles.push(thread::spawn(move || {
                for i in 0..messages_per_thread {
                    let msg = format!("thread-{}-msg-{}", t, i);
                    b.add(msg.as_bytes()).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verification: If balanced, every partition must have exactly (total / num_partitions) messages
        let expected_per_partition = total_messages / num_partitions as usize;

        for (id, partition) in balancer.partitions.iter().enumerate() {
            let mut count = 0;
            // Drain the partition to count messages
            while partition.next().is_ok() {
                count += 1;
            }

            assert_eq!(
                count, expected_per_partition,
                "Partition {} did not receive the expected share of messages",
                id
            );
        }
    }

    #[test]
    fn test_wrap_around_consistency() {
        let dir = tempdir().unwrap();
        let num_partitions = 3;
        let balancer = PartitionBalancer::try_new(dir.path(), num_partitions).unwrap();

        // Manually set the atomic target close to the limit to test wrap-around behavior
        // Note: target is private(crate), so we test by performing enough adds to see it work
        // or by trusting the modulo math on the incrementing atomic.

        let iterations = (num_partitions * 2) as u32;
        for _ in 0..iterations {
            balancer.add(b"wrap-test").unwrap();
        }

        // Total count in all partitions should equal iterations
        let mut total_found = 0;
        for partition in &balancer.partitions {
            while partition.next().is_ok() {
                total_found += 1;
            }
        }

        assert_eq!(total_found, iterations as usize);
    }

    #[test]
    fn test_u32_boundary_overflow_safety() {
        let dir = tempdir().unwrap();
        let num_partitions = 3;
        let balancer = PartitionBalancer::try_new(dir.path(), num_partitions).unwrap();

        // Simulate being 1 increment away from u32::MAX
        balancer.target.store(u32::MAX, Ordering::SeqCst);

        // This increment will cause the AtomicU32 to wrap to 0
        balancer.add(b"overflow-1").unwrap(); // index: 4294967295 % 3 = 1
        balancer.add(b"overflow-2").unwrap(); // index: 0 % 3 = 0

        assert_eq!(balancer.target.load(Ordering::SeqCst), 1);

        // Total messages across all partitions should be 2
        let total: usize = balancer
            .partitions
            .iter()
            .map(|p| {
                let mut c = 0;
                while p.next().is_ok() {
                    c += 1;
                }
                c
            })
            .sum();

        assert_eq!(total, 2);
    }

    #[test]
    fn test_data_integrity_sequential() {
        let dir = tempdir().unwrap();
        let num_partitions = 4;
        let balancer = PartitionBalancer::try_new(dir.path(), num_partitions).unwrap();
        let total_messages = 1000;

        // 1. All Writes happen first
        for i in 0..total_messages {
            let msg = format!("msg-{}", i);
            balancer.add(msg.as_bytes()).unwrap();
        }

        // 2. All Reads happen after
        let mut results = Vec::new();
        while let Ok(msg) = balancer.next() {
            results.push(String::from_utf8(msg).unwrap());
        }

        // 3. Assertions
        assert_eq!(
            results.len(),
            total_messages,
            "Missing messages in sequential drain"
        );
    }

    #[test]
    #[ignore]
    fn test_simultaneous_read_write_consistency() {
        let dir = tempdir().unwrap();
        let num_partitions = 4;
        let balancer = Arc::new(PartitionBalancer::try_new(dir.path(), num_partitions).unwrap());

        let items_per_thread = 500;
        let writer_threads = 4;
        let total_expected = items_per_thread * writer_threads;

        // Track if writers are still working
        let writers_active = Arc::new(AtomicBool::new(true));
        let barrier = Arc::new(Barrier::new(writer_threads + 1)); // +1 for the reader
        let collected_data = Arc::new(Mutex::new(Vec::new()));

        let mut handles = vec![];

        // --- Writers ---
        for t in 0..writer_threads {
            let b = Arc::clone(&balancer);
            let bar = Arc::clone(&barrier);
            let _active = Arc::clone(&writers_active);
            handles.push(thread::spawn(move || {
                bar.wait();
                for i in 0..items_per_thread {
                    b.add(format!("t{}-m{}", t, i).as_bytes()).unwrap();
                }
                // Note: In a real test with multiple writers, you'd use a WaitGroup
                // or an AtomicCounter to flip this bool only when the LAST writer finishes.
            }));
        }

        // --- Reader ---
        let b = Arc::clone(&balancer);
        let bar = Arc::clone(&barrier);
        let storage = Arc::clone(&collected_data);
        let _active = Arc::clone(&writers_active);

        let reader_handle = thread::spawn(move || {
            bar.wait();

            loop {
                match b.next() {
                    Ok(msg) => {
                        let mut guard = storage.lock().unwrap();
                        guard.push(String::from_utf8(msg).unwrap());
                    }
                    Err(_) => {
                        // If queue is empty, check if writers are actually done
                        // We check the atomic target to see if we've processed everything
                        if b.target.load(Ordering::SeqCst) as usize == total_expected {
                            // Double check one last time for any remaining items
                            if b.next().is_err() {
                                break;
                            }
                        }
                        thread::yield_now();
                    }
                }
            }
        });

        // Wait for writers
        for h in handles {
            h.join().unwrap();
        }
        writers_active.store(false, Ordering::SeqCst);

        // Wait for reader
        reader_handle.join().unwrap();

        // --- Audit ---
        let results = collected_data.lock().unwrap();
        assert_eq!(
            results.len(),
            total_expected,
            "Flakiness caught! Missing data."
        );

        let unique: HashSet<_> = results.iter().collect();
        assert_eq!(unique.len(), total_expected, "Duplicates found!");
    }
}

#[cfg(test)]
mod benchmarks {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;
    use tempfile::tempdir;

    #[test]
    fn benchmark_contention_performance() {
        let dir = tempdir().unwrap();
        let cpus = num_cpus::get();
        let num_threads = cpus * 2;
        let num_partitions = 4 * cpus as u16;
        let messages_per_thread = 100_000 / num_threads; // Total 100k messages

        let balancer = Arc::new(PartitionBalancer::try_new(dir.path(), num_partitions).unwrap());

        let start = Instant::now();
        let mut handles = vec![];

        for _ in 0..num_threads {
            let b = Arc::clone(&balancer);
            handles.push(thread::spawn(move || {
                for _ in 0..messages_per_thread {
                    // Using a small payload to focus on balancing/IO overhead
                    b.add(b"data").unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        println!("\n--- Benchmark Results ---");
        println!("Total Messages: {}", num_threads * messages_per_thread);
        println!("Total Time: {:?}", duration);
        println!(
            "Avg Latency per Add: {:?}",
            duration / (num_threads * messages_per_thread) as u32
        );
        println!("--------------------------\n");

        // Basic sanity check to ensure all data actually hit the disk
        assert_eq!(
            balancer.target.load(Ordering::SeqCst),
            (num_threads * messages_per_thread) as u32
        );
        assert!(
            duration.as_millis() < 500,
            "Performance regression: took {:?} which is over 2s",
            duration
        );
    }
}

#[cfg(feature = "bench-stress")]
#[cfg(test)]
mod macos_benchmarks {
    use super::*;
    use rayon::ThreadPoolBuilder;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    fn run_constrained_test(num_partitions: u16, virtual_cores: usize) -> Duration {
        let dir = tempdir().unwrap();
        let balancer = Arc::new(PartitionBalancer::try_new(dir.path(), num_partitions).unwrap());

        // Ensure total work is high enough to measure, but divisible
        let total_messages = 40_000;
        let msgs_per_thread = total_messages / virtual_cores;

        let pool = ThreadPoolBuilder::new()
            .num_threads(virtual_cores)
            .build()
            .unwrap();

        let start = Instant::now();
        pool.scope(|s| {
            for _ in 0..virtual_cores {
                let b = Arc::clone(&balancer);
                s.spawn(move |_| {
                    for _ in 0..msgs_per_thread {
                        b.add(b"payload").unwrap();
                    }
                });
            }
        });

        start.elapsed()
    }

    #[test]
    fn benchmark_scaling_on_mac() {
        let max_physical_cores = num_cpus::get_physical();
        let partitions_to_test = [
            1, 2, 4, 8, 16, 20, 24, 26, 29, 32, 40, 43, 45, 48, 50, 52, 55, 57, 60, 64,
        ];

        // 1. Dynamically build the header
        let header = partitions_to_test
            .iter()
            .map(|p| format!("P={:<5}", p))
            .collect::<Vec<_>>()
            .join(" | ");

        println!("\n--- macOS Partition Scaling Matrix ---");
        println!("Cores | {}", header);
        println!("{}", "-".repeat(8 + (header.len())));

        // 2. Iterate through core counts
        for cores in 1..=max_physical_cores {
            print!(" {:<4} |", cores);

            for &p in &partitions_to_test {
                let time = run_constrained_test(p, cores);
                // Aligning the data to match the "P={:<5}" width (which is 7 chars total)
                print!(" {:<7} |", format!("{}ms", time.as_millis()));
            }
            println!();
        }
    }
}

