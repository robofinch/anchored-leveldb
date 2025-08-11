#![allow(unexpected_cfgs, reason = "Distinguish whether to use `loom`, and stop Miri.")]
#![allow(unused_crate_dependencies, reason = "These are tests, not the main crate.")]
#![allow(
    unused_imports, dead_code,
    reason = "Depending on cfg, some are unused. Annoying to annotate.",
)]

#[cfg(all(loom, not(miri)))]
mod maybe_loom {
    pub(super) use loom::sync::Arc as Arc;
    pub(super) use loom::sync::atomic::AtomicBool as AtomicBool;
    pub(super) use loom::sync::atomic::AtomicU32 as AtomicU32;
    pub(super) use loom::sync::mpsc::channel as mpsc_channel;
    pub(super) use loom::thread::spawn as thread_spawn;
}

#[cfg(all(not(loom), not(miri)))]
mod maybe_loom {
    pub(super) use std::sync::Arc as Arc;
    pub(super) use std::sync::atomic::AtomicBool as AtomicBool;
    pub(super) use std::sync::atomic::AtomicU32 as AtomicU32;
    pub(super) use std::sync::mpsc::channel as mpsc_channel;
    pub(super) use std::thread::spawn as thread_spawn;
}


use std::array;
use std::iter;
use std::{cmp::Ordering as CmpOrdering, sync::atomic::Ordering};

use oorandom::Rand32;

use anchored_skiplist::{DefaultComparator, Skiplist, SkiplistIterator};
use anchored_skiplist::threadsafe::{LockedThreadsafeSkiplist, ThreadsafeSkiplist};
use self::maybe_loom::*;


/// The number of distinct groups of entry values.
///
/// Must be at most 15.
const NUM_GROUPS: usize = 4;

#[cfg(not(loom))]
/// The number of insertions performed by each writer in an insertion step.
const INSERTIONS_PER_STEP: usize = 10_000;
#[cfg(loom)]
#[cfg(not(loom_hard))]
/// The number of insertions performed by each writer in an insertion step.
const INSERTIONS_PER_STEP: usize = 5;
#[cfg(loom)]
#[cfg(loom_hard)]
/// The number of insertions performed by each writer in an insertion step.
const INSERTIONS_PER_STEP: usize = 2;

#[cfg(loom)]
const MAX_READER_STEPS: u32 = 1;


#[cfg(not(miri))]
#[test]
fn reader_writer() {
    #[cfg(not(loom))]
    reader_writer_impl();
    #[cfg(all(loom, loom_hard))]
    loom::model(reader_writer_impl);
}

/// - Spawn 1 thread that constantly reads the skiplist
/// - Spawn 1 thread that inserts 10,000 elements, write-locks the skiplist, and inserts 10,000 more
/// - Join the writer
/// - Tell the reader to stop
/// - Join the reader
/// - Confirm that there are 20,000 elements in the skiplist
fn reader_writer_impl() {
    let skiplist = ThreadsafeSkiplist::new(DefaultComparator);
    let (mut writer, generations) = Writer::new(1, 0, 13);
    let mut reader = Reader::new(vec![generations], 21);
    let continue_reading = Arc::new(AtomicBool::new(true));

    let reader_thread = thread_spawn({
        let skiplist = skiplist.refcounted_clone();
        let continue_reading = continue_reading.clone();
        move || {
            #[cfg(loom)]
            let mut num_iterations: u32 = 0;

            while continue_reading.load(Ordering::Relaxed) {
                reader.read_step(&skiplist);
                #[cfg(loom)]
                {
                    num_iterations += 1;
                    if num_iterations >= MAX_READER_STEPS {
                        break;
                    }
                }
            }
        }
    });

    let writer_thread = thread_spawn({
        let mut skiplist = skiplist.refcounted_clone();
        move || {
            writer.insertion_step(&mut skiplist);
            let mut write_locked = skiplist.write_locked();
            writer.insertion_step(&mut write_locked);
        }
    });

    writer_thread.join().unwrap();
    continue_reading.store(false, Ordering::Relaxed);
    reader_thread.join().unwrap();

    assert_eq!(skiplist.iter().count(), INSERTIONS_PER_STEP * 2)
}

#[cfg(not(miri))]
#[test]
fn writer_writer() {
    #[cfg(not(loom))]
    writer_writer_impl();
    #[cfg(loom)]
    loom::model(writer_writer_impl);
}

/// - Spawn 1 thread that inserts 10,000 elements, signals and waits, and inserts 10,000 more
/// - Spawn 1 thread that inserts 10,000 elements, signals and waits, write-locks the skiplist,
///   signals and waits, inserts 10,000 more, and signals and waits
/// - Wait for both of those threads to send a signal
/// - Tell the second thread to proceed and write-lock the skiplist
/// - Wait for the second thread to send a signal
/// - Confirm that there are 20,000 elements in the skiplist
/// - Signal both threads to proceed trying to insert elements
/// - Wait for the second thread to send a signal
/// - Confirm that there are 30,000 elements in the skiplist
/// - Tell the second thread to proceed
/// - Join the second thread
/// - Join the first thread
/// - Confirm that there are 40,000 elements in the skiplist
fn writer_writer_impl() {
    let skiplist = ThreadsafeSkiplist::new(DefaultComparator);
    let (mut writer_one, _) = Writer::new(2, 0, 34);
    let (mut writer_two, _) = Writer::new(2, 1, 55);

    let (signal_one, wait_for_main_one) = mpsc_channel();
    let (signal_two, wait_for_main_two) = mpsc_channel();
    let (signal_main_one, wait_for_one) = mpsc_channel();
    let (signal_main_two, wait_for_two) = mpsc_channel();

    let writer_one_thread = thread_spawn({
        let mut skiplist = skiplist.refcounted_clone();
        move || {
            writer_one.insertion_step(&mut skiplist);
            signal_main_one.send(()).unwrap();
            wait_for_main_one.recv().unwrap();
            writer_one.insertion_step(&mut skiplist);
        }
    });

    let writer_two_thread = thread_spawn({
        let mut skiplist = skiplist.refcounted_clone();
        move || {
            writer_two.insertion_step(&mut skiplist);
            signal_main_two.send(()).unwrap();
            wait_for_main_two.recv().unwrap();

            let mut write_locked = skiplist.write_locked().write_locked();
            write_locked = ThreadsafeSkiplist::write_unlocked(write_locked).write_locked();
            let mut still_write_locked = LockedThreadsafeSkiplist::write_unlocked(
                write_locked.write_locked(),
            );
            signal_main_two.send(()).unwrap();
            wait_for_main_two.recv().unwrap();

            writer_two.insertion_step(&mut still_write_locked);
            signal_main_two.send(()).unwrap();
            wait_for_main_two.recv().unwrap();

        }
    });

    // Wait for both writers to insert `10_000` elements
    wait_for_one.recv().unwrap();
    wait_for_two.recv().unwrap();

    // Tell the second thread to acquire the write lock
    signal_two.send(()).unwrap();
    wait_for_two.recv().unwrap();

    assert_eq!(skiplist.iter().count(), INSERTIONS_PER_STEP * 2);

    // Tell the first thread to try to continue (it won't be able to)
    signal_one.send(()).unwrap();
    // Tell the second one to continue (it will succeed)
    signal_two.send(()).unwrap();
    wait_for_two.recv().unwrap();

    assert_eq!(skiplist.iter().count(), INSERTIONS_PER_STEP * 3);

    // Tell the second one to finish, and drop its write lock
    signal_two.send(()).unwrap();
    writer_two_thread.join().unwrap();

    // Now the first thread should start back up and write stuff
    writer_one_thread.join().unwrap();

    assert_eq!(skiplist.iter().count(), INSERTIONS_PER_STEP * 4);
}

/// - Spawn 4 threads that constantly read the skiplist
/// - Spawn 2 threads that each insert 10,000 elements per iteration
/// - Spawn 1 thread which will write-lock the skiplist and insert 10,000 elements
/// - In a loop with 500 iterations:
///   - Signal the two normal writers to start their iterations
///   - Wait for the writers to finish their iteration
///   - Signal the write-locking thread to start
///   - Wait for the write-locked thread to finish its iteration
///   - Confirm that there are `30,000 * i` elements in the skiplist.
/// - Tell all the threads to stop, and signal all the writers so that they notice
#[cfg(all(not(loom), not(miri)))]
#[ignore = "this takes on the order of 30 seconds to run"]
#[test]
fn probabilistic() {
    const NUM_READERS: usize = 4;
    const NUM_WRITERS: usize = 2;
    const NUM_WRITE_LOCKING: usize = 1;

    const NUM_ITERATIONS: usize = 500;

    let total_num_writers: u8 = (NUM_WRITERS + NUM_WRITE_LOCKING) as u8;

    // ================================================================
    //  Initialization of readers and writers
    // ================================================================

    let reader_seeds: [u64; NUM_READERS] = [89, 144, 233, 377];
    let writer_seeds: [u64; NUM_WRITERS] = [42, 68];
    let write_locking_seeds: [u64; NUM_WRITE_LOCKING] = [163];

    let (writers, mut all_generations): (Vec<_>, Vec<_>) = writer_seeds
        .into_iter()
        .enumerate()
        .map(|(writer_id, seed)| {
            Writer::new(total_num_writers, writer_id as u8, seed)
        })
        .unzip();

    let write_locking: Vec<_> = write_locking_seeds
        .into_iter()
        .enumerate()
        .map(|(index, seed)| {
            let writer_id = (NUM_WRITERS + index) as u8;
            let (writer, generations) = Writer::new(total_num_writers, writer_id, seed);
            all_generations.push(generations);
            writer
        })
        .collect();

    let readers = reader_seeds.map(|seed| Reader::new(all_generations.clone(), seed));

    // ================================================================
    //  Initialization of threads
    // ================================================================

    let skiplist = ThreadsafeSkiplist::new(DefaultComparator);
    let continue_looping = Arc::new(AtomicBool::new(true));

    let (
        signal_writers,
        wait_for_main,
    ): (Vec<_>, Vec<_>) = iter::repeat_with(mpsc_channel).take(NUM_WRITERS).unzip();
    let (signal_main, wait_for_writers) = mpsc_channel();

    let (
        signal_locking,
        locking_wait_for_main,
    ): (Vec<_>, Vec<_>) = iter::repeat_with(mpsc_channel).take(NUM_WRITE_LOCKING).unzip();
    let (locking_signal_main, wait_for_write_locking) = mpsc_channel();

    let reader_threads = readers.map(|mut reader| {
        let skiplist = skiplist.refcounted_clone();
        let continue_looping = continue_looping.clone();
        thread_spawn(move || {
            while continue_looping.load(Ordering::Relaxed) {
                reader.read_step(&skiplist);
            }
        })
    });

    let writer_threads = iter::zip(writers, wait_for_main)
        .map(|(mut writer, wait_for_main)| {
            let mut skiplist = skiplist.refcounted_clone();
            let continue_looping = continue_looping.clone();
            let signal_main = signal_main.clone();
            thread_spawn(move || {
                loop {
                    wait_for_main.recv().unwrap();
                    // `Acquire` ensures that if main updated `continue_looping` just before sending
                    // something, we see it.
                    if !continue_looping.load(Ordering::Acquire) {
                        break;
                    }
                    writer.insertion_step(&mut skiplist);
                    signal_main.send(()).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();

    let write_locking_threads = iter::zip(write_locking, locking_wait_for_main)
        .map(|(mut writer, wait_for_main)| {
            let mut skiplist = skiplist.refcounted_clone();
            let continue_looping = continue_looping.clone();
            let signal_main = locking_signal_main.clone();
            thread_spawn(move || {
                loop {
                    wait_for_main.recv().unwrap();
                    if !continue_looping.load(Ordering::Acquire) {
                        break;
                    }

                    let mut write_locked = skiplist.write_locked();
                    writer.insertion_step(&mut write_locked);
                    skiplist = ThreadsafeSkiplist::write_unlocked(write_locked);

                    signal_main.send(()).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();

    // ================================================================
    //  Run everything
    // ================================================================

    for i in 0..NUM_ITERATIONS {
        let iteration = i + 1;
        println!("Iteration {iteration:3} / {NUM_ITERATIONS}");

        signal_writers.iter().for_each(|signal| signal.send(()).unwrap());
        for _ in 0..NUM_WRITERS {
            wait_for_writers.recv().unwrap();
        }

        signal_locking.iter().for_each(|signal| signal.send(()).unwrap());
        for _ in 0..NUM_WRITE_LOCKING {
            wait_for_write_locking.recv().unwrap();
        }

        let insertions_per_iteration = INSERTIONS_PER_STEP * usize::from(total_num_writers);
        assert_eq!(skiplist.iter().count(), insertions_per_iteration * iteration);
    }

    continue_looping.store(false, Ordering::Release);
    signal_writers.iter().for_each(|signal| signal.send(()).unwrap());
    signal_locking.iter().for_each(|signal| signal.send(()).unwrap());

    reader_threads.into_iter().for_each(|thread| thread.join().unwrap());
    writer_threads.into_iter().for_each(|thread| thread.join().unwrap());
    write_locking_threads.into_iter().for_each(|thread| thread.join().unwrap());
}

#[derive(Debug)]
struct Reader {
    writer_generations: Vec<Generations>,
    prng:               Rand32,
}

impl Reader {
    fn new(
        writer_generations: Vec<Generations>,
        seed:               u64,
    ) -> Self {
        let prng = Rand32::new(seed);
        Self { writer_generations, prng }
    }

    fn read_step<List: Skiplist<DefaultComparator>>(&mut self, skiplist: &List) {
        let generation_snapshots = self
            .writer_generations
            .iter()
            .map(Generations::snapshot)
            .collect::<Vec<_>>();

        let mut current_pos = Entry::random_target(&mut self.prng);
        let mut iter = skiplist.iter();
        iter.seek(&current_pos.bytes());

        let num_writers = self.writer_generations.len() as u32;

        loop {
            let seeked_to = if let Some(seeked_to) = iter.current() {
                Entry::from_slice(seeked_to)
            } else {
                Entry::end_of_list()
            };

            assert!(current_pos <= seeked_to);

            // If there were anything in `[current_pos, seeked_to)` in the initial state, we should
            // have seeked to it just now. We verify that there isn't anything there.

            // Check that `current_pos` and the following few generations should not exist
            let check_up_to = seeked_to.generation.min(current_pos.generation + num_writers);
            for generation in current_pos.generation..check_up_to {
                assert!(!Generations::entry_should_exist(
                    &generation_snapshots,
                    &Entry::new(current_pos.group, generation),
                ));
            }

            for group_to_check in (current_pos.group + 1)..seeked_to.group {
                // There should've existed no entries at all inserted in this group when the
                // snapshots were taken. Check that that's true.
                for writer in 0..num_writers {
                    assert!(!Generations::entry_should_exist(
                        &generation_snapshots,
                        &Entry::new(group_to_check, writer + num_writers),
                    ));
                }
            }

            let first_generation_to_check = if current_pos.group == seeked_to.group {
                current_pos.generation
            } else {
                // We know `0..num_writers` will always pass the check
                num_writers
            };

            for generation_to_check in first_generation_to_check..seeked_to.generation {
                assert!(!Generations::entry_should_exist(
                    &generation_snapshots,
                    &Entry::new(seeked_to.group, generation_to_check),
                ));
            }

            if !iter.is_valid() {
                break;
            }

            // Either move one generation past `seeked_to`, to `seeked_to` itself,
            // or move randomly forwards
            current_pos = if self.prng.rand_u32() % 2 == 0 {
                iter.next();
                Entry::new(seeked_to.group, seeked_to.generation + 1)
            } else {
                let random_target = Entry::random_target(&mut self.prng);
                // Ensure that we always make forwards progress, to avoid indefinitely
                // ping-ponging around the skiplist
                if random_target > current_pos {
                    iter.seek(random_target.bytes().as_slice());
                    random_target
                } else {
                    // Move `current_pos` up to what we seeked to
                    seeked_to
                }
            };
        }
    }
}

#[derive(Debug)]
struct Writer {
    num_writers: u8,
    #[allow(unused, reason = "useful for println debugging")]
    writer_id:   u8,
    generations: Generations,
    prng:        Rand32,
}

impl Writer {
    fn new(num_writers: u8, writer_id: u8, seed: u64) -> (Self, Generations) {
        assert!(writer_id < num_writers);
        let generations = Generations::new(writer_id);

        let writer = Self {
            num_writers,
            writer_id,
            generations: generations.clone(),
            prng:        Rand32::new(seed),
        };

        (writer, generations)
    }

    fn insertion_step<List: Skiplist<DefaultComparator>>(&mut self, skiplist: &mut List) {
        for _ in 0..INSERTIONS_PER_STEP {
            self.insert_entry(skiplist);
        }
    }

    fn insert_entry<List: Skiplist<DefaultComparator>>(&mut self, skiplist: &mut List) {
        let group = self.prng.rand_range(0..NUM_GROUPS as u32) as u8;
        let generation = self.generations.load_next_generation(group, self.num_writers);

        // if generation % 10_000 < self.num_writers as u32 {
        //     println!(
        //         "Writer {} inserting entry with group {}, gen {}",
        //         self.writer_id, group, generation,
        //     );
        // }

        let entry = Entry::new(group, generation).bytes();
        // Assert that the entry is new
        assert!(skiplist.insert_with(entry.len(), |slice| slice.copy_from_slice(&entry)));

        self.generations.store_generation(group, generation);
    }
}

/// Invariant enforced by the code in these tests: if a value in `Generations`, possibly obtained
/// from `Generations::snapshot`, is greater than or equal to `num_writers`, then an entry with
/// that generation has been inserted into the skiplist by the corresponding writer.
#[derive(Debug, Clone)]
struct Generations(Arc<[AtomicU32; NUM_GROUPS]>);

impl Generations {
    fn new(writer_id: u8) -> Self {
        // Set each generation to initially be the lowest generation associated with the writer,
        // which is `writer_id`.
        let generations = array::from_fn(|_| AtomicU32::new(u32::from(writer_id)));
        Self(Arc::new(generations))
    }

    /// Should only be called from methods of the `Writer` struct
    fn load_next_generation(&self, group: u8, num_writers: u8) -> u32 {
        self.0[group as usize].load(Ordering::Acquire) + u32::from(num_writers)
    }

    /// Should only be called from methods of the `Writer` struct.
    ///
    /// Must only be called after an entry with generation `generation` has already been inserted.
    fn store_generation(&self, group: u8, generation: u32) {
        self.0[group as usize].store(generation, Ordering::Release)
    }

    fn snapshot(&self) -> [u32; NUM_GROUPS] {
        self.0.each_ref().map(|generation| generation.load(Ordering::Acquire))
    }

    /// Returns `true` if `entry` should have been inserted into the skiplist, given that writers
    /// had the indicated generation snapshots.
    ///
    /// # Panics
    /// Panics if `generation_snapshots` is length 0, or if the entry's group is larger than
    /// `NUM_GROUPS`.
    fn entry_should_exist(generation_snapshots: &[[u32; NUM_GROUPS]], entry: &Entry) -> bool {
        let num_writers = generation_snapshots.len() as u32;

        // `load_next_generation` can never return anything that small
        if entry.generation < num_writers {
            return false;
        }

        let writer = entry.generation % num_writers;
        let snapshot = generation_snapshots[writer as usize];

        let inserted_generations = snapshot[usize::from(entry.group)];
        // If the entry was inserted by a certain writer, in a certain group, where entries with
        // at least the entry's own generation had been inserted, then the entry should have been
        // inserted.
        entry.generation <= inserted_generations
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    group:      u8,
    generation: u32,
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match self.group.cmp(&other.group) {
            CmpOrdering::Equal => self.generation.cmp(&other.generation),
            other              => other,
        }
    }
}

impl Entry {
    fn new(group: u8, generation: u32) -> Self {
        Self { group, generation }
    }

    fn end_of_list() -> Self {
        Self::new(NUM_GROUPS as u8, 0)
    }

    /// Choose a random entry to seek to
    fn random_target(prng: &mut Rand32) -> Self {
        match prng.rand_range(0..10) {
            // Seek to the start of the list
            0 => Self::new(0, 0),
            // Seek to the end of the list
            1 => Self::end_of_list(),
            // Seek to somewhere potentially in the middle (or sometimes the start)
            _ => Self::new(prng.rand_range(0..NUM_GROUPS as u32) as u8, 0),
        }
    }

    fn hash(&self) -> u32 {
        let seed = 100_u64
            .wrapping_mul(u64::from(self.generation))
            .wrapping_add(u64::from(self.group));
        Rand32::new(seed).rand_u32()
    }

    fn bytes(self) -> [u8; 9] {
        let mut bytes = [0; 9];
        bytes[0] = self.group;
        bytes[1..5].copy_from_slice(&self.generation.to_be_bytes());
        bytes[5..9].copy_from_slice(&self.hash().to_be_bytes());

        bytes
    }

    /// # Panics
    /// Panics if the provided `slice` is not length 9, or if the slice has an invalid hash.
    fn from_slice(slice: &[u8]) -> Self {
        let group      = slice[0];
        let generation = u32::from_be_bytes(slice[1..5].try_into().unwrap());
        let hash       = u32::from_be_bytes(slice[5..].try_into().unwrap());

        let entry = Self::new(group, generation);
        assert_eq!(hash, entry.hash());
        entry
    }
}
