use std::{thread, sync::mpsc};
use std::collections::HashSet;
use std::sync::{Arc, Condvar, Mutex};

use anchored_vfs::LevelDBFilesystem;

use crate::snapshot::SnapshotList;
use crate::{
    contention_queue::{ContentionQueue, PanicOptions},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    typed_bytes::{AtomicCloseStatus, CloseStatus},
};
use super::builder::BuildDB;
use super::state::{
    BackgroundCompactorHandle, CompactionState, ForegroundCompactor, FrontWriterState,
    InternalDBState, PerHandleState, SharedMutableState,
};


impl<FS, Cmp, Policy, Codecs, Pool> InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + Send,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
    // TODO: Weaken `Send + Sync` requirements
    Self:               Send + Sync + 'static,
    FS::WriteFile:      Send,
    Codecs::Encoders:   Send,
    Pool::PooledBuffer: Send,
{
    #[must_use]
    pub(super) fn build(
        build_db: BuildDB<FS, Cmp, Policy, Codecs, Pool>,
    ) -> (Arc<Self>, PerHandleState<Codecs::Decoders>) {
        let BuildDB {
            opts,
            mut_opts,
            open_opts,
            lockfile,
            version_set,
            memtable,
            current_write_log,
            next_file_number,
            table_builder,
            encoders,
            decoders,
        } = build_db;

        let memtable_writer = memtable.into_memtable(
            open_opts.unwrap_poison,
            open_opts.memtable_pool_size,
        );
        let current_memtable = memtable_writer.reader();

        let (background, sender, foreground) = if open_opts.compact_in_background {
            let (sender, receiver) = mpsc::sync_channel(0);
            let compactor_thread = thread::spawn(move || {
                let Ok(_shared_state) = receiver.recv() else { return };
                drop(receiver);

                let _table_builder = table_builder;
                let _encoders = encoders;

                // TODO: actually do background work.
                #[expect(clippy::empty_loop, clippy::infinite_loop, reason = "temporary")]
                loop {}
            });

            let background = BackgroundCompactorHandle {
                start_compaction: Condvar::new(),
                compactor_thread,
            };

            (Some(background), Some(sender), None)
        } else {
            let foreground = ForegroundCompactor {
                table_builder,
                encoders,
            };
            (None, None, Some(foreground))
        };

        let mutable_state = SharedMutableState {
            lockfile: Some(lockfile),
            write_status: Ok(()),
            version_set,
            current_memtable,
            iter_read_sample_seed: 0,
            foreground_compactor:  foreground,
            compaction_state:      CompactionState {
                has_ongoing_compaction:     false,
                suspending_compactions:     false,
                memtable_under_compaction:  None,
                pending_compaction_outputs: HashSet::new(),
                manual_compaction:          (),
            },
        };
        let close_status = AtomicCloseStatus::new(CloseStatus::Open);
        let contention_queue = ContentionQueue::new_with_options(
            FrontWriterState {
                memtable_writer,
                current_write_log,
                next_file_number,
            },
            PanicOptions {
                unwrap_mutex_poison: open_opts.unwrap_poison,
                unwrap_queue_poison: open_opts.unwrap_poison,
            },
        );

        let this = Arc::new(Self {
            opts,
            mut_opts,
            mutable_state:        Mutex::new(mutable_state),
            compaction_finished:  Condvar::new(),
            close_status,
            background_compactor: background,
            contention_queue,
            snapshot_list:        SnapshotList::new(),
        });

        if let Some(sender) = sender {
            #[expect(
                clippy::expect_used,
                reason = "there's no reason this should ever panic, but better to loudly error \
                          instead of silently deadlock (when no compactions happen)",
            )]
            sender
                .send(Arc::clone(&this))
                .expect("Background compaction thread failed to properly start");
        }

        let per_handle = PerHandleState {
            decoders,
        };

        (this, per_handle)
    }
}
