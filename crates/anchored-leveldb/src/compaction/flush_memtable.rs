use anchored_vfs::LevelDBFilesystem;
use clone_behavior::FastMirroredClone;

use crate::{
    file_tracking::FileMetadata,
    memtable::MemtableIter,
    pub_typed_bytes::FileNumber,
    table_file::TableFileBuilder,
};
use crate::{
    all_errors::{
        aliases::RwErrorKindAlias,
        types::{
            AddTableEntryError, OutOfFileNumbers, RwErrorKind, WriteError,
        },
    },
    options::{InternallyMutableOptions, InternalOptions},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
};


/// Writes the entries of the memtable to zero or more table files.
///
/// Note that the given memtable iterator is not `reset()`.
///
/// If the memtable iterator is empty, zero table files are used. Otherwise, table files are split
/// **only** when absolutely necessary (for the sake of not overfilling the table's index block),
/// regardless of settings for table file size. (This means that, almost always, at most one table
/// file is used.)
///
/// Note that if the builder was already active, the previous table file would be closed, but
/// it would _not_ be properly finished *or* deleted. That file would be an invalid table file
/// and should eventually be garbage collected by this program.
///
/// This function can be called on a builder at any time (regardless of whether it's active).
/// When this function returns, the builder is [inactive].
///
/// [inactive]: TableFileBuilder::active
#[expect(
    clippy::too_many_arguments,
    reason = "the first five arguments can't easily be conglomerated",
)]
pub(crate) fn flush_memtable<FS, Cmp, Policy, Codecs, Pool, F>(
    builder:             &mut TableFileBuilder<FS::WriteFile, Policy, Pool>,
    opts:                &InternalOptions<Cmp, Policy, Codecs>,
    mut_opts:            &InternallyMutableOptions<FS, Policy, Pool>,
    encoders:            &mut Codecs::Encoders,
    decoders:            &mut Codecs::Decoders,
    manifest_number:     FileNumber,
    mut get_file_number: F,
    mut memtable_iter:   MemtableIter<'_, Cmp>,
) -> Result<Vec<FileMetadata>, RwErrorKindAlias<FS, Cmp, Codecs>>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
    F:      FnMut() -> Result<FileNumber, OutOfFileNumbers>,
{
    let mut created_file_metadata = Vec::new();

    while let Some(mut current) = memtable_iter.next() {
        let table_file_number = get_file_number()
            .map_err(|OutOfFileNumbers {}| RwErrorKind::Write(WriteError::OutOfFileNumbers))?;

        builder.start(opts, mut_opts, table_file_number, None).map_err(RwErrorKind::Write)?;

        let smallest_key = current.0;

        // Correctness: the memtable is sorted solely by internal key
        // (in the same way in which `InternalComparator<Cmp>` would sort the internal keys)
        // and does not have any entries with duplicate keys.
        match builder.add_entry(opts, mut_opts, encoders, current.0, current.1) {
            Ok(()) => (),
            // Perhaps it would be ideal to avoid using `unreachable` (in favor of better
            // indicating the possible return values), but this is fine.
            #[expect(
                clippy::unreachable,
                reason = "not worth juggling where the proof of unreachability goes",
            )]
            Err(AddTableEntryError::AddEntryError) => unreachable!(
                "`TableBuilder::add_entry(empty_table, ..)` cannot return `AddEntryError`",
            ),
            Err(AddTableEntryError::Write(err)) => return Err(err),
        }

        let largest_key = loop {
            // Correctness: the memtable is sorted solely by internal key
            // (in the same way in which `InternalComparator<Cmp>` would sort the internal keys)
            // and does not have any entries with duplicate keys.
            match builder.add_entry(opts, mut_opts, encoders, current.0, current.1) {
                Ok(()) => {
                    if let Some(next) = memtable_iter.next() {
                        current = next;
                    } else {
                        break current.0;
                    }
                }
                Err(AddTableEntryError::AddEntryError) => break current.0,
                Err(AddTableEntryError::Write(err)) => return Err(err),
            }
        };

        created_file_metadata.push(builder.finish(
            opts,
            mut_opts,
            encoders,
            decoders,
            manifest_number,
            smallest_key.as_internal_key(),
            largest_key.as_internal_key(),
        )?);
    }

    Ok(created_file_metadata)
}
