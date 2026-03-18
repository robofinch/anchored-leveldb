#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style lifetime extension"),
)]

use std::sync::Arc;

use anchored_vfs::RandomAccess;

use crate::{table_caches::BlockCache, table_format::InternalComparator};
use crate::{
    all_errors::types::{CorruptedTableError, ReadTableBlockError},
    options::{InternalOptions, InternalOptionsPerRead},
    pub_traits::{
        cmp_and_policy::{FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::{BufferPool, ByteBuffer as _},
    },
    pub_typed_bytes::{BlockHandle, BlockType},
    typed_bytes::{EncodedInternalEntry, InternalKey},
};
use super::{
    data_block::DataBlockIter,
    index_block::IndexBlockIter,
    reader::TableReader,
};


/// This macro calls `next` or `prev` on `self.data_block.0`, and if the result is `Some`,
/// that entry is returned.
///
/// This uses a small amount of `unsafe` code for Polonius, so this macro should be kept internal
/// to this code.
macro_rules! maybe_return_entry {
    ( $iter:expr, $data_block_and_handle:expr, $opts:expr) => {
        let entry_result = if NEXT {
            $iter.next($data_block_and_handle.0.as_slice(), &$opts.cmp)
        } else {
            $iter.prev($data_block_and_handle.0.as_slice(), &$opts.cmp)
        };

        match entry_result {
            Ok(Some(entry)) => {
                // In this branch, `self.index_iter` and `self.data_block_iter` are `valid()`.

                // Unfortunately this is a case where Rust's current NLL borrow checker is overly
                // conservative; the newer, in-progress Polonius borrow checker accepts it.
                // To get this to work on stable Rust requires unsafe code.
                #[cfg(not(feature = "polonius"))]
                // SAFETY: We are doing a transmute that only changes lifetimes, and the code
                // compiles under Polonius, so it's sound.
                let entry = unsafe {
                    ::std::mem::transmute::<
                        crate::typed_bytes::EncodedInternalEntry<'_>,
                        crate::typed_bytes::EncodedInternalEntry<'_>,
                    >(entry)
                };

                return Ok(Some(entry));
            }
            Ok(None) => {},
            Err(seek_err) => {
                return Err(ReadTableBlockError::from_seek_err(
                    BlockType::Data,
                    $data_block_and_handle.1,
                    $iter.current_entry_offset(),
                    seek_err,
                ));
            }
        }
    };
}


pub(crate) struct TableIter<Pool: BufferPool> {
    /// # Invariant
    /// `self.index_iter.valid()` should hold if and only if `data_block` is `Some(_)`.
    ///
    /// `self.index_iter` should be set to the index block of the table which this iterator is set
    /// to (if any).
    index_iter:      IndexBlockIter,
    data_block:      Option<(Arc<Pool::PooledBuffer>, BlockHandle)>,
    /// # Invariant
    /// `self.data_block_iter.valid()` should hold if and only if `data_block` is `Some(_)`.
    ///
    /// In that case, it should also be set to `self.data_block`.
    data_block_iter: DataBlockIter,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Pool: BufferPool> TableIter<Pool> {
    pub fn new<File, Policy>(table: &TableReader<File, Policy, Pool>) -> Self
    where
        File:   RandomAccess,
        Policy: FilterPolicy,
    {
        Self {
            index_iter:      table.index_iter(),
            data_block:      None,
            data_block_iter: DataBlockIter::new_empty(),
        }
    }

    #[inline]
    #[must_use]
    pub const fn new_empty() -> Self {
        Self {
            index_iter:      IndexBlockIter::new_empty(),
            data_block:      None,
            data_block_iter: DataBlockIter::new_empty(),
        }
    }

    pub fn set<File, Policy>(&mut self, table: &TableReader<File, Policy, Pool>)
    where
        File:   RandomAccess,
        Policy: FilterPolicy,
    {
        table.set_index_iter(&mut self.index_iter);
        self.data_block = None;
        self.data_block_iter.clear();
    }

    pub fn clear(&mut self) {
        self.index_iter.clear();
        self.data_block = None;
        self.data_block_iter.clear();
    }

    fn next_or_prev<const NEXT: bool, File, Cmp, Policy, Codecs>(
        &mut self,
        table:       &TableReader<File, Policy, Pool>,
        opts:        &InternalOptions<Cmp, Policy, Codecs, Pool>,
        read_opts:   &InternalOptionsPerRead,
        decoders:    &mut Codecs::Decoders,
        block_cache: &BlockCache<Pool>,
    ) -> Result<
        Option<EncodedInternalEntry<'_>>,
        ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>,
    >
    where
        Cmp:    LevelDBComparator,
        File:   RandomAccess,
        Policy: FilterPolicy,
        Codecs: CompressionCodecs,
    {
        if let Some(data_block) = &mut self.data_block {
            maybe_return_entry!(self.data_block_iter, data_block, opts);
        }

        // Either `self.data_block_iter` is not set, or calling `next` or `prev` made it
        // `!valid()`.
        self.next_or_prev_fallback::<NEXT, _, _, _, _>(
            table,
            opts,
            read_opts,
            decoders,
            block_cache,
        )
    }

    /// Assuming that `self.data_block.is_none()` or that it is `Some(_)` but that
    /// `!self.data_block_iter.is_valid()` -- temporarily violating the invariant of that field --
    /// get either the next entry of the next nonempty block or the previous entry of the previous
    /// nonempty block, depending on whether `NEXT` is true or false.
    ///
    /// After this call, `self.data_block` is either still `None`, or is `Some(_)` and its
    /// iterator is `valid()`. Additionally, `self.index_iter` is `valid()` iff `self.data_block`
    /// is `Some(_)`.
    #[inline(never)]
    fn next_or_prev_fallback<const NEXT: bool, File, Cmp, Policy, Codecs>(
        &mut self,
        table:       &TableReader<File, Policy, Pool>,
        opts:        &InternalOptions<Cmp, Policy, Codecs, Pool>,
        read_opts:   &InternalOptionsPerRead,
        decoders:    &mut Codecs::Decoders,
        block_cache: &BlockCache<Pool>,
    ) -> Result<
        Option<EncodedInternalEntry<'_>>,
        ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>,
    >
    where
        Cmp:    LevelDBComparator,
        File:   RandomAccess,
        Policy: FilterPolicy,
        Codecs: CompressionCodecs,
    {
        let index_block_contents = table.index_block();

        loop {
            let mut existing_buf = self.data_block
                .take()
                .and_then(|(buf, _)| Arc::into_inner(buf));

            let new_block = if NEXT {
                self.index_iter.next(index_block_contents)
            } else {
                self.index_iter.prev(index_block_contents)
            };

            let new_block = new_block.map_err(|index_err| ReadTableBlockError::from_index_err(
                table.index_handle(),
                self.index_iter.current_entry_offset(),
                self.index_iter.current_value_offset(),
                index_err
            ))?;

            // Here, `self.data_block.is_none()`.
            let Some(block_handle) = new_block else { break };

            let block_contents = table.read_data_block(
                block_handle,
                opts,
                read_opts,
                decoders,
                block_cache,
                &mut existing_buf,
            )?;
            self.data_block_iter
                .set(block_contents.as_slice())
                .map_err(|(offset, err)| {
                    ReadTableBlockError::TableCorruption(CorruptedTableError::CorruptedBlock(
                        BlockType::Data,
                        block_handle,
                        offset,
                        err,
                    ))
                })?;

            let data_block = self.data_block.insert((block_contents, block_handle));

            maybe_return_entry!(self.data_block_iter, data_block, opts);
        }

        // In this branch, `self.index_iter` is `!valid()`.
        // Note that we call `self.data_block.take()` before `break`ing above.
        self.data_block_iter.clear();
        Ok(None)
    }

    fn seek_bound<const GEQ: bool, File, Cmp, Policy, Codecs>(
        &mut self,
        table:       &TableReader<File, Policy, Pool>,
        opts:        &InternalOptions<Cmp, Policy, Codecs, Pool>,
        read_opts:   &InternalOptionsPerRead,
        decoders:    &mut Codecs::Decoders,
        block_cache: &BlockCache<Pool>,
        bound:       InternalKey<'_>,
    ) -> Result<(), ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>>
    where
        Cmp:    LevelDBComparator,
        File:   RandomAccess,
        Policy: FilterPolicy,
        Codecs: CompressionCodecs,
    {
        let index_block_contents = table.index_block();

        let result = if GEQ {
            self.index_iter.try_seek(index_block_contents, &opts.cmp, bound)
        } else {
            self.index_iter.try_seek_before(index_block_contents, &opts.cmp, bound)
        };

        result.map_err(|seek_err| ReadTableBlockError::from_seek_err(
            BlockType::Index,
            table.index_handle(),
            self.index_iter.current_entry_offset(),
            seek_err,
        ))?;

        let mut current_index = self.index_iter
            .current_mapped_err(index_block_contents)
            .map_err(ReadTableBlockError::TableCorruption)?;

        let mut existing_buf = self.data_block
            .take()
            .and_then(|(buf, _)| Arc::into_inner(buf));

        while let Some(block_handle) = current_index {
            let block_contents = table.read_data_block(
                block_handle,
                opts,
                read_opts,
                decoders,
                block_cache,
                &mut existing_buf,
            )?;

            self.data_block_iter
                .set(block_contents.as_slice())
                .map_err(|(offset, err)| {
                    ReadTableBlockError::TableCorruption(CorruptedTableError::CorruptedBlock(
                        BlockType::Data,
                        block_handle,
                        offset,
                        err,
                    ))
                })?;

            let seek_result = if GEQ {
                self.data_block_iter.try_seek(block_contents.as_slice(), &opts.cmp, bound)
            } else {
                self.data_block_iter.try_seek_before(block_contents.as_slice(), &opts.cmp, bound)
            };

            if let Err(seek_err) = seek_result {
                return Err(ReadTableBlockError::from_seek_err(
                    BlockType::Index,
                    table.index_handle(),
                    self.index_iter.current_entry_offset(),
                    seek_err,
                ));
            }

            if self.data_block_iter.valid() {
                self.data_block = Some((block_contents, block_handle));
                // In this branch, `self.index_iter` and `self.data_block_iter` are `valid()`.
                return Ok(());
            } else {
                existing_buf = Arc::into_inner(block_contents);
                let index_res = if GEQ {
                    self.index_iter.next(index_block_contents)
                } else {
                    self.index_iter.prev(index_block_contents)
                };

                current_index = index_res.map_err(|index_err| ReadTableBlockError::from_index_err(
                    table.index_handle(),
                    self.index_iter.current_entry_offset(),
                    self.index_iter.current_value_offset(),
                    index_err
                ))?;
                // Note that `self.data_block.is_none()` here.
            }
        }

        // In this branch, we seeked too far forwards or backwards;
        // `self.index_iter` is `!valid()`, and we make `self.data_block_iter` be not initialized.
        self.data_block_iter.clear();
        Ok(())
    }

    #[inline]
    pub const fn valid(&self) -> bool {
        // See the invariants of `self`'s fields.
        self.data_block.is_some()
    }

    pub fn next<File, Cmp, Policy, Codecs>(
        &mut self,
        table:       &TableReader<File, Policy, Pool>,
        opts:        &InternalOptions<Cmp, Policy, Codecs, Pool>,
        read_opts:   &InternalOptionsPerRead,
        decoders:    &mut Codecs::Decoders,
        block_cache: &BlockCache<Pool>,
    ) -> Result<
        Option<EncodedInternalEntry<'_>>,
        ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>,
    >
    where
        Cmp:    LevelDBComparator,
        File:   RandomAccess,
        Policy: FilterPolicy,
        Codecs: CompressionCodecs,
    {
        self.next_or_prev::<true, _, _, _, _>(table, opts, read_opts, decoders, block_cache)
    }

    #[inline]
    pub fn current<Cmp: LevelDBComparator, Decompression>(
        &self,
        cmp: &InternalComparator<Cmp>,
    ) -> Result<
        Option<EncodedInternalEntry<'_>>,
        CorruptedTableError<Cmp::InvalidKeyError, Decompression>,
    > {
        if let Some((data_block, handle)) = &self.data_block {
            self.data_block_iter.current_mapped_err(data_block.as_slice(), *handle, cmp)
        } else {
            Ok(None)
        }
    }

    pub fn prev<File, Cmp, Policy, Codecs>(
        &mut self,
        table:       &TableReader<File, Policy, Pool>,
        opts:        &InternalOptions<Cmp, Policy, Codecs, Pool>,
        read_opts:   &InternalOptionsPerRead,
        decoders:    &mut Codecs::Decoders,
        block_cache: &BlockCache<Pool>,
    ) -> Result<
        Option<EncodedInternalEntry<'_>>,
        ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>,
    >
    where
        Cmp:    LevelDBComparator,
        File:   RandomAccess,
        Policy: FilterPolicy,
        Codecs: CompressionCodecs,
    {
        self.next_or_prev::<false, _, _, _, _>(table, opts, read_opts, decoders, block_cache)
    }

    pub fn reset(&mut self) {
        self.index_iter.reset();
        self.data_block = None;
        self.data_block_iter.clear();
    }

    pub fn seek<File, Cmp, Policy, Codecs>(
        &mut self,
        table:       &TableReader<File, Policy, Pool>,
        opts:        &InternalOptions<Cmp, Policy, Codecs, Pool>,
        read_opts:   &InternalOptionsPerRead,
        decoders:    &mut Codecs::Decoders,
        block_cache: &BlockCache<Pool>,
        min_bound:   InternalKey<'_>,
    ) -> Result<(), ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>>
    where
        Cmp:    LevelDBComparator,
        File:   RandomAccess,
        Policy: FilterPolicy,
        Codecs: CompressionCodecs,
    {
        self.seek_bound::<true, _, _, _, _>(
            table,
            opts,
            read_opts,
            decoders,
            block_cache,
            min_bound,
        )
    }

    pub fn seek_before<File, Cmp, Policy, Codecs>(
        &mut self,
        table:              &TableReader<File, Policy, Pool>,
        opts:               &InternalOptions<Cmp, Policy, Codecs, Pool>,
        read_opts:          &InternalOptionsPerRead,
        decoders:           &mut Codecs::Decoders,
        block_cache:        &BlockCache<Pool>,
        strict_upper_bound: InternalKey<'_>,
    ) -> Result<(), ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>>
    where
        Cmp:    LevelDBComparator,
        File:   RandomAccess,
        Policy: FilterPolicy,
        Codecs: CompressionCodecs,
    {
        self.seek_bound::<false, _, _, _, _>(
            table,
            opts,
            read_opts,
            decoders,
            block_cache,
            strict_upper_bound,
        )
    }

    pub fn seek_to_first<File, Cmp, Policy, Codecs>(
        &mut self,
        table:       &TableReader<File, Policy, Pool>,
        opts:        &InternalOptions<Cmp, Policy, Codecs, Pool>,
        read_opts:   &InternalOptionsPerRead,
        decoders:    &mut Codecs::Decoders,
        block_cache: &BlockCache<Pool>,
    ) -> Result<(), ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>>
    where
        Cmp:    LevelDBComparator,
        File:   RandomAccess,
        Policy: FilterPolicy,
        Codecs: CompressionCodecs,
    {
        self.reset();
        self.next(table, opts, read_opts, decoders, block_cache)?;
        Ok(())
    }

    pub fn seek_to_last<File, Cmp, Policy, Codecs>(
        &mut self,
        table:       &TableReader<File, Policy, Pool>,
        opts:        &InternalOptions<Cmp, Policy, Codecs, Pool>,
        read_opts:   &InternalOptionsPerRead,
        decoders:    &mut Codecs::Decoders,
        block_cache: &BlockCache<Pool>,
    ) -> Result<(), ReadTableBlockError<Cmp::InvalidKeyError, Codecs::DecompressionError>>
    where
        Cmp:    LevelDBComparator,
        File:   RandomAccess,
        Policy: FilterPolicy,
        Codecs: CompressionCodecs,
    {
        self.reset();
        self.prev(table, opts, read_opts, decoders, block_cache)?;
        Ok(())
    }
}
