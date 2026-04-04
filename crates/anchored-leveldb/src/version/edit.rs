use std::{borrow::Cow, collections::BTreeSet, sync::Arc};

use crate::{
    all_errors::types::VersionEditDecodeError,
    file_tracking::FileMetadata,
    options::pub_options::SeekCompactionOptions,
};
use crate::{
    pub_typed_bytes::{
        FileNumber, FileSize, Level, ReadPrefixedBytes as _, SequenceNumber, ShortSlice,
        VersionEditKeyType,
    },
    typed_bytes::{
        CompactionPointer, EncodedInternalKey, InternalKey, UnvalidatedInternalKey, UserKey,
        VersionEditTag,
    },
    utils::{ReadVarint as _, WriteVarint as _},
};


#[derive(Debug)]
pub(crate) struct VersionEdit {
    /// # Panics
    /// Downstream panics may occur if the length of this field exceeds `u32::MAX`.
    pub comparator_name:     Option<Cow<'static, [u8]>>,
    /// On writes, this is the file number of the current `.log` file.
    ///
    /// On reads, this is the minimum file number of the current `.log` file.
    pub log_number:          Option<FileNumber>,
    pub prev_log_number:     Option<FileNumber>,
    pub next_file_number:    Option<FileNumber>,
    pub last_sequence:       Option<SequenceNumber>,
    pub compaction_pointers: Vec<(Level, CompactionPointer)>,
    pub deleted_files:       BTreeSet<(Level, FileNumber)>,
    pub added_files:         Vec<(Level, Arc<FileMetadata>)>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl VersionEdit {
    #[inline]
    #[must_use]
    pub const fn new_empty() -> Self {
        Self {
            comparator_name:     None,
            log_number:          None,
            prev_log_number:     None,
            next_file_number:    None,
            last_sequence:       None,
            compaction_pointers: Vec::new(),
            deleted_files:       BTreeSet::new(),
            added_files:         Vec::new(),
        }
    }

    pub fn decode_from<V, InvalidKey>(
        input:                 &mut &[u8],
        opts:                  SeekCompactionOptions,
        mut validate_user_key: V,
    ) -> Result<Self, VersionEditDecodeError<InvalidKey>>
    where
        V: FnMut(UserKey<'_>) -> Result<(), InvalidKey>,
    {
        let mut edit = Self::new_empty();

        while !input.is_empty() {
            let tag = read_tag(input)?;

            match tag {
                VersionEditTag::Comparator => {
                    // Note that `read_comparator_name` returns a `Vec<u8>` of length at most
                    // `u32::MAX` (if successful).
                    edit.comparator_name = Some(Cow::Owned(read_comparator_name(input)?));
                }
                VersionEditTag::LogNumber => {
                    edit.log_number = Some(read_file_number(input)?);
                }
                VersionEditTag::NextFileNumber => {
                    edit.next_file_number = Some(read_file_number(input)?);
                }
                VersionEditTag::LastSequence => {
                    edit.last_sequence = Some(read_sequence_number(input)?);
                }
                VersionEditTag::CompactPointer => {
                    let level = read_level(input)?;
                    let key = read_internal_key(
                        input,
                        VersionEditKeyType::CompactionPointer,
                        &mut validate_user_key,
                    )?;
                    edit.compaction_pointers.push((level, CompactionPointer::new(key)));
                }
                VersionEditTag::DeletedFile => {
                    let level = read_level(input)?;
                    let file_number = read_file_number(input)?;
                    edit.deleted_files.insert((level, file_number));
                }
                VersionEditTag::NewFile => {
                    let level = read_level(input)?;
                    let file_number = read_file_number(input)?;
                    let file_size = read_file_size(input)?;
                    let smallest_key = read_internal_key(
                        input,
                        VersionEditKeyType::SmallestFileKey(file_number),
                        &mut validate_user_key,
                    )?;
                    let largest_key = read_internal_key(
                        input,
                        VersionEditKeyType::LargestFileKey(file_number),
                        &mut validate_user_key,
                    )?;

                    let metadata = Arc::new(FileMetadata::new(
                        file_number,
                        file_size,
                        smallest_key,
                        largest_key,
                        opts,
                    ));

                    edit.added_files.push((level, metadata));
                }
                VersionEditTag::PrevLogNumber => {
                    edit.prev_log_number = Some(read_file_number(input)?);
                }
            }
        }

        Ok(edit)
    }

    pub fn encode(&self, output: &mut Vec<u8>) {
        if let Some(comparator_name) = &self.comparator_name {
            write_tag(output, VersionEditTag::Comparator);
            // Should not panic, as the `self.comparator_name` field documents that it should have
            // length at most `u32::MAX`.
            #[expect(clippy::expect_used, reason = "could only panic due to a bug")]
            let comparator_name = ShortSlice::new(comparator_name)
                .expect("`VersionEdit.comparator_name`'s length must not exceed `u32::MAX`");
            write_comparator_name(output, comparator_name);
        }
        if let Some(log_number) = self.log_number {
            write_tag(output, VersionEditTag::LogNumber);
            write_file_number(output, log_number);
        }
        if let Some(prev_log_number) = self.prev_log_number {
            write_tag(output, VersionEditTag::PrevLogNumber);
            write_file_number(output, prev_log_number);
        }
        if let Some(next_file_number) = self.next_file_number {
            write_tag(output, VersionEditTag::NextFileNumber);
            write_file_number(output, next_file_number);
        }
        if let Some(last_sequence) = self.last_sequence {
            write_tag(output, VersionEditTag::LastSequence);
            write_sequence_number(output, last_sequence);
        }
        for compaction_pointer in &self.compaction_pointers {
            write_tag(output, VersionEditTag::CompactPointer);
            write_level(output, compaction_pointer.0);
            write_internal_key(output, compaction_pointer.1.internal_key());
        }
        for deleted_file in &self.deleted_files {
            write_tag(output, VersionEditTag::DeletedFile);
            write_level(output, deleted_file.0);
            write_file_number(output, deleted_file.1);
        }
        for (new_file_level, new_file_meta) in &self.added_files {
            write_tag(output, VersionEditTag::NewFile);
            write_level(output, *new_file_level);
            write_file_number(output, new_file_meta.file_number());
            write_file_size(output, new_file_meta.file_size());
            write_internal_key(output, new_file_meta.smallest_key());
            write_internal_key(output, new_file_meta.largest_key());
        }
    }
}

/// The returned value, if `Ok(_)`, has length at most `u32::MAX`.
fn read_comparator_name<E>(input: &mut &[u8]) -> Result<Vec<u8>, VersionEditDecodeError<E>> {
    let comparator_name = input.read_prefixed_bytes()?.unprefixed_inner();
    Ok(comparator_name.inner().to_owned())
}

fn write_comparator_name(output: &mut Vec<u8>, name: ShortSlice<'_>) {
    output.write_varint32(u32::from(name.len()));
    output.extend(name.inner());
}

fn read_internal_key<'a, V, E>(
    input:             &mut &'a [u8],
    key_type:          VersionEditKeyType,
    validate_user_key: V,
) -> Result<InternalKey<'a>, VersionEditDecodeError<E>>
where
    V: FnOnce(UserKey<'_>) -> Result<(), E>,
{
    let encoded_internal_key = input.read_prefixed_bytes()?.unprefixed_inner();
    let encoded_internal_key = EncodedInternalKey::validate(
        UnvalidatedInternalKey(encoded_internal_key.inner()),
        validate_user_key,
    ).map_err(|err| VersionEditDecodeError::InvalidInternalKey(key_type, err))?;
    Ok(encoded_internal_key.as_internal_key())
}

fn write_internal_key(output: &mut Vec<u8>, key: InternalKey<'_>) {
    // Since `UserKey::len(&key.0) <= u32::MAX - 8`, this sum does not overflow.
    output.write_varint32(u32::from(key.0.len()) + 8);
    key.append_encoded(output);
}

fn read_file_number<E>(input: &mut &[u8]) -> Result<FileNumber, VersionEditDecodeError<E>> {
    let file_number = input.read_varint64()?.0;
    Ok(FileNumber(file_number))
}

fn write_file_number(output: &mut Vec<u8>, file_number: FileNumber) {
    output.write_varint64(file_number.0);
}

fn read_file_size<E>(input: &mut &[u8]) -> Result<FileSize, VersionEditDecodeError<E>> {
    let file_size = input.read_varint64()?.0;
    Ok(FileSize(file_size))
}

fn write_file_size(output: &mut Vec<u8>, file_size: FileSize) {
    output.write_varint64(file_size.0);
}

fn read_sequence_number<E>(input: &mut &[u8]) -> Result<SequenceNumber, VersionEditDecodeError<E>> {
    let sequence = input.read_varint64()?.0;
    SequenceNumber::new_usable(sequence)
        .ok_or(VersionEditDecodeError::LastSequenceNumberTooLarge)
}

fn write_sequence_number(output: &mut Vec<u8>, sequence: SequenceNumber) {
    output.write_varint64(sequence.inner());
}

fn read_level<E>(input: &mut &[u8]) -> Result<Level, VersionEditDecodeError<E>> {
    let level = input.read_varint32()?.0;
    Level::from_u32(level)
        .ok_or(VersionEditDecodeError::LevelTooLarge(level))
}

fn write_level(output: &mut Vec<u8>, level: Level) {
    output.write_varint32(u32::from(level.inner()));
}

fn read_tag<E>(
    input: &mut &[u8],
) -> Result<VersionEditTag, VersionEditDecodeError<E>> {
    let tag = input.read_varint32()?.0;
    VersionEditTag::try_from(tag)
        .map_err(|()| VersionEditDecodeError::UnknownVersionEditTag(tag))
}

fn write_tag(output: &mut Vec<u8>, tag: VersionEditTag) {
    output.write_varint32(u32::from(tag));
}
