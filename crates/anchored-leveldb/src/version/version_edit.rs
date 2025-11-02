use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use integer_encoding::{VarInt as _, VarIntWriter as _};
use generic_container::FragileTryContainer as _;

use crate::{
    compaction::CompactionPointer,
    containers::RefcountedFamily,
    public_format::LengthPrefixedBytes,
};
use crate::{
    file_tracking::{FileMetadata, Level, RefcountedFileMetadata, SeeksBetweenCompactionOptions},
    format::{EncodedInternalKey, FileNumber, InternalKey, SequenceNumber, VersionEditTag},
};


pub(crate) struct VersionEdit<Refcounted: RefcountedFamily> {
    pub comparator_name:     Option<Vec<u8>>,
    pub log_number:          Option<FileNumber>,
    pub prev_log_number:     Option<FileNumber>,
    pub next_file_number:    Option<FileNumber>,
    pub last_sequence:       Option<SequenceNumber>,
    pub compaction_pointers: Vec<(Level, CompactionPointer)>,
    pub deleted_files:       BTreeSet<(Level, FileNumber)>,
    pub added_files:         Vec<(Level, RefcountedFileMetadata<Refcounted>)>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> VersionEdit<Refcounted> {
    #[inline]
    #[must_use]
    pub fn new_empty() -> Self {
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

    pub fn decode_from(
        mut input: &[u8],
        opts:      SeeksBetweenCompactionOptions,
    ) -> Result<Self, ()> {
        let mut edit = Self::new_empty();

        while !input.is_empty() {
            let tag = read_tag(&mut input)?;

            match tag {
                VersionEditTag::Comparator => {
                    edit.comparator_name = Some(read_byte_slice(&mut input)?.to_owned());
                }
                VersionEditTag::LogNumber => {
                    edit.log_number = Some(read_file_number(&mut input)?);
                }
                VersionEditTag::NextFileNumber => {
                    edit.next_file_number = Some(read_file_number(&mut input)?);
                }
                VersionEditTag::LastSequence => {
                    edit.last_sequence = Some(read_sequence_number(&mut input)?);
                }
                VersionEditTag::CompactPointer => {
                    let level = read_level(&mut input)?;
                    let key = read_internal_key(&mut input)?;
                    edit.compaction_pointers.push((level, CompactionPointer::new(key)));
                }
                VersionEditTag::DeletedFile => {
                    let level = read_level(&mut input)?;
                    let file_number = read_file_number(&mut input)?;
                    edit.deleted_files.insert((level, file_number));
                }
                VersionEditTag::NewFile => {
                    let level = read_level(&mut input)?;
                    let file_number = read_file_number(&mut input)?;
                    let file_size = read_varint_u64(&mut input)?;
                    let smallest_key = read_internal_key(&mut input)?;
                    let largest_key = read_internal_key(&mut input)?;

                    let metadata = Refcounted::Container::new_container(FileMetadata::new(
                        file_number,
                        file_size,
                        smallest_key,
                        largest_key,
                        opts,
                    ));

                    edit.added_files.push((level, metadata));
                }
                VersionEditTag::PrevLogNumber => {
                    edit.prev_log_number = Some(read_file_number(&mut input)?);
                }
            }
        }

        Ok(edit)
    }

    pub fn encode(&self, output: &mut Vec<u8>) {
        if let Some(comparator_name) = &self.comparator_name {
            write_tag(output, VersionEditTag::Comparator);
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
            write_varint_u64(output, new_file_meta.file_size());
            write_internal_key(output, new_file_meta.smallest_key());
            write_internal_key(output, new_file_meta.largest_key());
        }
    }
}

impl<Refcounted: RefcountedFamily> Debug for VersionEdit<Refcounted> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> FmtResult {
        /// Type solely for debugging `self.added_files`
        struct DebugFiles<'a, Refcounted: RefcountedFamily> {
            added_files: &'a [(Level, RefcountedFileMetadata<Refcounted>)],
        }

        impl<'a, Refcounted: RefcountedFamily> DebugFiles<'a, Refcounted> {
            #[must_use]
            pub fn new(added_files: &'a [(Level, RefcountedFileMetadata<Refcounted>)]) -> Self {
                Self { added_files }
            }
        }

        impl<Refcounted: RefcountedFamily> Debug for DebugFiles<'_, Refcounted> {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                f.debug_list()
                    .entries(self.added_files.iter().map(|(_, file)| Refcounted::debug(file)))
                    .finish()
            }
        }

        f.debug_struct("VersionEdit")
            .field("comparator_name",     &self.comparator_name)
            .field("log_number",          &self.log_number)
            .field("prev_log_number",     &self.prev_log_number)
            .field("next_file_number",    &self.next_file_number)
            .field("last_sequence",       &self.last_sequence)
            .field("compaction_pointers", &self.compaction_pointers)
            .field("deleted_files",       &self.deleted_files)
            .field("added_files",         &DebugFiles::<Refcounted>::new(&*self.added_files))
            .finish()
    }
}

fn read_varint_u32(input: &mut &[u8]) -> Result<u32, ()> {
    let (num, num_len) = u32::decode_var(input).ok_or(())?;
    *input = &input[num_len..];
    Ok(num)
}

fn write_varint_u32(output: &mut Vec<u8>, value: u32){
    output.write_varint(value).expect("writing to a Vec does not fail");
}

fn read_varint_u64(input: &mut &[u8]) -> Result<u64, ()> {
    let (num, num_len) = u64::decode_var(input).ok_or(())?;
    *input = &input[num_len..];
    Ok(num)
}

fn write_varint_u64(output: &mut Vec<u8>, value: u64) {
    output.write_varint(value).expect("writing to a Vec does not fail");
}

fn read_byte_slice<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], ()> {
    let (slice, after_slice) = LengthPrefixedBytes::parse(input)?;
    *input = after_slice;
    Ok(slice.data())
}

/// # Panics
/// Panics if `name.len()` exceeds [`u32::MAX`].
///
/// For [`LevelDBComparator::name`], this function is guaranteed by the comparator implementor
/// to not panic.
///
/// [`LevelDBComparator::name`]: crate::table_traits::trait_equivalents::LevelDBComparator::name
fn write_comparator_name(output: &mut Vec<u8>, name: &[u8]) {
    let name_len = u32::try_from(name.len())
        .expect("`LevelDBComparator::name` exceeded 4 GiB");
    output.write_varint(name_len).expect("writing to a Vec does not fail");
    output.extend(name);
}

fn read_internal_key<'a>(input: &mut &'a [u8]) -> Result<InternalKey<'a>, ()> {
    let encoded_internal_key = read_byte_slice(input)?;
    InternalKey::decode(EncodedInternalKey(encoded_internal_key))
}

fn write_internal_key(output: &mut Vec<u8>, key: InternalKey<'_>) {
    output.write_varint(key.encoded_len()).expect("writing to a Vec does not fail");
    key.append_encoded(output);
}

fn read_file_number(input: &mut &[u8]) -> Result<FileNumber, ()> {
    read_varint_u64(input).map(FileNumber)
}

fn write_file_number(output: &mut Vec<u8>, file_number: FileNumber) {
    write_varint_u64(output, file_number.0);
}

fn read_sequence_number(input: &mut &[u8]) -> Result<SequenceNumber, ()> {
    let sequence = read_varint_u64(input)?;
    SequenceNumber::new_usable(sequence).ok_or(())
}

fn write_sequence_number(output: &mut Vec<u8>, sequence: SequenceNumber) {
    write_varint_u64(output, sequence.inner());
}

fn read_level(input: &mut &[u8]) -> Result<Level, ()> {
    let level = read_varint_u32(input)?;
    Level::try_from(level)
}

fn write_level(output: &mut Vec<u8>, level: Level) {
    write_varint_u32(output, u32::from(level.inner()));
}

fn read_tag(input: &mut &[u8]) -> Result<VersionEditTag, ()> {
    VersionEditTag::try_from(read_varint_u32(input)?)
}

fn write_tag(output: &mut Vec<u8>, tag: VersionEditTag) {
    write_varint_u32(output, u32::from(tag));
}
