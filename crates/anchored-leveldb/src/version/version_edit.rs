use std::collections::BTreeSet;

use integer_encoding::{VarInt as _, VarIntWriter as _};

use crate::public_format::LengthPrefixedBytes;
use crate::format::{
    EncodedInternalKey, FileNumber, InternalKey, Level, SequenceNumber, VersionEditTag,
};
use super::file_metadata::{FileMetadata, SeeksBetweenCompactionOptions};


#[derive(Debug)]
pub(crate) struct VersionEdit<'a> {
    pub comparator_name:  Option<&'a [u8]>,
    pub log_number:       Option<FileNumber>,
    pub prev_log_number:  Option<FileNumber>,
    pub next_file_number: Option<FileNumber>,
    pub last_sequence:    Option<SequenceNumber>,
    pub compact_pointers: Vec<(Level, InternalKey<'a>)>,
    pub deleted_files:    BTreeSet<(Level, FileNumber)>,
    pub new_files:        Vec<(Level, FileMetadata)>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> VersionEdit<'a> {
    #[inline]
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            comparator_name:  None,
            log_number:       None,
            prev_log_number:  None,
            next_file_number: None,
            last_sequence:    None,
            compact_pointers: Vec::new(),
            deleted_files:    BTreeSet::new(),
            new_files:        Vec::new(),
        }
    }

    pub fn decode_from(
        mut input: &'a [u8],
        opts:      SeeksBetweenCompactionOptions,
    ) -> Result<Self, ()> {
        let mut edit = Self::new_empty();

        while !input.is_empty() {
            let tag = read_tag(&mut input)?;

            match tag {
                VersionEditTag::Comparator => {
                    edit.comparator_name = Some(read_byte_slice(&mut input)?);
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
                    edit.compact_pointers.push((level, key));
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

                    let metadata = FileMetadata::new(
                        file_number,
                        file_size,
                        smallest_key,
                        largest_key,
                        opts,
                    );

                    edit.new_files.push((level, metadata));
                }
                VersionEditTag::PrevLogNumber => {
                    edit.prev_log_number = Some(read_file_number(&mut input)?);
                }
            }
        }

        Ok(edit)
    }

    pub fn encode(&self, output: &mut Vec<u8>) {
        if let Some(comparator_name) = self.comparator_name {
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
        for compact_pointer in &self.compact_pointers {
            write_tag(output, VersionEditTag::CompactPointer);
            write_level(output, compact_pointer.0);
            write_internal_key(output, compact_pointer.1);
        }
        for deleted_file in &self.deleted_files {
            write_tag(output, VersionEditTag::DeletedFile);
            write_level(output, deleted_file.0);
            write_file_number(output, deleted_file.1);
        }
        for (new_file_level, new_file_meta) in &self.new_files {
            write_tag(output, VersionEditTag::NewFile);
            write_level(output, *new_file_level);
            write_file_number(output, new_file_meta.file_number());
            write_varint_u64(output, new_file_meta.file_size());
            write_internal_key(output, new_file_meta.smallest_key());
            write_internal_key(output, new_file_meta.largest_key());
        }
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
        .expect("`LevelDBComparator::name` exceeded 4 gigabytes");
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
    write_varint_u32(output, u32::from(level.0));
}

fn read_tag(input: &mut &[u8]) -> Result<VersionEditTag, ()> {
    VersionEditTag::try_from(read_varint_u32(input)?)
}

fn write_tag(output: &mut Vec<u8>, tag: VersionEditTag) {
    write_varint_u32(output, u32::from(tag));
}
