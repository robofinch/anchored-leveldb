/// The maximum number of levels in the LevelDB database.
pub const NUM_LEVELS: u8 = 7;

/// Once there are [`L0_COMPACTION_TRIGGER`]-many level 0 files, compaction begins.
pub const L0_COMPACTION_TRIGGER: u8 = 4;
/// Once there are [`L0_SOFT_FILE_LIMIT`]-many level 0 files, writes are slowed down
/// in order to let compaction catch up.
pub const L0_SOFT_FILE_LIMIT: u8 = 8;
/// Once there are [`L0_HARD_FILE_LIMIT`]-many level 0 files, writes are entirely stopped
/// in order to let compaction catch up.
pub const L0_HARD_FILE_LIMIT: u8 = 12;

pub const MAX_LEVEL_FOR_COMPACTION: u8 = 2;

pub const READ_SAMPLE_PERIOD: u32 = 2 << 20;


#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct UserKey<'a>(pub &'a [u8]);

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct EncodedInternalKey<'a>(pub &'a [u8]);

impl<'a> EncodedInternalKey<'a> {
    pub fn user_key(self) -> Result<UserKey<'a>, ()> {
        let user_key_len = self.0.len()
            .checked_sub(8)
            .ok_or(())?;

        Ok(UserKey(&self.0[..user_key_len]))
    }

    fn split(self) -> Result<(UserKey<'a>, u64), ()> {
        let user_key_len = self.0.len()
            .checked_sub(8)
            .ok_or(())?;

        let (user_key, last_eight_bytes) = self.0.split_at(user_key_len);
        let last_eight_bytes: [u8; 8] = last_eight_bytes.try_into().unwrap();

        Ok((
            UserKey(user_key),
            u64::from_le_bytes(last_eight_bytes),
        ))
    }
}

#[inline]
#[must_use]
pub fn sequence_and_type_tag(sequence_number: SequenceNumber, value_type: ValueType) -> u64 {
    (sequence_number.0 << 8) | u64::from(u8::from(value_type))
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct SequenceNumber(pub u64);

impl SequenceNumber {
    pub const MAX_SEQUENCE_NUMBER: Self = Self((1 << 56) - 1);
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum ValueType {
    Deletion = 0,
    Value    = 1,
}

impl ValueType {
    pub const MIN_TYPE: Self = Self::Deletion;
    pub const MAX_TYPE: Self = Self::Value;
}

impl From<ValueType> for u8 {
    #[inline]
    fn from(value: ValueType) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for ValueType {
    type Error = ();

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Deletion),
            1 => Ok(Self::Value),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InternalKey<'a> {
    pub user_key:        UserKey<'a>,
    pub sequence_number: SequenceNumber,
    pub value_type:      ValueType,
}

impl<'a> InternalKey<'a> {
    pub fn decode(key: EncodedInternalKey<'a>) -> Result<Self, ()> {
        let (user_key, tag) = key.split()?;

        let sequence_number = SequenceNumber(tag >> 8);
        let value_type      = ValueType::try_from(tag as u8)?;

        Ok(Self {
            user_key,
            sequence_number,
            value_type,
        })
    }

    #[inline]
    #[must_use]
    pub fn tag(&self) -> u64 {
        sequence_and_type_tag(self.sequence_number, self.value_type)
    }

    #[inline]
    pub fn append_encoded(&self, output: &mut Vec<u8>) {
        output.extend(self.user_key.0);
        output.extend(self.tag().to_le_bytes());
    }
}
