use super::prefixed_bytes::PrefixedBytes;


#[derive(Debug, Clone, Copy)]
pub enum WriteEntry<'a> {
    Value {
        key:   PrefixedBytes<'a>,
        value: PrefixedBytes<'a>,
    },
    Deletion {
        key:   PrefixedBytes<'a>,
    },
}
