use crate::table_format::{InternalComparator, InternalFilterPolicy};


pub(crate) struct InternalOptions<Cmp, Policy, Codecs, Pool> {
    pub cmp:         InternalComparator<Cmp>,
    pub policy:      Option<InternalFilterPolicy<Policy>>,
    pub codecs:      Codecs,
    pub buffer_pool: Pool,
}

pub(crate) struct InternalOptionsPerRead {
    pub verify_checksums: bool,
}

pub(crate) struct InternalOptionsPerWrite {
    pub verify_checksums: bool,
}
