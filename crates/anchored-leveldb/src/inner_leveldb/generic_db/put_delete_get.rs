use crate::leveldb_generics::LevelDBGenerics;
use super::super::write_impl::DBWriteImpl;
use super::InnerGenericDB;


// #[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    // put
    // put_with
    // delete
    // delete_with
    // write
    // write_with
    // flush
    // get
    // get_with
}
