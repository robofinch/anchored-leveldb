use anchored_leveldb::read_test::UnsyncDB;

fn main() {
    UnsyncDB::open_and_crc32c_with_mcbe_compressors("../put-mc-world-db-here/db".into());
}
