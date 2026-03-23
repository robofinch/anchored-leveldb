use anchored_vfs::LevelDBFilesystem;

use crate::pub_traits::{cmp_and_policy::LevelDBComparator, compression::CompressionCodecs};
use super::types;


pub(crate) type RecoveryErrorAlias<FS, Cmp, Codecs> = types::RecoveryError<
    <FS as LevelDBFilesystem>::Error,
    <Cmp as LevelDBComparator>::InvalidKeyError,
    <Codecs as CompressionCodecs>::CompressionError,
    <Codecs as CompressionCodecs>::DecompressionError,
>;

pub(crate) type RecoveryErrorKindAlias<FS, Cmp, Codecs> = types::RecoveryErrorKind<
    <FS as LevelDBFilesystem>::Error,
    <Cmp as LevelDBComparator>::InvalidKeyError,
    <Codecs as CompressionCodecs>::CompressionError,
    <Codecs as CompressionCodecs>::DecompressionError,
>;

pub(crate) type RwErrorKindAlias<FS, Cmp, Codecs> = types::RwErrorKind<
    <FS as LevelDBFilesystem>::Error,
    <Cmp as LevelDBComparator>::InvalidKeyError,
    <Codecs as CompressionCodecs>::CompressionError,
    <Codecs as CompressionCodecs>::DecompressionError,
>;

pub(crate) type WriteErrorAlias<FS, Cmp, Codecs> = types::WriteError<
    <FS as LevelDBFilesystem>::Error,
    <Cmp as LevelDBComparator>::InvalidKeyError,
    <Codecs as CompressionCodecs>::CompressionError,
    <Codecs as CompressionCodecs>::DecompressionError,
>;
