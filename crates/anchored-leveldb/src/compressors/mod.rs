mod compressor_list;


pub use self::compressor_list::CompressorList;


use std::fmt::Debug;

use dyn_clone::DynClone;


pub trait Compressor: Debug + DynClone {
    fn encode_into(&self, source: &[u8], output_buf: &mut Vec<u8>) -> Result<(), CompressionError>;
    fn decode_into(&self, source: &[u8], output_buf: &mut Vec<u8>) -> Result<(), CompressionError>;
}

dyn_clone::clone_trait_object!(Compressor);

pub trait CompressorId {
    const ID: u8;
}


// todo: actually implement Debug and Error properly.
#[derive(Debug, Clone)]
pub struct CompressionError {
    pub error_msg: String,
}
