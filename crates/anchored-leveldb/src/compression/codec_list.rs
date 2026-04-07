/// Define a [`CompressionCodecs`] implementation (and corresponding encoder, decoder, and error
/// types) given a list of [`CompressionCodec`] implementations.
///
/// Up to 32 compression codecs are supported.
///
/// The list can be empty for a trivial implementation that always reports that a given compressor
/// ID is unsupported.
///
/// The individual codecs can be given custom [`CompressorId`]s (which are given to this macro
/// as `u8` literals). The IDs default to `1, 2, 3, ...`.
///
/// The syntax of this macro is as follows:
///
/// ```ignore
/// codec_list! {
///     // This line is optional. The generic parameters and bounds apply to *all* generated items,
///     // including type definitions.
///     impl {generic_params} where {where_bounds};
///
///     // The custom IDs (if given) should be nonzero numeric literals small enough to fit in a
///     // `u8`. They are **not** confirmed by the macro to be nonzero or distinct; that burden
///     // is on you.
///     //
///     // The codec parameters should be types that implement `CompressionCodec`.
///     // EITHER:
///     codecs[(CodecVariantName1, CodecWithId1), (CodecVariantName2, CodecWithId2)];
///     // OR:
///     codecs[
///         (first_id, CodecVariantName1, CodecWithFirstId),
///         (second_id, CodecVariantName1, CodecWithSecondId),
///     ];
///
///     /// This doc comment is optional. You can also give it any visibility qualifier you wish.
///     /// The `struct`, though, is not optional.
///     // The struct's fields are always made private. A leading `()` field is included in each
///     // of the three structs, to ensure that their constructor remains private.
///     pub struct CodecsStructName;
///     /// Other meta attributes also work.
///     #[doc(hidden)]
///     pub struct EncodersStructName;
///     pub struct DecodersStructName;
///     // This must be an `enum`. `CodecVariantName1` and `CodecVariantName2` would be the
///     // the names of the variants. Note that the variants are public.
///     pub enum CompressionErrorEnumName;
///     pub enum DecompressionErrorEnumName;
/// }
/// ```
///
/// Note that bounds on generics must be placed inside `where {}`, never in `impl {}`. For instance,
/// use `impl {T} where {T: CompressionCodec};` rather than `impl {T: CompressionCodec} where {};`.
///
/// [`CompressionCodecs`]: crate::pub_traits::compression::CompressionCodecs
/// [`CompressorId`]: crate::pub_traits::compression::CompressorId
/// [`CompressionCodec`]: super::codec_trait::CompressionCodec
#[macro_export]
macro_rules! codec_list {
    {
        $(impl {$($generics:tt)*} where {$($where_bounds:tt)*};)?
        codecs[];

        $(#[$codecs_meta:meta])*
        $codecs_vis:vis struct $codecs:ident;
        $(#[$encoders_meta:meta])*
        $encoders_vis:vis struct $encoders:ident;
        $(#[$decoders_meta:meta])*
        $decoders_vis:vis struct $decoders:ident;
        $(#[$cerr_meta:meta])*
        $cerr_vis:vis enum $cerr:ident;
        $(#[$derr_meta:meta])*
        $derr_vis:vis enum $derr:ident;
    } => {
        $crate::codec_list_impl! {
            __index_target;
            [];
            {$($($generics)*)?};
            {$($($where_bounds)*)?};

            $(#[$codecs_meta])*
            $codecs_vis struct $codecs;
            $(#[$encoders_meta])*
            $encoders_vis struct $encoders;
            $(#[$decoders_meta])*
            $decoders_vis struct $decoders;
            $(#[$cerr_meta])*
            $cerr_vis enum $cerr;
            $(#[$derr_meta])*
            $derr_vis enum $derr;
        }
    };

    {
        $(impl{$($generics:tt)*} where {$($where_bounds:tt)*};)?
        codecs[$(($codec_name:ident, $codec_ty:ty $(,)?)),+$(,)?];

        $(#[$codecs_meta:meta])*
        $codecs_vis:vis struct $codecs:ident;
        $(#[$encoders_meta:meta])*
        $encoders_vis:vis struct $encoders:ident;
        $(#[$decoders_meta:meta])*
        $decoders_vis:vis struct $decoders:ident;
        $(#[$cerr_meta:meta])*
        $cerr_vis:vis enum $cerr:ident;
        $(#[$derr_meta:meta])*
        $derr_vis:vis enum $derr:ident;
    } => {
        $crate::default_id_list_impl! {
            __index_target,
            () () ($(($codec_name, $codec_ty),)+)
            {
                {$($($generics)*)?};
                {$($($where_bounds)*)?};
                $(#[$codecs_meta])*
                $codecs_vis struct $codecs;
                $(#[$encoders_meta])*
                $encoders_vis struct $encoders;
                $(#[$decoders_meta])*
                $decoders_vis struct $decoders;
                $(#[$cerr_meta])*
                $cerr_vis enum $cerr;
                $(#[$derr_meta])*
                $derr_vis enum $derr;
            }
        }
    };

    {
        $(impl{$($generics:tt)*} where {$($where_bounds:tt)*};)?
        codecs[$(($codec_id:literal, $codec_name:ident, $codec_ty:ty $(,)?)),+$(,)?];

        $(#[$codecs_meta:meta])*
        $codecs_vis:vis struct $codecs:ident;
        $(#[$encoders_meta:meta])*
        $encoders_vis:vis struct $encoders:ident;
        $(#[$decoders_meta:meta])*
        $decoders_vis:vis struct $decoders:ident;
        $(#[$cerr_meta:meta])*
        $cerr_vis:vis enum $cerr:ident;
        $(#[$derr_meta:meta])*
        $derr_vis:vis enum $derr:ident;
    } => {
        $crate::custom_id_list_impl! {
            __index_target,
            () () ($(($codec_id, $codec_name, $codec_ty),)+)
            {
                {$($($generics)*)?};
                {$($($where_bounds)*)?};

                $(#[$codecs_meta])*
                $codecs_vis struct $codecs;
                $(#[$encoders_meta])*
                $encoders_vis struct $encoders;
                $(#[$decoders_meta])*
                $decoders_vis struct $decoders;
                $(#[$cerr_meta])*
                $cerr_vis enum $cerr;
                $(#[$derr_meta])*
                $derr_vis enum $derr;
            }
        }
    };
}

/// Transforms `codecs[(name1, ty1), (name1, ty1), ..]` into
/// `codecs[($__index.0, 1, name1, ty1), ($__index.1, 2, name2, ty2), ..]`.
#[macro_export]
#[doc(hidden)]
macro_rules! default_id_list_impl {
    {
        $__index:ident,
        ($($next_id:ident)*) ($($acc:tt)*) (($car_name:ident, $car_ty:ty), $($cdr:tt,)*)
        {
            {$($generics:tt)*};
            {$($where_bounds:tt)*};

            $(#[$codecs_meta:meta])*
            $codecs_vis:vis struct $codecs:ident;
            $(#[$encoders_meta:meta])*
            $encoders_vis:vis struct $encoders:ident;
            $(#[$decoders_meta:meta])*
            $decoders_vis:vis struct $decoders:ident;
            $(#[$cerr_meta:meta])*
            $cerr_vis:vis enum $cerr:ident;
            $(#[$derr_meta:meta])*
            $derr_vis:vis enum $derr:ident;
        }
    } => {
        $crate::default_id_list_impl! {
            $__index,
            (S$($next_id)*)
            (
                $($acc)*
                (
                    $crate::unary_to_indexed!($__index, S$($next_id)*),
                    $crate::unary_to_literal!(S$($next_id)*),
                    $car_name,
                    $car_ty
                ),
            )
            ($($cdr,)*)
            {
                {$($generics)*};
                {$($where_bounds)*};

                $(#[$codecs_meta])*
                $codecs_vis struct $codecs;
                $(#[$encoders_meta])*
                $encoders_vis struct $encoders;
                $(#[$decoders_meta])*
                $decoders_vis struct $decoders;
                $(#[$cerr_meta])*
                $cerr_vis enum $cerr;
                $(#[$derr_meta])*
                $derr_vis enum $derr;
            }
        }
    };

    {
        $__index:ident,
        ($($next_id:ident)*) ($($acc:tt)*) ()
        {
            {$($generics:tt)*};
            {$($where_bounds:tt)*};

            $(#[$codecs_meta:meta])*
            $codecs_vis:vis struct $codecs:ident;
            $(#[$encoders_meta:meta])*
            $encoders_vis:vis struct $encoders:ident;
            $(#[$decoders_meta:meta])*
            $decoders_vis:vis struct $decoders:ident;
            $(#[$cerr_meta:meta])*
            $cerr_vis:vis enum $cerr:ident;
            $(#[$derr_meta:meta])*
            $derr_vis:vis enum $derr:ident;
        }
    } => {
        $crate::codec_list_impl! {
            $__index;
            [$($acc)*];
            {$($generics)*};
            {$($where_bounds)*};

            $(#[$codecs_meta])*
            $codecs_vis struct $codecs;
            $(#[$encoders_meta])*
            $encoders_vis struct $encoders;
            $(#[$decoders_meta])*
            $decoders_vis struct $decoders;
            $(#[$cerr_meta])*
            $cerr_vis enum $cerr;
            $(#[$derr_meta])*
            $derr_vis enum $derr;
        }
    };
}

/// Transforms `codecs[(id1, name1, ty1), (id2, name1, ty1), ..]` into
/// `codecs[($__index.0, id1, name1, ty1), ($__index.1, id2, name2, ty2), ..]`.
#[macro_export]
#[doc(hidden)]
macro_rules! custom_id_list_impl {
    {
        $__index:ident,
        ($($next_id:ident)*) ($($acc:tt)*) (($car_id:literal, $car_name:ident, $car_ty:ty), $($cdr:tt,)*)
        {
            {$($generics:tt)*};
            {$($where_bounds:tt)*};

            $(#[$codecs_meta:meta])*
            $codecs_vis:vis struct $codecs:ident;
            $(#[$encoders_meta:meta])*
            $encoders_vis:vis struct $encoders:ident;
            $(#[$decoders_meta:meta])*
            $decoders_vis:vis struct $decoders:ident;
            $(#[$cerr_meta:meta])*
            $cerr_vis:vis enum $cerr:ident;
            $(#[$derr_meta:meta])*
            $derr_vis:vis enum $derr:ident;
        }
    } => {
        $crate::custom_id_list_impl! {
            $__index,
            (S$($next_id)*)
            (
                $($acc)*
                (
                    $crate::unary_to_indexed!($__index, S$($next_id)*),
                    $car_id,
                    $car_name,
                    $car_ty
                ),
            )
            ($($cdr,)*)
            {
                {$($generics)*};
                {$($where_bounds)*};

                $(#[$codecs_meta])*
                $codecs_vis struct $codecs;
                $(#[$encoders_meta])*
                $encoders_vis struct $encoders;
                $(#[$decoders_meta])*
                $decoders_vis struct $decoders;
                $(#[$cerr_meta])*
                $cerr_vis enum $cerr;
                $(#[$derr_meta])*
                $derr_vis enum $derr;
            }
        }
    };

    {
        $__index:ident,
        ($($next_id:ident)*) ($($acc:tt)*) ()
        {
            {$($generics:tt)*};
            {$($where_bounds:tt)*};

            $(#[$codecs_meta:meta])*
            $codecs_vis:vis struct $codecs:ident;
            $(#[$encoders_meta:meta])*
            $encoders_vis:vis struct $encoders:ident;
            $(#[$decoders_meta:meta])*
            $decoders_vis:vis struct $decoders:ident;
            $(#[$cerr_meta:meta])*
            $cerr_vis:vis enum $cerr:ident;
            $(#[$derr_meta:meta])*
            $derr_vis:vis enum $derr:ident;
        }
    } => {
        $crate::codec_list_impl! {
            $__index;
            [$($acc)*];
            {$($generics)*};
            {$($where_bounds)*};

            $(#[$codecs_meta])*
            $codecs_vis struct $codecs;
            $(#[$encoders_meta])*
            $encoders_vis struct $encoders;
            $(#[$decoders_meta])*
            $decoders_vis struct $decoders;
            $(#[$cerr_meta])*
            $cerr_vis enum $cerr;
            $(#[$derr_meta])*
            $derr_vis enum $derr;
        }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! unary_to_literal {
    () => { 0 };
    (S) => { 1 };
    (S S) => { 2 };
    (S S S) => { 3 };
    (S S S S) => { 4 };
    (S S S S S) => { 5 };
    (S S S S S S) => { 6 };
    (S S S S S S S) => { 7 };
    (S S S S S S S S) => { 8 };
    (S S S S S S S S S) => { 9 };
    (S S S S S S S S S S) => { 10 };
    (S S S S S S S S S S S) => { 11 };
    (S S S S S S S S S S S S) => { 12 };
    (S S S S S S S S S S S S S) => { 13 };
    (S S S S S S S S S S S S S S) => { 14 };
    (S S S S S S S S S S S S S S S) => { 15 };
    (S S S S S S S S S S S S S S S S) => { 16 };
    (S S S S S S S S S S S S S S S S S) => { 17 };
    (S S S S S S S S S S S S S S S S S S) => { 18 };
    (S S S S S S S S S S S S S S S S S S S) => { 19 };
    (S S S S S S S S S S S S S S S S S S S S) => { 20 };
    (S S S S S S S S S S S S S S S S S S S S S) => { 21 };
    (S S S S S S S S S S S S S S S S S S S S S S) => { 22 };
    (S S S S S S S S S S S S S S S S S S S S S S S) => { 23 };
    (S S S S S S S S S S S S S S S S S S S S S S S S) => { 24 };
    (S S S S S S S S S S S S S S S S S S S S S S S S S) => { 25 };
    (S S S S S S S S S S S S S S S S S S S S S S S S S S) => { 26 };
    (S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { 27 };
    (S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { 28 };
    (S S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { 29 };
    (S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { 30 };
    (S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { 31 };
    (S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { 32 };
    ($(S)+) => { compile_error!("`codec_list!` only supports up to 32 codecs"); }
}

#[macro_export]
#[doc(hidden)]
macro_rules! unary_to_indexed {
    ($__index:ident, ) => { $__index.0 };
    ($__index:ident, S) => { $__index.1 };
    ($__index:ident, S S) => { $__index.2 };
    ($__index:ident, S S S) => { $__index.3 };
    ($__index:ident, S S S S) => { $__index.4 };
    ($__index:ident, S S S S S) => { $__index.5 };
    ($__index:ident, S S S S S S) => { $__index.6 };
    ($__index:ident, S S S S S S S) => { $__index.7 };
    ($__index:ident, S S S S S S S S) => { $__index.8 };
    ($__index:ident, S S S S S S S S S) => { $__index.9 };
    ($__index:ident, S S S S S S S S S S) => { $__index.10 };
    ($__index:ident, S S S S S S S S S S S) => { $__index.11 };
    ($__index:ident, S S S S S S S S S S S S) => { $__index.12 };
    ($__index:ident, S S S S S S S S S S S S S) => { $__index.13 };
    ($__index:ident, S S S S S S S S S S S S S S) => { $__index.14 };
    ($__index:ident, S S S S S S S S S S S S S S S) => { $__index.15 };
    ($__index:ident, S S S S S S S S S S S S S S S S) => { $__index.16 };
    ($__index:ident, S S S S S S S S S S S S S S S S S) => { $__index.17 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S) => { $__index.18 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S) => { $__index.19 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S) => { $__index.20 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S) => { $__index.21 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S) => { $__index.22 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.23 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.24 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.25 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.26 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.27 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.28 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.29 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.30 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.31 };
    ($__index:ident, S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S S) => { $__index.32 };
    ($__index:ident, $(S)+) => { compile_error!("`codec_list!` only supports up to 32 codecs"); }
}

/// The actual implementation.
#[macro_export]
#[doc(hidden)]
macro_rules! codec_list_impl {
    {
        $__index:ident;
        [$(($indexed:expr, $codec_id:expr, $codec_variant:ident, $codec_ty:ty),)*];
        {$($generics:tt)*};
        {$($where_bounds:tt)*};

        $(#[$codecs_meta:meta])*
        $codecs_vis:vis struct $codecs:ident;
        $(#[$encoders_meta:meta])*
        $encoders_vis:vis struct $encoders:ident;
        $(#[$decoders_meta:meta])*
        $decoders_vis:vis struct $decoders:ident;
        $(#[$cerr_meta:meta])*
        $cerr_vis:vis enum $cerr:ident;
        $(#[$derr_meta:meta])*
        $derr_vis:vis enum $derr:ident;
    } => {

        $(#[$codecs_meta])*
        $codecs_vis struct $codecs<$($generics)*>(
            (),
            $($codec_ty,)*
        )
        where
            $($where_bounds)*;

        $(#[$encoders_meta])*
        $encoders_vis struct $encoders<$($generics)*>(
            (),
            $(<$codec_ty as $crate::db_options::CompressionCodec>::Encoder,)*
        )
        where
            $($where_bounds)*;

        $(#[$decoders_meta])*
        $decoders_vis struct $decoders<$($generics)*>(
            (),
            $(<$codec_ty as $crate::db_options::CompressionCodec>::Decoder,)*
        )
        where
            $($where_bounds)*;

        $(#[$cerr_meta])*
        $cerr_vis enum $cerr<$($generics)*>
        where
            $($where_bounds)*
        {
            $($codec_variant(
                <$codec_ty as $crate::db_options::CompressionCodec>::CompressionError,
            )),*
        }

        $(#[$derr_meta])*
        $derr_vis enum $derr<$($generics)*>
        where
            $($where_bounds)*
        {
            $($codec_variant(
                <$codec_ty as $crate::db_options::CompressionCodec>::DecompressionError,
            )),*
        }

        impl<$($generics)*> $crate::db_options::CompressionCodecs for $codecs<$($generics)*>
        where
            $($where_bounds)*
        {
            type Encoders = $encoders<$($generics)*>;
            type Decoders = $decoders<$($generics)*>;
            type CompressionError = $cerr<$($generics)*>;
            type DecompressionError = $derr<$($generics)*>;

            #[inline]
            fn init_encoders(&self) -> Self::Encoders {
                $encoders(
                    (),
                    $(<$codec_ty as $crate::db_options::CompressionCodec>::init_encoder(
                        {let $__index = self; &$indexed},
                    ),)*
                )
            }

            #[inline]
            fn encode<Pool: $crate::db_options::BufferPool>(
                __encoders:         &mut Self::Encoders,
                __src:              &[::core::primitive::u8],
                __id:               $crate::db_options::CompressorId,
                __compression_goal: ::core::primitive::usize,
                __pool:             &Pool,
                __existing_buf:     &mut Option<<Pool as $crate::db_options::BufferPool>::PooledBuffer>,
            ) -> Result<
                <Pool as $crate::db_options::BufferPool>::PooledBuffer,
                $crate::db_options::CodecsCompressionError<Self::CompressionError>
            > {
                match __id.0.get() {
                    $(__codec_id @ $codec_id => {
                        <$codec_ty as $crate::db_options::CompressionCodec>::encode(
                            {let $__index = __encoders; &mut $indexed},
                            __src,
                            __compression_goal,
                            __pool,
                            __existing_buf,
                        ).map_err(|__error| {
                            $crate::db_options::CodecsCompressionError::from(
                                __error.map_custom($cerr::$codec_variant),
                            )
                        })
                    })*
                    _ => ::std::result::Result::Err(
                        $crate::db_options::CodecsCompressionError::Unsupported,
                    ),
                }
            }

            #[inline]
            fn init_decoders(&self) -> Self::Decoders {
                $decoders(
                    (),
                    $(<$codec_ty as $crate::db_options::CompressionCodec>::init_decoder(
                        {let $__index = self; &$indexed},
                    ),)*
                )
            }

            #[inline]
            fn decode<Pool: $crate::db_options::BufferPool>(
                __decoders:     &mut Self::Decoders,
                __src:          &[::core::primitive::u8],
                __id:           $crate::db_options::CompressorId,
                __pool:         &Pool,
                __existing_buf: &mut Option<<Pool as $crate::db_options::BufferPool>::PooledBuffer>,
            ) -> Result<
                <Pool as $crate::db_options::BufferPool>::PooledBuffer,
                $crate::db_options::CodecsDecompressionError<Self::DecompressionError>
            > {
                match __id.0.get() {
                    $(__codec_id @ $codec_id => {
                        <$codec_ty as $crate::db_options::CompressionCodec>::decode(
                            {let $__index = __decoders; &mut $indexed},
                            __src,
                            __pool,
                            __existing_buf,
                        ).map_err(|__error| {
                            $crate::db_options::CodecsDecompressionError::from(
                                __error.map_custom($derr::$codec_variant),
                            )
                        })
                    })*
                    _ => ::std::result::Result::Err(
                        $crate::db_options::CodecsDecompressionError::Unsupported,
                    ),
                }
            }
        }
    };
}


#[cfg(test)]
#[allow(clippy::absolute_paths, dead_code, unused_qualifications, reason = "compilation test")]
mod test {
    codec_list! {
        impl {} where {};
        codecs[];

        struct EmptyFoo;
        /// Docs
        struct EmptyBar;
        struct EmptyBaz;
        enum EmptyQux;
        enum EmptyQuux;
    }

    mod one_test {
        codec_list! {
            impl {} where {String: Clone};
            codecs[(
                42,
                None,
                <crate::db_options::NoCompressionCodec as std::borrow::ToOwned>::Owned,
            )];

            /// Docs
            struct OneFoo;
            struct OneBar;
            /// Docs
            struct OneBaz;
            enum OneQux;
            enum OneQuux;
        }
    }

    #[cfg(all(
        feature = "snappy-compression",
        feature = "zlib-compression",
        feature = "zstd-compression",
    ))]
    mod all_test {
        use crate::db_options::{NoCompressionCodec, SnappyCodec, ZlibCodec, ZstdCodec};

        codec_list! {
            impl {T} where {T: crate::db_options::CompressionCodec};
            codecs[
                (None, NoCompressionCodec), (Snappy, SnappyCodec),
                (Zlib, ZlibCodec), (Zstd, ZstdCodec),
                (Custom, T),
            ];

            /// Docs
            struct AllFoo;
            struct AllBar;
            /// Docs
            struct AllBaz;
            enum AllQux;
            enum AllQuux;
        }
    }
}
