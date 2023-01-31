extern crate blosc_src;

/*
Portions of the blosc decompression code originate from blosc-rs
(github.com/asomers/blosc-rs).  The code is inlined here in order
to use the source bindings from blosc-src in order to avoid
linkages issues in upstream/dependent modules and crates.

The license for blosc-rs is as follows:

Copyright (c) 2018 Alan Somers

Permission is hereby granted, free of charge, to any
person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the
Software without restriction, including without
limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software
is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice
shall be included in all copies or substantial portions
of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
*/

use std::io::{Cursor, Read, Write};
use std::{error, fmt, mem, os::raw::c_void, ptr};

use serde::{Deserialize, Serialize};

use blosc_src::*;

use super::Compression;

const COMPRESSOR_BLOSCLZ: &str = "blosclz";
const COMPRESSOR_LZ4: &str = "lz4";
const COMPRESSOR_ZLIB: &str = "zlib";
const COMPRESSOR_ZSTD: &str = "zstd";

/// An unspecified error from C-Blosc
/// Same BloscError as github.com/asomers/blosc-rs (blosc v0.1.3)
#[derive(Clone, Copy, Debug)]
pub struct BloscError;

impl fmt::Display for BloscError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unspecified error from c-Blosc")
    }
}

impl error::Error for BloscError {}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "lowercase")]
pub struct BloscCompression {
    #[serde(default = "default_blosc_blocksize")]
    blocksize: usize,
    #[serde(default = "default_blosc_clevel")]
    clevel: u8,
    cname: String,
    #[serde(default = "default_blosc_shufflemode")]
    shuffle: u8, // serialize shuffle mode into enum by index
}

fn default_blosc_blocksize() -> usize {
    0
}

fn default_blosc_clevel() -> u8 {
    5
}

fn default_blosc_shufflemode() -> u8 {
    1
}

impl Default for BloscCompression {
    fn default() -> BloscCompression {
        BloscCompression {
            blocksize: default_blosc_blocksize(),
            clevel: 5,
            cname: String::from(COMPRESSOR_BLOSCLZ),
            shuffle: default_blosc_shufflemode(),
        }
    }
}

impl BloscCompression {
    fn decompress<T>(src: &[u8]) -> Result<Vec<T>, BloscError> {
        unsafe { BloscCompression::decompress_bytes(src) }
    }

    // Adapted from https://github.com/asomers/blosc-rs
    //
    // same as decompress_bytes from blosc-0.1.3, but use the
    // blosc-src direct lib to allow easier builds without
    // linkage
    unsafe fn decompress_bytes<T>(src: &[u8]) -> Result<Vec<T>, BloscError> {
        let typesize = mem::size_of::<T>();
        let mut nbytes: usize = 0;
        let mut _cbytes: usize = 0;
        let mut _blocksize: usize = 0;

        // unsafe
        blosc_cbuffer_sizes(
            src.as_ptr() as *const c_void,
            &mut nbytes as *mut usize,
            &mut _cbytes as *mut usize,
            &mut _blocksize as *mut usize,
        );
        let dest_size = nbytes / typesize;
        let mut dest: Vec<T> = Vec::with_capacity(dest_size);

        // unsafe
        let rsize = blosc_decompress_ctx(
            src.as_ptr() as *const c_void,
            dest.as_mut_ptr() as *mut c_void,
            nbytes,
            1,
        );
        if rsize > 0 {
            // unsafe
            dest.set_len(rsize as usize / typesize);
            dest.shrink_to_fit();
            Ok(dest)
        } else {
            Err(BloscError)
        }
    }
}

impl Compression for BloscCompression {
    fn decoder<'a, R: Read + 'a>(&self, mut r: R) -> Box<dyn Read + 'a> {
        // blosc is all at the same time...
        let mut bytes: Vec<u8> = Vec::new();
        r.read_to_end(&mut bytes);
        let decompressed = BloscCompression::decompress(&bytes).unwrap();
        Box::new(Cursor::new(decompressed))
    }

    // TODO not currently supported
    fn encoder<'a, W: Write + 'a>(&self, w: W) -> Box<dyn Write + 'a> {
        // TODO: need wrapper that only does the compression when
        // the end of the data/EOF is reached.
        Box::new(w)
        // TODO adapt members to compress() method, write compress method
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionType;

    #[rustfmt::skip]
  const TEST_CHUNK_I16_BLOSC: [u8; 28] = [
      0x02, 0x01, 0x33, 0x02,
      0x0c, 0x00, 0x00, 0x00,
      0x0c, 0x00, 0x00, 0x00,
      0x1c, 0x00, 0x00, 0x00,
      0x00, 0x01, 0x00, 0x02, // target payload is big endian
      0x00, 0x03, 0x00, 0x04,
      0x00, 0x05, 0x00, 0x06, // not very compressed now is it
  ];

    #[test]
    fn test_read_doc_spec_chunk() {
        let blosc_lz4: BloscCompression = BloscCompression {
            blocksize: 0,
            clevel: 5,
            cname: COMPRESSOR_LZ4.to_string(),
            shuffle: 1,
        };
        crate::tests::test_read_doc_spec_chunk(
            TEST_CHUNK_I16_BLOSC.as_ref(),
            CompressionType::Blosc(blosc_lz4),
        );
    }
}
