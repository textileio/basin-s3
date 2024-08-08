use crate::error::*;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use s3s::StdError;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

pub async fn copy_bytes<S, W>(mut stream: S, writer: &mut W) -> Result<u64>
where
    S: Stream<Item = Result<Bytes, StdError>> + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut nwritten: u64 = 0;
    while let Some(result) = stream.next().await {
        let bytes = match result {
            Ok(x) => x,
            Err(e) => return Err(Error::new(e)),
        };
        writer.write_all(&bytes).await?;
        nwritten += bytes.len() as u64;
    }
    writer.flush().await?;
    Ok(nwritten)
}

pub fn hex(input: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(input.as_ref(), hex_simd::AsciiCase::Lower)
}
