use std::{pin::Pin, task::Poll};

use async_trait::async_trait;
use tokio::io::{self, AsyncWrite, AsyncWriteExt, BufWriter};

pub struct CountingWriter<W> {
    underlying: W,
    written_bytes: u64,
}

impl<W: AsyncWrite> CountingWriter<W> {
    pub fn wrap(underlying: W) -> CountingWriter<W> {
        CountingWriter {
            underlying,
            written_bytes: 0,
        }
    }

    #[inline]
    pub fn written_bytes(&self) -> u64 {
        self.written_bytes
    }

    /// Returns the underlying write object.
    /// Note that this method does not trigger any flushing.
    #[inline]
    pub fn finish(self) -> W {
        self.underlying
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for CountingWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        let pinned = Pin::new(&mut self.underlying);
        match pinned.poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                self.written_bytes += n as u64;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let pinned = Pin::new(&mut self.get_mut().underlying);
        pinned.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let pinned = Pin::new(&mut self.get_mut().underlying);
        pinned.poll_shutdown(cx)
    }
}

#[async_trait]
impl<W: TerminatingWrite + Unpin> TerminatingWrite for CountingWriter<W> {
    #[inline]
    async fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        self.underlying.terminate_ref(token).await
    }
}

/// Struct used to prevent from calling
/// [`terminate_ref`](TerminatingWrite::terminate_ref) directly
///
/// The point is that while the type is public, it cannot be built by anyone
/// outside of this module.
pub struct AntiCallToken(());

/// Trait used to indicate when no more write need to be done on a writer
#[async_trait]
pub trait TerminatingWrite: AsyncWrite + Send + Sync {
    /// Indicate that the writer will no longer be used. Internally call terminate_ref.
    async fn terminate(mut self) -> io::Result<()>
    where
        Self: Sized,
    {
        self.terminate_ref(AntiCallToken(())).await
    }

    /// You should implement this function to define custom behavior.
    /// This function should flush any buffer it may hold.
    async fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()>;
}

#[async_trait]
impl<W: TerminatingWrite + ?Sized + Unpin> TerminatingWrite for Box<W> {
    async fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        self.as_mut().terminate_ref(token).await
    }
}

#[async_trait]
impl<W: TerminatingWrite + Unpin> TerminatingWrite for BufWriter<W> {
    async fn terminate_ref(&mut self, a: AntiCallToken) -> io::Result<()> {
        self.flush().await?;
        self.get_mut().terminate_ref(a).await
    }
}

#[async_trait]
impl TerminatingWrite for &mut Vec<u8> {
    async fn terminate_ref(&mut self, _a: AntiCallToken) -> io::Result<()> {
        self.flush().await
    }
}

#[cfg(test)]
mod test {

    use std::io::Write;

    use super::CountingWriter;

    #[test]
    fn test_counting_writer() {
        let buffer: Vec<u8> = vec![];
        let mut counting_writer = CountingWriter::wrap(buffer);
        let bytes = (0u8..10u8).collect::<Vec<u8>>();
        counting_writer.write_all(&bytes).unwrap();
        let len = counting_writer.written_bytes();
        let buffer_restituted: Vec<u8> = counting_writer.finish();
        assert_eq!(len, 10u64);
        assert_eq!(buffer_restituted.len(), 10);
    }
}
