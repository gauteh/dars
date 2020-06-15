use std::pin::Pin;
use futures::ready;
use futures::stream::{Stream, StreamExt};
use futures::task::{Context, Poll};
use futures::{AsyncRead};

/// Flatten `Stream` of `AsyncRead` into `AsyncRead`.
pub struct AsyncReadFlatten<T, I>
    where T: Stream<Item = I> + std::marker::Unpin,
          I: AsyncRead + std::marker::Unpin + Send + Sync + 'static
{
    readers: T,
    state: AsyncReadFlattenState
}

impl <T, I> AsyncReadFlatten<T, I>
    where T: Stream<Item = I> + std::marker::Unpin,
          I: AsyncRead + std::marker::Unpin + Send + Sync + 'static
{
    pub fn from(readers: T) -> AsyncReadFlatten<T, I> {
        AsyncReadFlatten {
            readers: readers,
            state: AsyncReadFlattenState::PendingReader
        }
    }
}

impl<T,I> Unpin for AsyncReadFlatten<T,I>
    where T: Stream<Item = I> + std::marker::Unpin,
          I: AsyncRead + std::marker::Unpin + Send + Sync + 'static
{
}

pub enum AsyncReadFlattenState
{
    PendingReader,
    Reader(Pin<Box<dyn AsyncRead + std::marker::Unpin + Send + Sync + 'static>>),
    Eof
}

impl<T, I> AsyncRead for AsyncReadFlatten<T, I>
    where T: Stream<Item = I> + std::marker::Unpin,
          I: AsyncRead + std::marker::Unpin + Send + Sync + 'static
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8]
    ) -> Poll<Result<usize, std::io::Error>> {
        use AsyncReadFlattenState as St;

        loop {
            match &mut self.state {
                St::Reader(reader) => {
                    match ready!(reader.as_mut().poll_read(cx, buf)) {
                        Ok(len) if len == 0 => self.state = St::PendingReader,
                        Ok(len) => return Poll::Ready(Ok(len)),
                        Err(e) => return Poll::Ready(Err(e))
                    }
                },
                St::PendingReader => {
                    match ready!(self.readers.poll_next_unpin(cx)) {
                        Some(reader) => self.state = St::Reader(Box::pin(reader)), // fall through to next iteration
                        None => {
                            self.state = St::Eof;
                            return Poll::Ready(Ok(0))
                        }
                    }
                },
                St::Eof => return Poll::Ready(Ok(0))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::DodsVariable;
    use futures::io::{Cursor, AsyncReadExt, BufReader};
    use futures::stream::{self, StreamExt};
    use futures::executor::block_on;

    #[test]
    fn flatten_variables() {
        block_on(async {
            let stream = Box::pin(stream::once(
                async {
                    DodsVariable::Value(Box::pin(Cursor::new(vec![1u8, 2, 3, 4, 5, 6, 7, 8])))
                }).chain(
                stream::once(
                    async {
                        DodsVariable::Array(8, Box::pin(Cursor::new(vec![1u8, 2, 3, 4, 5, 6, 7, 8])))
                    }
                )).map(|d| d.as_reader()));

            let mut reader = BufReader::new(AsyncReadFlatten::from(stream));
            // let mut reader = AsyncReadFlatten::from(stream);
            let mut output = Vec::new();

            reader.read_to_end(&mut output).await.unwrap();
            assert_eq!(output, vec![1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 8, 0, 0, 0, 8, 1, 2, 3, 4, 5, 6, 7, 8]);
        });
    }
}
