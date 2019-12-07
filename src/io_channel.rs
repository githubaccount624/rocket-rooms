//! SPSC channel with an AsyncRead and an AsyncWrite half. This is useful for
//! situations where a reader is required but a writer would be more useful or
//! idiomatic.
//!
//! A more efficient implementation of this module might be suitable for
//! an ecosystem crate such as futures-util or tokio-io.

use std::convert::TryInto;
use std::io::{self, Cursor, Read};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::prelude::Sink;
use tokio::stream::Stream;
use tokio::sync::mpsc;

pub struct IoChannelWriter {
    tx: mpsc::Sender<Vec<u8>>,
}

pub struct IoChannelReader {
    rx: mpsc::Receiver<Vec<u8>>,
    state: ReaderState,
}

enum ReaderState {
    Pending,
    Partial(Cursor<Vec<u8>>),
    Done,
}

impl AsyncWrite for IoChannelWriter {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, io::Error>> {
        // Wait for the receiver to be ready to get a new buffer
        // TODO: handle disconnects properly
        futures_core::ready!(self.tx.poll_ready(cx)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Send the buffer
        // TODO: to_vec() SAD
        Pin::new(&mut self.tx).start_send(buf.to_vec()).expect("poll_ready lied :(");

        // Report the whole buffer as being written
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // TODO: Is this implementable? Would need a way for the Reader to
        // tell the Writer it has finished
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // TODO: What would be the semantics of this?
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for IoChannelReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, io::Error>> {
        if buf.len() == 0 {
            return Poll::Ready(Ok(0));
        }

        loop {
            match &mut self.state {
                ReaderState::Pending => {
                    // Get the next buffer
                    match futures_core::ready!(Pin::new(&mut self.rx).poll_next(cx)) {
                        Some(next_buf) => self.state = ReaderState::Partial(Cursor::new(next_buf)),
                        None => self.state = ReaderState::Done,
                    }
                },
                ReaderState::Partial(cursor) => {
                    // Copy as much pending data as possible
                    let copied = cursor.read(buf)?;
                    if TryInto::<usize>::try_into(cursor.position()).unwrap() == cursor.get_ref().len() {
                        self.state = ReaderState::Pending;
                    }
                    return Poll::Ready(Ok(copied));
                },
                ReaderState::Done => return Poll::Ready(Ok(0)),
            }
        }
    }
}

pub fn io_channel() -> (IoChannelWriter, IoChannelReader) {
    let (tx, rx) = mpsc::channel(1);
    (IoChannelWriter { tx }, IoChannelReader { rx, state: ReaderState::Pending })
}

#[cfg(test)]
mod example {
    use std::future::Future;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    // Both Request and Response are read-like
    type Request = Pin<Box<dyn AsyncRead>>;
    type Response = Pin<Box<dyn AsyncRead>>;

    // 'service' is an async request -> response function.
    // This demo simulates a single request to 'service'
    fn run_server<F: Fn(Request) -> Fut, Fut: Future<Output=Response>>(service: F) {
        let req_body = b"Hello\n";
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut response = service(Box::pin(Cursor::new(req_body))).await;
            response.copy(&mut tokio::io::stdout()).await.expect("ouch");
        });
    }

    #[test]
    fn test1() {
        // Run a server that responds to requests in "read-like" ways
        run_server(|mut req_body| async move {
            let mut buf = vec![];
            req_body.read_to_end(&mut buf).await.expect("oops");
            for byte in &mut buf {
                *byte += 1;
            }
            Box::pin(Cursor::new(buf)) as Response
        });
    }

    #[test]
    fn test2() {
        // Run a server that responds to requests in "write-like" ways
        run_server(|mut _req_body| async move {
            // We have to return a response before it will start being read to the client, so just
            // spawn the writing as a background task. The background task will write into the
            // channel...
            let (mut writer, response) = io_channel();
            tokio::spawn(async move {
                let mut i = 0i32;
                loop {
                    i += 1;
                    let message = format!("Loop iteration {}\n", i);
                    writer.write_all(message.as_bytes()).await.expect("fail");

                    tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_secs(1)).await;
                    if i == 5 {
                        break;
                    }
                }
            });

            // ... and we immediately return the read end
            Box::pin(response) as Response
        });
    }
}
