use std::future::Future;

use rocket::request::Request;
use rocket::response::{Responder, Response, ResultFuture};
use tokio::io::{BufWriter, AsyncWrite, AsyncWriteExt};

use super::io_channel::{io_channel, IoChannelReader, IoChannelWriter};

#[derive(Clone, Debug)]
pub struct Event {
    event: Option<String>,
    data: String,
    // retry field? id field?
}

impl Event {
    pub fn new(event: Option<String>, data: String) -> Option<Self> {
        Some(Self { event, data })
    }

    /// Writes this event to a `writer` according in the EventStream format
    // TODO: Remove Unpin bound?
    pub async fn write_to<W: AsyncWrite + Unpin>(self, mut writer: W) -> Result<(), std::io::Error> {
        if let Some(event) = self.event {
            writer.write_all(b"event: ").await?;
            writer.write_all(event.as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }

        for line in self.data.lines() {
            writer.write_all(b"data: ").await?;
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }

        writer.write_all(b"\n").await?;
        Ok(())
    }
}

/// The 'read half' of an SSE stream. This type implements `Responder`; see the
/// [`with_writer`] function for a usage example.
pub struct SSE(IoChannelReader);

/// The 'send half' of an SSE stream. You can use the [`SSEWriter::send`] method
/// to send events to the stream
pub struct SSEWriter(BufWriter<IoChannelWriter>);

impl SSEWriter {
    /// Sends the `event` to the connected client
    pub async fn send(&mut self, event: Event) -> Result<(), std::io::Error> {
        event.write_to(&mut self.0).await?;
        self.0.flush().await?;
        Ok(())
    }
}

impl<'r> Responder<'r> for SSE {
    fn respond_to(self, _req: &'r Request<'_>) -> ResultFuture<'r> {
        Box::pin(async move {
            Response::build()
                .raw_header("Content-Type", "text/event-stream")
                .raw_header("Cache-Control", "no-transform") // no-cache
                .raw_header("Connection", "keep-alive")
                .streamed_body(self.0)
                .ok()
        })
    }
}

pub fn with_writer<F, Fut>(func: F) -> SSE
where
    F: FnOnce(SSEWriter) -> Fut,
    Fut: Future<Output=()> + Send + 'static,
{
    let (tx, rx) = io_channel();
    tokio::spawn(func(SSEWriter(BufWriter::new(tx))));
    SSE(rx)
}

// TODO: Consider an SSEStream that wraps an Stream<Item=Event>.
// Users would probably need to use something like async_stream, and the
// AsyncRead impl would probably have to be a pretty complex state machine