//! An SSE Responder.
//!
//! This module might be suitable for inclusion in rocket_contrib.

use std::future::Future;

use rocket::request::Request;
use rocket::response::{Responder, Response, ResultFuture};
use tokio::io::{BufWriter, AsyncWrite, AsyncWriteExt};

use super::io_channel::{io_channel, IoChannelReader, IoChannelWriter};


// TODO: Comprehensive support for all possible message types and fields:
//   * comments
//   * 'retry' field
//   * custom fields (ignored by EventSource API, but worth considering)
/// A single SSE message, with optional `event`, `data`, and `id` fields.
#[derive(Clone, Debug)]
pub struct Event {
    event: Option<String>,
    data: Option<String>,
    id: Option<String>
}

impl Event {
    /// Create a new Event with only the data field specified
    pub fn data<S: Into<String>>(data: S) -> Self {
        Self { event: None, id: None, data: Some(data.into()) }
    }

    // TODO: Result instead of panic!
    /// Create a new Event with event, data, and id all (optionally) specified
    ///
    /// # Panics
    ///
    /// Panics if either `event` or `id` contain newlines
    pub fn new(event: Option<String>, data: Option<String>, id: Option<String>) -> Self {
        if event.as_ref().map_or(false, |e| e.find(|b| b == '\r' || b == '\n').is_some()) {
            panic!("event cannot contain newlines");
        }

        if id.as_ref().map_or(false, |i| i.find(|b| b == '\r' || b == '\n').is_some()) {
            panic!("id cannot contain newlines");
        }

        Self { event, id, data }
    }

    /// Writes this event to a `writer` according in the EventStream
    /// format
    //TODO: Remove Unpin bound?
    pub async fn write_to<W: AsyncWrite + Unpin>(self, mut writer: W) -> Result<(), std::io::Error> {
        if let Some(event) = self.event {
            writer.write_all(b"event: ").await?;
            writer.write_all(event.as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }
        if let Some(id) = self.id {
            writer.write_all(b"id: ").await?;
            writer.write_all(id.as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }
        if let Some(data) = self.data {
            for line in data.lines() {
                writer.write_all(b"data: ").await?;
                writer.write_all(line.as_bytes()).await?;
                writer.write_all(b"\n").await?;
            }
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
                .streamed_body(self.0)
                .ok()
        })
    }
}

/// Creates an SSE stream based on an [`SSEWriter`].
///
/// Typical usage:
///
/// ```rust
/// # use rocket::get;
/// #
///
/// use rocket_rooms::sse::{self, Event, SSE};
/// #[get("/stream")]
/// fn stream() -> SSE {
///     sse::with_writer(|mut writer| async move {
///         writer.send(Event::data("data1")).await.unwrap();
///         writer.send(Event::data("data2")).await.unwrap();
///         writer.send(Event::data("data3")).await.unwrap();
///     })
/// }
/// ```
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
