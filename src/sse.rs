use std::convert::TryInto;
use std::io::{self, Cursor, Read};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::stream::Stream;
use rocket::request::Request;
use rocket::response::{Responder, Response, ResultFuture};
use tokio::io::AsyncRead;

#[derive(Clone, Debug)]
pub struct Event {
    pub serialized: Vec<u8>
    // event, data, retry, and id fields
}

impl Event {
    pub fn new(event: Option<String>, data: String) -> Self {
        Self { serialized: Self::serialize(event, data) }
    }

    fn serialize(event: Option<String>, data: String) -> Vec<u8> {
        let mut vec = Vec::with_capacity(6); // minimum size for an SSE message

        if let Some(event) = event {
            vec.extend(b"event: ");
            vec.extend(event.into_bytes());
            vec.extend(b"\n");
        }

        for line in data.lines() {
            vec.extend(b"data: ");
            vec.extend(line.as_bytes());
            vec.extend(b"\n");
        }

        vec.extend(b"\n");

        vec
    }
}

pin_project_lite::pin_project! {
    /// An SSE stream. This type implements `Responder`; see the
    /// [`from_stream`] function for a usage example.
    pub struct SSE<S> {
        #[pin]
        stream: S,
        state: State,
    }
}

pub fn from_stream<S: Stream<Item=Event>>(stream: S) -> SSE<S> {
    SSE { stream, state: State::Pending }
}

impl<'r, S: Stream<Item=Event> + Send + 'r> Responder<'r> for SSE<S> {
    fn respond_to(self, _req: &'r Request<'_>) -> ResultFuture<'r> {
        Box::pin(async move {
            Response::build()
                .raw_header("Content-Type", "text/event-stream")
                .raw_header("Cache-Control", "no-transform") // no-cache
                .raw_header("Expires", "0")
                .raw_header("Connection", "keep-alive")
                .streamed_body(self)
                .ok()
        })
    }
}

enum State {
    Pending,
    Partial(Cursor<Vec<u8>>),
    Done,
}

impl<S: Stream<Item=Event>> AsyncRead for SSE<S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, io::Error>> {
        let mut this = self.project();

        if buf.len() == 0 {
            return Poll::Ready(Ok(0));
        }

        loop {
            match this.state {
                State::Pending => {
                    // Get the next buffer
                    match this.stream.as_mut().poll_next(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(next_event)) => *this.state = State::Partial(Cursor::new(next_event.serialized)),
                        Poll::Ready(None) => *this.state = State::Done,
                    }
                },
                State::Partial(cursor) => {
                    // Copy as much pending data as possible
                    let copied = cursor.read(buf)?;
                    if TryInto::<usize>::try_into(cursor.position()).unwrap() == cursor.get_ref().len() {
                        *this.state = State::Pending;
                    }
                    return Poll::Ready(Ok(copied));
                },
                State::Done => return Poll::Ready(Ok(0)),
            }
        }
    }
}