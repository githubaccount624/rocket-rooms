//! Implements a chat server using async rocket and SSE.

#![feature(proc_macro_hygiene)]

use futures_util::stream::StreamExt;

use rocket::{get, post, routes};
use rocket::request::{Form, State};
use rocket::response::NamedFile;

use rocket_rooms::sse;
use rocket_rooms::rooms::Rooms;

use tokio;

use futures_util::stream::Stream;

#[derive(rocket::FromForm)]
#[derive(Clone)]
struct Message {
    pub from: String,
    pub text: String,
}

#[get("/")]
fn index() -> NamedFile {
    NamedFile::open("index.html").expect("index.html")
}

// Can also use SocketAddr for key instead of i32 user id

type RoomType = Rooms<String, i32>;

#[get("/sse/<user_id>")]
async fn room_stream(user_id: i32, rooms: State<'_, RoomType>) -> sse::SSE<impl Stream<Item = sse::Event>> {
    let mut subscription = rooms.subscribe(user_id).await;

    let stream = async_stream::stream! {
        while let Some(event) = subscription.next().await {
            yield event;
        }
    };

    sse::from_stream(stream)
}

#[post("/join_room/<room>/<user_id>")]
async fn join_room(room: String, user_id: i32, rooms: State<'_, RoomType>) {
    rooms.join(room, user_id).await;
}

#[post("/room/<room>", data="<form>")]
async fn post_message(room: String, form: Form<Message>, rooms: State<'_, RoomType>) {
    let inner_form = form.into_inner();

    let formatted = format!("{}: {}", inner_form.from, inner_form.text);

    if let Some(msg) = sse::Event::new(Some(room.clone()), formatted) {
        rooms.send(room, msg).await;
    }
}

#[tokio::main]
async fn main() {
    rocket::ignite()
        .manage(RoomType::new())
        .mount("/", routes![index, room_stream, join_room, post_message])
        .serve().await
        .expect("server quit unexpectedly")
}
