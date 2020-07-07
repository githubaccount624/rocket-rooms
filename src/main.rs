//! Implements a chat server using async rocket and SSE.
// this file is not up to date

#![feature(proc_macro_hygiene)]

use futures_util::stream::StreamExt;

use rocket::{get, post, routes};
use rocket::request::{Form, State};
use rocket::response::NamedFile;

use rocket_rooms::sse;
use rocket_rooms::rooms::Rooms;

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

#[get("/sse/<user_id>")]
async fn room_stream(user_id: i32, rooms: State<'_, Rooms>) -> sse::SSE<impl Stream<Item = sse::Event>> {
    let mut subscription = rooms.subscribe(user_id.to_string()).await;

    let stream = async_stream::stream! {
        while let Some(event) = subscription.next().await {
            yield event;
        }
    };

    sse::from_stream(stream)
}

#[post("/join_room/<room>/<user_id>")]
async fn join_room(room: String, user_id: i32, rooms: State<'_, Rooms>) {
    let member = rooms.contains(room.clone(), user_id.to_string()).await;

    println!("member: {}", member);

    rooms.join(room.clone(), user_id.to_string()).await;

    let member = rooms.contains(room.clone(), user_id.to_string()).await;

    println!("member: {}", member);
}

#[post("/room/<room>", data="<form>")]
async fn post_message(room: String, form: Form<Message>, rooms: State<'_, Rooms>) {
    let inner_form = form.into_inner();

    let formatted = format!("{}: {}", inner_form.from, inner_form.text);

    let event = sse::Event::new(Some(room.clone()), formatted);
    rooms.send_room(room, event).await;
}

#[tokio::main]
async fn main() {
    let rooms = Rooms::new();
    rooms.spawn_heartbeat_task(60).await;

    rocket::ignite()
        .manage(rooms)
        .mount("/", routes![index, room_stream, join_room, post_message])
        .serve().await
        .expect("server quit unexpectedly")
}
