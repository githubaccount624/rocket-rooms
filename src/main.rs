//! Implements a chat server using async rocket and SSE.

#![feature(proc_macro_hygiene)]

use futures_util::stream::StreamExt;

use rocket::{get, post, routes};
use rocket::request::{Form, State};
use rocket::response::NamedFile;

use rocket_rooms::sse;
use rocket_rooms::rooms::Rooms;

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

type RoomType = Rooms<String, i32, Message>;

#[get("/room/<room>/<user_id>")]
async fn room_stream(room: String, user_id: i32, rooms: State<'_, RoomType>) -> sse::SSE {
    // Subscribe to the room. 'subscription' is a Stream of Messages.
    let mut subscription = rooms.add_user(&room, user_id).await;

    // Create the SSE stream
    sse::with_writer(|mut writer| async move {
        loop {
            // Asynchronously get the next message from the room
            let message = subscription.next().await.expect("rooms can't 'close'");

            // Format messages as "<user> hi!"
            let formatted = format!("<{}> {}", message.from, message.text);

            // Send the message to the client
            if let Err(_) = writer.send(sse::Event::data(formatted)).await {
                // An error usually (TODO: always?) means the client has disconnected
                break;
            }
        }
    })
}

/*
#[get("/sse")]
async fn room_stream(room: String, user_id: i32, rooms: State<'_, RoomType>) -> sse::SSE {
    // Subscribe to the room. 'subscription' is a Stream of Messages.
    let mut subscription = rooms.add_user(&room, user_id).await;

    // Create the SSE stream
    sse::with_writer(|mut writer| async move {
        loop {
            // Asynchronously get the next message from the room
            let message = subscription.next().await.expect("rooms can't 'close'");

            // Format messages as "<user> hi!"
            let formatted = format!("<{}> {}", message.from, message.text);

            // Send the message to the client
            if let Err(_) = writer.send(sse::Event::data(formatted)).await {
                // An error usually (TODO: always?) means the client has disconnected
                break;
            }
        }
    })
}
*/

#[post("/join_room/<room>/<user>")]
async fn join_room(room: String, user: i32, rooms: State<'_, RoomType>) {
    // Send the message to the requested room
    // rooms.broadcast(&room, form.into_inner()).await;
    let mut subscription = rooms.add_user(&room, user).await;

}

#[post("/room/<room>", data="<form>")]
async fn post_message(room: String, form: Form<Message>, rooms: State<'_, RoomType>) {
    // Send the message to the requested room
    rooms.broadcast(&room, form.into_inner()).await;
}

fn main() {
    rocket::ignite()
        .manage(RoomType::new())
        .mount("/", routes![index, room_stream, join_room, post_message])
        .launch()
        .expect("server quit unexpectedly")
}
