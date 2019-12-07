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

#[get("/room/<room>/<user_id>")]
async fn room_stream(room: String, user_id: i32, rooms: State<'_, Rooms<Message>>) -> sse::SSE {
    // Subscribe to the room. 'subscription' is a Stream of Messages.
    let mut subscription = rooms.subscribe(&room, user_id).await;

    // let mut new_subscription = rooms.new_subscribe(&room, user_id);

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

#[post("/room/<room>", data="<form>")]
async fn post_message(room: String, form: Form<Message>, rooms: State<'_, Rooms<Message>>) {
    // Send the message to the requested room
    rooms.send(&room, form.into_inner()).await;
}

fn main() {
    rocket::ignite()
        .manage(Rooms::<Message>::new())
        .mount("/", routes![index, room_stream, post_message])
        .launch()
        .expect("server quit unexpectedly")
}
