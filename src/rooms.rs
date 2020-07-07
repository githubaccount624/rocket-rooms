use std::pin::Pin;
use std::task::{Context, Poll};
use futures::stream::Stream;

use tokio::sync::mpsc;
use tokio::time;

use std::time::Duration;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::sse::Event;

use core::fmt::Debug;

use futures::channel::oneshot;
use futures::future::FutureExt;

struct Mappings {
    user_to_subs: HashMap<i32, HashSet<String>>,
    room_to_subs: HashMap<String, HashSet<String>>,
    sub_to_client: HashMap<String, Client>,
    sub_to_log: HashMap<String, Vec<u64>>,
    id_to_event: HashMap<u64, Event>
}

// try_send will either succeed immediately, and the client will get the event "eventually". Or it will fail, because events are being queued faster than they can be sent to the client, or because the client has disconnected

pub struct Client {
    consecutive_failures: u64,
    sender: mpsc::Sender<Event>,
    rooms: HashSet<String>
}

// Possible Ideas:
// Replace String with Arc<str> and make cloning a simple atomic increment since don't mutate the strings afterwards?
// Wrapping Room with a future aware mutex instead of using a channel?

use std::sync::atomic::{AtomicU64, Ordering};

pub struct Rooms {
    tx: mpsc::Sender<Command>,
    last_generated_id: AtomicU64
}

#[derive(Debug)]
enum Command {
    Subscribe { sub: String, user_id: Option<i32>, tx: mpsc::Sender<Event> },
    Join { sub: String, room: String },
    JoinUser { user_id: i32, room: String },
    RoomJoinRoom { current_room: String, new_room: String },
    Leave { sub: String, room: String },
    Contains { sub: String, room: String, sender: oneshot::Sender<bool> },
    SendRoom { room: String, event: Event },
    SendUser { user_id: i32, event: Event },
    SendMissingEvents { sub: String, last_event_id: u64 },
    SendHeartbeat
}

pub struct Subscription(mpsc::Receiver<Event>);

const TASK_SHUTDOWN_ERROR_MESSAGE: &'static str = "Permanent background task was shut down unexpectedly";
const FAILURE_LIMIT: u64 = 100; // increase?

impl Rooms {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(Self::background_task(rx));
        Rooms { tx, last_generated_id: AtomicU64::new(1) }
    }

    pub fn create_event(&self, event: &str, data: String) -> Event {
        let id = self.last_generated_id.fetch_add(1, Ordering::SeqCst);

        Event::new(event, data, id)
    }

    pub async fn subscribe(&self, sub: String, user_id: Option<i32>) -> Subscription {
        let (tx, rx) = mpsc::channel(128);

        self.tx.clone().send(Command::Subscribe { sub, user_id, tx }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);

        Subscription(rx)
    }

    pub async fn join(&self, room: String, sub: String) {
        self.tx.clone().send(Command::Join { room, sub }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn join_user(&self, room: String, user_id: i32) {
        self.tx.clone().send(Command::JoinUser { room, user_id }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn room_join_room(&self, current_room: String, new_room: String) {
        self.tx.clone().send(Command::RoomJoinRoom { current_room, new_room }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn leave(&self, room: String, sub: String) {
        self.tx.clone().send(Command::Leave { room, sub }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn contains(&self, room: String, sub: String) -> bool {
        let (sender, receiver) = oneshot::channel::<bool>();

        self.tx.clone().send(Command::Contains { room, sub, sender }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);

        receiver.map(|member| { return member }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE)
    }

    pub async fn send_room(&self, room: String, event: Event) {
        self.tx.clone().send(Command::SendRoom { room, event }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn send_user(&self, user_id: i32, event: Event) {
        self.tx.clone().send(Command::SendUser { user_id, event }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn send_missing_events(&self, sub: String, last_event_id: u64) {
        self.tx.clone().send(Command::SendMissingEvents { sub, last_event_id }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn spawn_heartbeat_task(&self, heartbeat_interval_secs: u64) {
        tokio::spawn(Self::heartbeat_task(self.tx.clone(), heartbeat_interval_secs));
    }
    
    fn helper_subscribe(mappings: &mut Mappings, sub: String, user_id: Option<i32>, tx: mpsc::Sender<Event>) {
        if let Some(user_id) = user_id {
            let mut subs = HashSet::new();
            subs.insert(sub.clone());

            mappings.user_to_subs.insert(user_id, subs);
        }

        mappings.sub_to_client.insert(sub, Client { sender: tx, consecutive_failures: 0, rooms: HashSet::new() });
    }

    fn helper_join(mappings: &mut Mappings, room: String, sub: String) {
        let subs = mappings.room_to_subs.entry(room.clone()).or_insert(HashSet::new());
        subs.insert(sub.clone());

        if let Some(client) = mappings.sub_to_client.get_mut(&sub) {
            client.rooms.insert(room);
        }
    }

    fn helper_room_join_room(mappings: &mut Mappings, current_room: String, new_room: String) {
        if let Some(subs) = mappings.room_to_subs.get(&current_room) {
            let cloned = subs.clone();

            for sub in cloned {
                Self::helper_join(mappings, new_room.clone(), sub);
            }
        }
    }

    fn helper_join_user(mappings: &mut Mappings, room: String, user_id: i32) {
        if let Some(subs) = mappings.user_to_subs.get_mut(&user_id) {
            for sub in subs.iter() {
                if let Some(client) = mappings.sub_to_client.get_mut(sub) {
                    client.rooms.insert(room.clone());
                }

                let room_subs = mappings.room_to_subs.entry(room.clone()).or_insert(HashSet::new());
                room_subs.insert(sub.clone());
            }
        }
    }

    fn helper_leave(mappings: &mut Mappings, room: String, sub: String) {
        if let Some(subs) = mappings.room_to_subs.get_mut(&room) {
            subs.remove(&sub);
        }

        if let Some(client) = mappings.sub_to_client.get_mut(&sub) {
            client.rooms.remove(&room);
        }
    }

    async fn helper_contains(mappings: &mut Mappings, room: String, sub: String, sender: oneshot::Sender<bool>) { 
        if let Some(subs) = mappings.room_to_subs.get(&room) {
            sender.send(subs.contains(&sub)).expect(TASK_SHUTDOWN_ERROR_MESSAGE);
            
            return;
        }

        sender.send(false).expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    fn send_client_message(sub_to_log: &mut HashMap<String, Vec<u64>>, id_to_event: &mut HashMap<u64, Event>, sub: String, client: &mut Client, event: Event) -> bool {
        let log = sub_to_log.entry(sub).or_insert(Vec::new());
        log.push(event.id);

        if id_to_event.get(&event.id).is_none() {
            id_to_event.insert(event.id, event.clone());
        }

        let failed_to_send = client.sender.try_send(event).is_err();

        if failed_to_send {
            client.consecutive_failures += 1;
        } else {
            client.consecutive_failures = 0;
        }

        client.consecutive_failures < FAILURE_LIMIT
    }

    async fn helper_send_room(mappings: &mut Mappings, room: String, event: Event) { 
        let mut disconnects = vec![];
        
        if let Some(subs) = mappings.room_to_subs.get(&room) {
            for sub in subs.iter() {
                if let Some(mut client) = mappings.sub_to_client.get_mut(sub) {
                    println!("sending a message to this client {} to this room {}", sub, room);

                    if !Self::send_client_message(&mut mappings.sub_to_log, &mut mappings.id_to_event, sub.clone(), &mut client, event.clone()) {
                        disconnects.push(sub.to_string());
                    }
                }
            }
        }

        for sub in disconnects {
            Self::clean_up_user(mappings, sub);
        }
    }

    async fn helper_send_user(mappings: &mut Mappings, user_id: i32, event: Event) { 
        let mut disconnects = vec![];

        if let Some(subs) = mappings.user_to_subs.get_mut(&user_id) {
            for sub in subs.iter() {
                if let Some(mut client) = mappings.sub_to_client.get_mut(sub) {
                    if !Self::send_client_message(&mut mappings.sub_to_log, &mut mappings.id_to_event, sub.clone(), &mut client, event.clone()) {
                        disconnects.push(sub.to_string());
                    }
                }
            }
        }

        for sub in disconnects {
            Self::clean_up_user(mappings, sub);
        }
    }

    async fn helper_send_missing_events(mappings: &mut Mappings, sub: String, last_event_id: u64) {
        if let Some(client) = mappings.sub_to_client.get_mut(&sub) {
            if let Some(log) = mappings.sub_to_log.get(&sub) {
                for id in log {
                    if *id > last_event_id { // change to binary search?
                        if let Some(event) = mappings.id_to_event.get(id) {
                            if client.sender.try_send(event.clone()).is_err() {
                                // delete?
                            }
                        }
                    }
                }
            }
        }
    }

    async fn helper_send_heartbeat(mappings: &mut Mappings) {
        let mut disconnects = vec![];

        let heartbeat_message = Event::new("hb", "\n\n".to_string(), 0); // change from hb to empty message

        for (sub, client) in mappings.sub_to_client.iter_mut() {
            if client.sender.try_send(heartbeat_message.clone()).is_err() {
                client.consecutive_failures += 1;

                if client.consecutive_failures > FAILURE_LIMIT { // refactor this to a shared function?
                    disconnects.push(sub.to_string());
                }
            } else {
                client.consecutive_failures = 0;
            }
        }

        for sub in disconnects {
            Self::clean_up_user(mappings, sub);
        }
    }

    fn clean_up_user(mappings: &mut Mappings, sub: String) {
        let splitted: Vec<_> = sub.split("-").collect();

        if splitted.len() > 0 {
            let user_id = splitted[0].parse::<i32>().unwrap();

            if let Some(subs) = mappings.user_to_subs.get_mut(&user_id) {
                subs.remove(&sub);
            }
        }

        if let Some(client) = mappings.sub_to_client.remove(&sub) {
            for room in client.rooms.iter() {
                if let Some(subs) = mappings.room_to_subs.get_mut(room) {
                    subs.remove(&sub);
                }
            }
        }
    }

    async fn heartbeat_task(mut tx: mpsc::Sender<Command>, heartbeat_interval_secs: u64) {
        let mut interval = time::interval(Duration::from_secs(heartbeat_interval_secs));

        loop {
            tx.send(Command::SendHeartbeat).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    
            interval.tick().await;
        }
    }

    async fn background_task(mut rx: mpsc::Receiver<Command>) {
        let mut mappings = Mappings {
            user_to_subs: HashMap::new(),
            room_to_subs: HashMap::new(),
            sub_to_client: HashMap::new(),
            sub_to_log: HashMap::new(),
            id_to_event: HashMap::new()
        };

        while let Some(command) = rx.recv().await {
            match command {
                Command::Subscribe { sub, user_id, tx } => {
                    Self::helper_subscribe(&mut mappings, sub, user_id, tx);
                }
                Command::Join { room, sub } => {
                    Self::helper_join(&mut mappings, room, sub);
                }
                Command::JoinUser { room, user_id } => {
                    Self::helper_join_user(&mut mappings, room, user_id);
                }
                Command::RoomJoinRoom { current_room, new_room } => {
                    Self::helper_room_join_room(&mut mappings, current_room, new_room);
                }
                Command::Leave { room, sub } => {
                    Self::helper_leave(&mut mappings, room, sub);
                }
                Command::Contains { room, sub, sender } => {
                    Self::helper_contains(&mut mappings, room, sub, sender).await;
                }
                Command::SendRoom { room, event } => {
                    Self::helper_send_room(&mut mappings, room, event).await;
                }
                Command::SendUser { user_id, event } => {
                    Self::helper_send_user(&mut mappings, user_id, event).await;
                }
                Command::SendMissingEvents { sub, last_event_id } => {
                    Self::helper_send_missing_events(&mut mappings, sub, last_event_id).await;
                }
                Command::SendHeartbeat => {
                    Self::helper_send_heartbeat(&mut mappings).await;
                }
            }
        }
    }
}

impl Stream for Subscription {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_recv(cx)
    }
}
