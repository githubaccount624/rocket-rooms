use std::pin::Pin;
use std::task::{Context, Poll};
use futures::stream::Stream;

use tokio::sync::mpsc;
use tokio::time;

use std::time::Duration;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use crate::sse::Event;

use core::fmt::Debug;

use futures::channel::oneshot;
use futures::future::FutureExt;

struct Mappings {
    user_to_subs: HashMap<i32, HashSet<String>>,
    room_to_subs: HashMap<String, HashSet<String>>,
    sub_to_client: HashMap<String, Client>,
    event_log: VecDeque<Event>,
    last_generated_id: u64
}

const MAX_EVENT_LOG_SIZE: usize = 10000;
const MAX_CLIENT_LOG_SIZE: usize = 500;

// try_send will either succeed immediately, and the client will get the event "eventually". Or it will fail,
// because events are being queued faster than they can be sent to the client, or because the client has disconnected

// Possible Ideas:
// Replace String with Arc<str> and make cloning a simple atomic increment since don't mutate the strings afterwards?
// Wrapping Room with a future aware mutex instead of using a channel?

pub struct Client {
    consecutive_heartbeat_failures: u64,
    sender: mpsc::Sender<Event>,
    log: VecDeque<u64>,
    rooms: HashSet<String>
}

pub trait VecDequeExt<T> {
    fn push_drop(&mut self, val: T, cap: usize);
}

impl<T> VecDequeExt<T> for VecDeque<T> {
    fn push_drop(&mut self, val: T, cap: usize) {
        self.push_back(val);
    
        if self.len() > cap {
            self.pop_front();
        }
    }
}

fn search_event_log(event_log: &VecDeque<Event>, id: u64) -> Option<Event> {
	if let Some(e0) = event_log.get(0) {
	    if let Some(idx) = id.checked_sub(e0.id) {
        	if let Some(val) = event_log.get(idx as usize) {
				if val.id == id {
					return Some(val.clone());
				}
    	    		}
	    	}
	}
	
	return None;
}

pub struct Rooms {
    tx: mpsc::Sender<Command>
}

#[derive(Debug)]
enum Command {
    Subscribe { sub: String, user_id: Option<i32>, tx: mpsc::Sender<Event> },
    Join { sub: String, room: String },
    JoinUser { user_id: i32, room: String },
    RoomJoinRoom { current_room: String, new_room: String },
    Leave { sub: String, room: String },
    Contains { sub: String, room: String, sender: oneshot::Sender<bool> },
    SendRoom { room: String, event: &'static str, data: String },
    SendUser { user_id: i32, event: &'static str, data: String },
    SendMissingEvents { sub: String, last_event_id: u64 },
    SendHeartbeat
}

pub struct Subscription(mpsc::Receiver<Event>);

const TASK_SHUTDOWN_ERROR_MESSAGE: &'static str = "Permanent background task was shut down unexpectedly";
const HEARTBEAT_FAILURE_LIMIT: u64 = 500;

impl Rooms {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(Self::background_task(rx));
        Rooms { tx }
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

    pub async fn send_room(&self, room: String, event: &'static str, data: String) {
        self.tx.clone().send(Command::SendRoom { room, event, data }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn send_user(&self, user_id: i32, event: &'static str, data: String) {
        self.tx.clone().send(Command::SendUser { user_id, event, data }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn send_missing_events(&self, sub: String, last_event_id: u64) {
        self.tx.clone().send(Command::SendMissingEvents { sub, last_event_id }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn spawn_heartbeat_task(&self, heartbeat_interval_secs: u64) {
        tokio::spawn(Self::heartbeat_task(self.tx.clone(), heartbeat_interval_secs));
    }
    
    fn helper_subscribe(m: &mut Mappings, sub: String, user_id: Option<i32>, tx: mpsc::Sender<Event>) {
        if let Some(user_id) = user_id {
            let subs = m.user_to_subs.entry(user_id).or_insert(HashSet::new());
            subs.insert(sub.clone());
        }

        if let Some(client) = m.sub_to_client.get_mut(&sub) {
            client.sender = tx;
            client.consecutive_heartbeat_failures = 0;
        } else {
            m.sub_to_client.insert(sub, Client { sender: tx, log: VecDeque::new(), consecutive_heartbeat_failures: 0, rooms: HashSet::new() });
        }
    }

    fn helper_join(m: &mut Mappings, room: String, sub: String) {
        let subs = m.room_to_subs.entry(room.clone()).or_insert(HashSet::new());
        subs.insert(sub.clone());

        if let Some(client) = m.sub_to_client.get_mut(&sub) {
            client.rooms.insert(room);
        }
    }

    fn helper_room_join_room(m: &mut Mappings, current_room: String, new_room: String) {
        if let Some(subs) = m.room_to_subs.get(&current_room) {
            let cloned = subs.clone();

            for sub in cloned {
                Self::helper_join(m, new_room.clone(), sub);
            }
        }
    }

    fn helper_join_user(m: &mut Mappings, room: String, user_id: i32) {
        if let Some(subs) = m.user_to_subs.get_mut(&user_id) {
            for sub in subs.iter() {
                if let Some(client) = m.sub_to_client.get_mut(sub) {
                    client.rooms.insert(room.clone());
                }

                let room_subs = m.room_to_subs.entry(room.clone()).or_insert(HashSet::new());
                room_subs.insert(sub.clone());
            }
        }
    }

    fn helper_leave(m: &mut Mappings, room: String, sub: String) {
        if let Some(subs) = m.room_to_subs.get_mut(&room) {
            subs.remove(&sub);
        }

        if let Some(client) = m.sub_to_client.get_mut(&sub) {
            client.rooms.remove(&room);
        }
    }

    async fn helper_contains(m: &mut Mappings, room: String, sub: String, sender: oneshot::Sender<bool>) { 
        if let Some(subs) = m.room_to_subs.get(&room) {
            sender.send(subs.contains(&sub)).expect(TASK_SHUTDOWN_ERROR_MESSAGE);
            
            return;
        }

        sender.send(false).expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    fn send_client_message(event_log: &mut VecDeque<Event>, sub: String, client: &mut Client, event: Event) -> bool {
        client.log.push_drop(event.id, MAX_CLIENT_LOG_SIZE);

        let sended_successfully = client.sender.try_send(event).is_ok();

        if sended_successfully {
            client.consecutive_heartbeat_failures = 0;
        }

        client.consecutive_heartbeat_failures < HEARTBEAT_FAILURE_LIMIT
    }

    fn create_new_event(m: &mut Mappings, event: &str, data: String) -> Event {
        let event = Event::new(event, data, m.last_generated_id);
        m.last_generated_id += 1;

        m.event_log.push_drop(event.clone(), MAX_EVENT_LOG_SIZE);

        event
    }

    async fn helper_send_room(m: &mut Mappings, room: String, event: &str, data: String) { 
        let mut disconnects = vec![];

        let event = Self::create_new_event(m, event, data);
        
        if let Some(subs) = m.room_to_subs.get(&room) {
            for sub in subs.iter() {
                if let Some(mut client) = m.sub_to_client.get_mut(sub) {
                    if !Self::send_client_message(&mut m.event_log, sub.clone(), &mut client, event.clone()) {
                        disconnects.push(sub.to_string());
                    }
                }
            }
        }

        for sub in disconnects {
            Self::clean_up_user(m, sub);
        }
    }

    async fn helper_send_user(m: &mut Mappings, user_id: i32, event: &str, data: String) { 
        let mut disconnects = vec![];

        let event = Self::create_new_event(m, event, data);

        if let Some(subs) = m.user_to_subs.get_mut(&user_id) {
            for sub in subs.iter() {
                if let Some(mut client) = m.sub_to_client.get_mut(sub) {
                    if !Self::send_client_message(&mut m.event_log, sub.clone(), &mut client, event.clone()) {
                        disconnects.push(sub.to_string());
                    }
                }
            }
        }

        for sub in disconnects {
            Self::clean_up_user(m, sub);
        }
    }

    fn send_refresh_event(client: &mut Client) {
        let refresh_event = Event::new("sse-refresh", "refresh".to_string(), 0);

        if client.sender.try_send(refresh_event).is_err() {
            // ?
        }
    }

    async fn helper_send_missing_events(m: &mut Mappings, sub: String, last_event_id: u64) {
        if let Some(client) = m.sub_to_client.get_mut(&sub) {
            for id in &client.log {
                if client.log.len() >= MAX_CLIENT_LOG_SIZE && last_event_id < client.log[0] { // there's a gap in event log. refresh
                    Self::send_refresh_event(client);

                    return;
                } 

                if *id > last_event_id { // change to binary search?
                    if let Some(event) = search_event_log(&m.event_log, *id) {
                        if client.sender.try_send(event).is_err() {
                            // delete?
                        }
                    } else { // event not found in global log. refresh
                        Self::send_refresh_event(client);

                        return;
                    }
                }
            }
        }
    }

    async fn helper_send_heartbeat(m: &mut Mappings) {
        let mut disconnects = vec![];

        let heartbeat_message = Event::new("hb", "\n\n".to_string(), 0); // change from hb to empty message?

        for (sub, client) in m.sub_to_client.iter_mut() {
            if client.sender.try_send(heartbeat_message.clone()).is_err() {
                client.consecutive_heartbeat_failures += 1;

                if client.consecutive_heartbeat_failures > HEARTBEAT_FAILURE_LIMIT {
                    disconnects.push(sub.to_string());
                }
            } else {
                client.consecutive_heartbeat_failures = 0;
            }
        }

        for sub in disconnects {
            Self::clean_up_user(m, sub);
        }
    }

    fn clean_up_user(m: &mut Mappings, sub: String) {
        let splitted: Vec<_> = sub.split("-").collect();

        if splitted.len() > 0 {
            let user_id = splitted[0].parse::<i32>().unwrap();

            if let Some(subs) = m.user_to_subs.get_mut(&user_id) {
                subs.remove(&sub);
            }
        }

        if let Some(client) = m.sub_to_client.remove(&sub) {
            for room in client.rooms.iter() {
                if let Some(subs) = m.room_to_subs.get_mut(room) {
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
        let mut m = Mappings {
            user_to_subs: HashMap::new(),
            room_to_subs: HashMap::new(),
            sub_to_client: HashMap::new(),
            event_log: VecDeque::new(),
            last_generated_id: 1
        };

        while let Some(command) = rx.recv().await {
            match command {
                Command::Subscribe { sub, user_id, tx } => {
                    Self::helper_subscribe(&mut m, sub, user_id, tx);
                }
                Command::Join { room, sub } => {
                    Self::helper_join(&mut m, room, sub);
                }
                Command::JoinUser { room, user_id } => {
                    Self::helper_join_user(&mut m, room, user_id);
                }
                Command::RoomJoinRoom { current_room, new_room } => {
                    Self::helper_room_join_room(&mut m, current_room, new_room);
                }
                Command::Leave { room, sub } => {
                    Self::helper_leave(&mut m, room, sub);
                }
                Command::Contains { room, sub, sender } => {
                    Self::helper_contains(&mut m, room, sub, sender).await;
                }
                Command::SendRoom { room, event, data } => {
                    Self::helper_send_room(&mut m, room, event, data).await;
                }
                Command::SendUser { user_id, event, data } => {
                    Self::helper_send_user(&mut m, user_id, event, data).await;
                }
                Command::SendMissingEvents { sub, last_event_id } => {
                    Self::helper_send_missing_events(&mut m, sub, last_event_id).await;
                }
                Command::SendHeartbeat => {
                    Self::helper_send_heartbeat(&mut m).await;
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
