use std::pin::Pin;
use std::task::{Context, Poll};
use futures::stream::Stream;

use tokio::sync::mpsc;
use tokio::time;

use std::time::Duration;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::sse::Event;

type UsersToRooms = HashMap<String, HashSet<String>>;
type RoomsToUsers = HashMap<String, HashSet<String>>;
type UsersToSubscriptions = HashMap<String, mpsc::Sender<Event>>;

use core::fmt::Debug;

use futures::channel::oneshot;
use futures::future::FutureExt;

// Possible Ideas:
// Replace String with Arc<str> and make cloning a simple atomic increment since don't mutate the strings afterwards?
// Wrapping Room with a future aware mutex instead of using a channel?

pub struct Rooms {
    tx: mpsc::Sender<Command>
}

#[derive(Debug)]
enum Command {
    Subscribe { user: String, tx: mpsc::Sender<Event> },
    Join { user: String, room: String },
    Leave { user: String, room: String },
    Contains { user: String, room: String, sender: oneshot::Sender<bool> },
    SendRoom { room: String, message: Event },
    SendUser { user: String, message: Event },
    SendHeartbeat
}

pub struct Subscription(mpsc::Receiver<Event>);

const TASK_SHUTDOWN_ERROR_MESSAGE: &'static str = "Permanent background task was shut down unexpectedly";

impl Rooms {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1024); // what number? unbounded?
        tokio::spawn(Self::background_task(rx));
        Rooms { tx }
    }

    pub async fn subscribe(&self, user: String) -> Subscription {
        let (tx, rx) = mpsc::channel(128);

        self.tx.clone().send(Command::Subscribe { user, tx }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);

        Subscription(rx)
    }

    pub async fn join(&self, room: String, user: String) {
        self.tx.clone().send(Command::Join { room, user }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn leave(&self, room: String, user: String) {
        self.tx.clone().send(Command::Leave { room, user }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn contains(&self, room: String, user: String) -> bool {
        let (sender, receiver) = oneshot::channel::<bool>();

        self.tx.clone().send(Command::Contains { room, user, sender }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);

        receiver.map(|member| { return member }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE)
    }

    pub async fn send_room(&self, room: String, message: Event) {
        self.tx.clone().send(Command::SendRoom { room, message }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn send_user(&self, user: String, message: Event) {
        self.tx.clone().send(Command::SendUser { user, message }).await.expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    pub async fn spawn_heartbeat_task(&self, heartbeat_interval_secs: u64) {
        tokio::spawn(Self::heartbeat_task(self.tx.clone(), heartbeat_interval_secs));
    }
    
    fn helper_subscribe(uts: &mut UsersToSubscriptions, user: String, tx: mpsc::Sender<Event>) {
        uts.insert(user.clone(), tx);
    }

    fn helper_join(rtu: &mut RoomsToUsers, utr: &mut UsersToRooms, room: String, user: String) {
        let users_set = rtu.entry(room.clone()).or_insert(HashSet::new());
        users_set.insert(user.clone());

        let rooms_set = utr.entry(user).or_insert(HashSet::new());
        rooms_set.insert(room);
    }

    fn helper_leave(rtu: &mut RoomsToUsers, utr: &mut UsersToRooms, room: String, user: String) {
        if let Some(users_set) = rtu.get_mut(&room) {
            users_set.remove(&user);
        }

        if let Some(rooms_set) = utr.get_mut(&user) {
            rooms_set.remove(&room);
        }
    }

    async fn helper_contains(rtu: &RoomsToUsers, room: String, user: String, sender: oneshot::Sender<bool>) { 
        if let Some(room) = rtu.get(&room) {
            sender.send(room.contains(&user)).expect(TASK_SHUTDOWN_ERROR_MESSAGE);
            
            return;
        }

        sender.send(false).expect(TASK_SHUTDOWN_ERROR_MESSAGE);
    }

    async fn helper_send_room(uts: &mut UsersToSubscriptions, rtu: &mut RoomsToUsers, utr: &mut UsersToRooms, room: String, message: Event) { 
        let mut disconnects = vec![];
        
        if let Some(room) = rtu.get(&room) {
            for user in room.iter() {
                if let Some(sender) = uts.get_mut(user) {
                    if sender.try_send(message.clone()).is_err() {
                        disconnects.push(user.to_string());
                    }
                }
            }
        }

        for user in disconnects {
            Self::clean_up_user(uts, rtu, utr, user);
        }
    }

    async fn helper_send_user(uts: &mut UsersToSubscriptions, rtu: &mut RoomsToUsers, utr: &mut UsersToRooms, user: String, message: Event) { 
        if let Some(sender) = uts.get_mut(&user) {
            if sender.try_send(message.clone()).is_err() {
                Self::clean_up_user(uts, rtu, utr, user);
            }
        }
    }

    async fn helper_send_heartbeat(uts: &mut UsersToSubscriptions, rtu: &mut RoomsToUsers, utr: &mut UsersToRooms) {
        let mut disconnects = vec![];

        let heartbeat_message = Event::new(None, "\n\n".to_string());

        for (user, sender) in uts.iter_mut() {
            if sender.try_send(heartbeat_message.clone()).is_err() {
                disconnects.push(user.to_string());
            }
        }

        for user in disconnects {
            Self::clean_up_user(uts, rtu, utr, user);
        }
    }

    fn clean_up_user(uts: &mut UsersToSubscriptions, rtu: &mut RoomsToUsers, utr: &mut UsersToRooms, user: String) {
        uts.remove(&user);

        if let Some(member_rooms) = utr.remove(&user) {
            for r in member_rooms.iter() {
                if let Some(the_room) = rtu.get_mut(r) {
                    the_room.remove(&user);
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
        let mut utr: UsersToRooms = HashMap::new();
        let mut rtu: RoomsToUsers = HashMap::new();
        let mut uts: UsersToSubscriptions = HashMap::new();

        while let Some(command) = rx.recv().await {
            match command {
                Command::Subscribe { user, tx } => {
                    Self::helper_subscribe(&mut uts, user, tx);
                }
                Command::Join { room, user } => {
                    Self::helper_join(&mut rtu, &mut utr, room, user);
                }
                Command::Leave { room, user } => {
                    Self::helper_leave(&mut rtu, &mut utr, room, user);
                }
                Command::Contains { room, user, sender } => {
                    Self::helper_contains(&rtu, room, user, sender).await;
                }
                Command::SendRoom { room, message } => {
                    Self::helper_send_room(&mut uts, &mut rtu, &mut utr, room, message).await;
                }
                Command::SendUser { user, message } => {
                    Self::helper_send_user(&mut uts, &mut rtu, &mut utr, user, message).await;
                }
                Command::SendHeartbeat => {
                    Self::helper_send_heartbeat(&mut uts, &mut rtu, &mut utr).await;
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
