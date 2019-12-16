use std::pin::Pin;
use std::task::{Context, Poll};
use futures::stream::Stream;

use tokio::sync::mpsc;

use std::hash::Hash;

use std::collections::HashMap;
use std::collections::HashSet;

use crate::sse::Event;

type UsersToRooms = HashMap<String, HashSet<String>>;
type RoomsToUsers = HashMap<String, HashSet<String>>;
type UsersToSubscriptions = HashMap<String, mpsc::Sender<Event>>;

pub struct Rooms {
    tx: mpsc::Sender<Command>
}

enum Command {
    Subscribe { user: String, tx: mpsc::Sender<Event> },
    Join { user: String, room: String },
    Leave { user: String, room: String },
    Contains { user: String, room: String, cb: Box<dyn FnOnce(bool) + Send + 'static> },
    SendRoom { room: String, message: Event },
    SendUser { user: String, message: Event }
}

pub struct Subscription(mpsc::Receiver<Event>);

// const TASK_SHUTDOWN_ERROR_MESSAGE = "permanent background task was shut down unexpectedly"

impl Rooms {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1024); // what number?
        tokio::spawn(Self::background_task(rx));
        Rooms { tx }
    }

    pub async fn subscribe(&self, user: String) -> Subscription {
        let (tx, rx) = mpsc::channel(10);

        self.tx.clone().send(Command::Subscribe { user, tx }).await;

        Subscription(rx)
    }

    pub async fn join(&self, room: String, user: String) {
        self.tx.clone().send(Command::Join { room, user }).await;
    }

    pub async fn leave(&self, room: String, user: String) {
        self.tx.clone().send(Command::Leave { room, user }).await;
    }

    pub async fn contains(&self, room: String, user: String, cb: Box<dyn FnOnce(bool) + Send + 'static>) {
        self.tx.clone().send(Command::Contains { room, user, cb }).await;
    }

    pub async fn send_room(&self, room: String, message: Event) {
        self.tx.clone().send(Command::SendRoom { room, message }).await;
    }

    pub async fn send_user(&self, user: String, message: Event) {
        self.tx.clone().send(Command::SendUser { user, message }).await;
    }
    
    fn helper_subscribe(uts: &mut UsersToSubscriptions, user: String, tx: mpsc::Sender<Event>) {
        uts.insert(user.clone(), tx);
    }

    fn helper_join(rtu: &mut RoomsToUsers, utr: &mut UsersToRooms, room: String, user: String) {
        let users_set = rtu.entry(room.clone()).or_insert(HashSet::new());
        users_set.insert(user.clone());

        let rooms_set = utr.entry(user.clone()).or_insert(HashSet::new());
        rooms_set.insert(room.clone());
    }

    fn helper_leave(rtu: &mut RoomsToUsers, utr: &mut UsersToRooms, room: String, user: String) {
        if let Some(users_set) = rtu.get_mut(&room) {
            users_set.remove(&user);
        }

        if let Some(rooms_set) = utr.get_mut(&user) {
            rooms_set.remove(&room);
        }
    }

    async fn helper_contains<'a>(rtu: &'a RoomsToUsers, room: String, user: String, cb: Box<dyn FnOnce(bool) + Send + 'static>) { 
        if let Some(room) = rtu.get(&room) {
            cb(room.contains(&user));
            
            return;
        }

        cb(false);

        // TODO: Should the above remove the client if they're disconnected?
        // Not sure how to do this other than sending a dummy message and seeing if it sent?
    }

    async fn helper_send_room(uts: &mut UsersToSubscriptions, rtu: &mut RoomsToUsers, utr: &mut UsersToRooms, room: String, message: Event) { 
        let mut disconnects = vec![];
        
        if let Some(room) = rtu.get(&room) {
            for user in room.iter() {
                if let Some(sender) = uts.get_mut(user) {
                    if sender.send(message.clone()).await.is_err() {
                        disconnects.push(user.clone());
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
            if sender.send(message.clone()).await.is_err() {
                Self::clean_up_user(uts, rtu, utr, user);
            }
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
                Command::Contains { room, user, cb } => {
                    Self::helper_contains(&rtu, room, user, cb).await;
                }
                Command::SendRoom { room, message } => {
                    Self::helper_send_room(&mut uts, &mut rtu, &mut utr, room, message).await;
                }
                Command::SendUser { user, message } => {
                    Self::helper_send_user(&mut uts, &mut rtu, &mut utr, user, message).await;
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