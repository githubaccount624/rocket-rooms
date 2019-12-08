//! A collection of "rooms" (like chat rooms) that manages
//! subscriptions and broadcasting.
//!
//! The implementation is not very efficient; sending a message
//! will wait for any pending subscriptions and vice versa.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::stream::Stream;
use tokio::sync::{mpsc};

use dashmap::DashMap;
use std::hash::Hash;

use std::collections::HashSet;
use std::fmt::Display;

use crate::sse::Event;

pub struct Rooms<R: Eq + Hash + Clone + Display, U: Eq + Hash + Clone> {
    rooms_to_users: DashMap<R, HashSet<U>>,
    users_to_subscriptions: DashMap<U, mpsc::Sender<Event>>, // Should this be the pair instead of the Sender only?
    users_to_rooms: DashMap<U, HashSet<R>>
}

pub struct Subscription(mpsc::Receiver<Event>);

impl<R: Eq + Hash + Clone + Display, U: Eq + Hash + Clone> Rooms<R, U> {
    pub fn new() -> Self {
        Self {
            rooms_to_users: DashMap::new(), // The users in this room
            users_to_subscriptions: DashMap::new(), // The mpsc sender for each user
            users_to_rooms: DashMap::new() // The rooms this user belongs to
        }
    }

    // How to have it return the existing one if one already exists? (useful if client refreshes)
    // Would need to save rx too, no? But don't we have to return the actual copy here?
    pub async fn get_or_create_stream(&self, user: &U) -> Subscription {
        let (tx, rx) = mpsc::channel(10);

        self.users_to_subscriptions.insert(user.clone(), tx);

        Subscription(rx)
    }

    pub async fn add_user(&self, room: &R, user: &U) {
        let mut users_set = self.rooms_to_users.entry(room.clone()).or_insert(HashSet::new());
        users_set.insert(user.clone());

        let mut rooms_set = self.users_to_rooms.entry(user.clone()).or_insert(HashSet::new());
        rooms_set.insert(room.clone());
    }

    pub fn remove_user(&self, room: &R, user: &U) {
        if let Some(mut users_set) = self.rooms_to_users.get_mut(room) {
            users_set.remove(&user);
        }

        if let Some(mut rooms_set) = self.users_to_rooms.get_mut(user) {
            rooms_set.remove(&room);
        }
    }

    pub fn contains_user(&self, room: &R, user: &U) -> bool {
        if let Some(room) = self.rooms_to_users.get(room) {
            return room.contains(&user);
        }

        // TODO: The above should remove the user if their connection is dropped
        // Not sure how to do this other than sending a dummy message and seeing if it sent?

        return false;
    }
}

impl<R: Eq + Hash + Clone + Display, U: Eq + Hash + Clone> Rooms<R, U> {
    pub async fn broadcast(&self, room: &R, message: Event) { 
        if let Some(room) = self.rooms_to_users.get(room) {
            // let mut disconnects = vec![];

            for user in room.iter() {
                if let Some(mut sender) = self.users_to_subscriptions.get_mut(user) {
                    if sender.send(message.clone()).await.is_err() {
                        // Disconnected client. Remove from hashmaps
                    }
                }
            }

            /*
            for mut kv in &mut room.iter_mut() {
                if kv.value_mut().send(message.clone()).await.is_err() {
                    disconnects.push(kv.key().clone());
                }
            }

            for user in disconnects {
                room.remove(&user);
            }
            */
        }
    }
}

impl Stream for Subscription {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}
