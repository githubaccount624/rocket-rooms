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

pub struct Rooms<R: Eq + Hash + Clone, U: Eq + Hash + Clone, M> {
    subscriptions: DashMap<R, DashMap<U, mpsc::Sender<M>>>
}

pub struct Subscription<M>(mpsc::Receiver<M>);

impl<R: Eq + Hash + Clone, U: Eq + Hash + Clone, M> Rooms<R, U, M> {
    pub fn new() -> Self {
        Self {
            subscriptions: DashMap::new()
        }
    }

    /*
    pub async fn get_stream(&self, user: U) -> Subscription<M> {
        // have a hashmap of user to stream?
    }
    */

    pub async fn add_user(&self, room: &R, user: U) -> Subscription<M> {
        let (tx, rx) = mpsc::channel(10);
        {
            let room = self.subscriptions.entry(room.clone()).or_insert(DashMap::new());
            room.insert(user, tx);
        }
        Subscription(rx)
    }

    pub fn remove_user(&self, room: &R, user: U) -> Option<(U, mpsc::Sender<M>)> {
        if let Some(room) = self.subscriptions.get_mut(room) {
            return room.remove(&user);
        }

        return None;

        // Question: Does the above automatically drop the MPSC associated with the user?
    }

    pub fn contains_user(&self, room: &R, user: U) -> bool {
        if let Some(room) = self.subscriptions.get_mut(room) {
            return room.contains_key(&user);
        }

        // TODO: The above should remove the user if their connection is dropped
        // Not sure how to do this other than sending a dummy message and seeing if it sent?

        return false;
    }
}

impl<R: Eq + Hash + Clone, U: Eq + Hash + Clone, M: Clone> Rooms<R, U, M> {
    pub async fn broadcast(&self, room: &R, message: M) { 
        if let Some(room) = self.subscriptions.get_mut(room) {
            let mut disconnects = vec![];

            for mut kv in &mut room.iter_mut() {
                if kv.value_mut().send(message.clone()).await.is_err() {
                    disconnects.push(kv.key().clone());
                }
            }

            for user in disconnects {
                room.remove(&user);
            }
        }
    }
}

impl<M> Stream for Subscription<M> {
    type Item = M;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}
