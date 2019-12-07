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

pub struct Rooms<M> {
    subscriptions: DashMap<String, DashMap<i32, mpsc::Sender<M>>>
}

pub struct Subscription<M>(mpsc::Receiver<M>);

impl<M> Rooms<M> {
    pub fn new() -> Self {
        Self {
            subscriptions: DashMap::new()
        }
    }

    pub async fn subscribe(&self, room: &str, user_id: i32) -> Subscription<M> {
        let (tx, rx) = mpsc::channel(10);
        {
            let room = self.subscriptions.entry(room.to_string()).or_insert(DashMap::new());
            room.insert(user_id, tx);
        }
        Subscription(rx)
    }
}

impl<M: Clone> Rooms<M> {
    pub async fn send(&self, room: &str, message: M) { 
        if let Some(room) = self.subscriptions.get_mut(room) {
            let mut disconnects = vec![];

            for mut kv in &mut room.iter_mut() {
                if kv.value_mut().send(message.clone()).await.is_err() {
                    disconnects.push(kv.key().clone());
                }
            }

            for user_id in disconnects {
                room.remove(&user_id);
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
