use std::{collections::HashSet, sync::atomic::AtomicU64};

use axum::extract::ws::{Message, WebSocket};
use dashmap::{DashMap, DashSet};
use futures::{
    SinkExt, TryStreamExt,
    stream::{SplitSink, SplitStream},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

//message coming into the server from client
#[derive(Serialize, Deserialize)]
pub struct Incoming {
    pub content: String,
}

//message going out of the server from the client
#[derive(Serialize, Deserialize)]
pub struct Outgoing {
    pub sender: String,
    pub content: String,
}

pub struct ChatRoom {
    id: u64,
    clients: HashSet<u64>,
}

impl ChatRoom {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            clients: HashSet::new(),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn add_client(&mut self, client_id: u64) {
        self.clients.insert(client_id);
    }

    pub fn remove_client(&mut self, client_id: u64) {
        self.clients.remove(&client_id);
    }
}

pub struct ClientWriter {
    writer: SplitSink<WebSocket, Message>,
}

impl ClientWriter {
    pub fn new(writer: SplitSink<WebSocket, Message>) -> Self {
        Self { writer }
    }

    pub async fn send(&mut self, msg: Message) -> anyhow::Result<()> {
        self.writer.send(msg).await?;
        Ok(())
    }
}

pub struct ClientReader {
    reader: SplitStream<WebSocket>,
}

impl ClientReader {
    pub fn new(reader: SplitStream<WebSocket>) -> Self {
        Self { reader }
    }

    pub async fn recv(&mut self) -> anyhow::Result<Option<Message>> {
        let msg = self.reader.try_next().await?;
        Ok(msg)
    }
}

pub struct IdGenerator {
    room: AtomicU64,
    client: AtomicU64,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self {
            room: AtomicU64::new(0),
            client: AtomicU64::new(0),
        }
    }

    pub fn next_room_id(&self) -> u64 {
        self.room.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn next_client_id(&self) -> u64 {
        self.client
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

//struct for chat room structures
pub struct ServerState {
    id_generator: IdGenerator,
    //list of writers
    clients: DashMap<u64, UnboundedSender<Message>>,
    chat_rooms: DashMap<String, ChatRoom>,
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            id_generator: IdGenerator::new(),
            clients: DashMap::new(),
            chat_rooms: DashMap::new(),
        }
    }

    pub fn create_room(&self, name: String) -> anyhow::Result<()> {
        match self.chat_rooms.entry(name) {
            dashmap::Entry::Occupied(_) => {
                anyhow::bail!("chat room name is already taken");
            }
            dashmap::Entry::Vacant(entry) => {
                let id = self.id_generator.next_room_id();
                entry.insert(ChatRoom::new(id));
            }
        };
        Ok(())
    }

    pub fn remove_room(&self, name: &str) {
        self.chat_rooms
            .remove(name)
            .expect("room should be present");
    }

    pub fn create_client(&self, sender: UnboundedSender<Message>) -> u64 {
        let client_id = self.id_generator.next_client_id();
        self.clients.insert(client_id, sender);
        client_id
    }

    pub fn remove_client(&self, client_id: u64) {
        self.clients
            .remove(&client_id)
            .expect("client should exist");
    }
}
