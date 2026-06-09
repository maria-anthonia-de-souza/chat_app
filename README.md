# chat_app
A multi chat room WebSocket chat server written in Rust with Tokio and Axum. You can join rooms, create new rooms and broadcast messages. 

## Features

- **Multi-room chat** — clients join named rooms and only exchange messages with others in the same room.
- **Create-on-join** — joining a room that doesn't exist creates it; joining one that does drops you straight in. Rooms are deleted automatically once the last member leaves.
- **Server-assigned usernames** — each connection gets a unique name (`user_0`, `user_1`, …) that the server stamps onto outgoing messages, so clients can't impersonate one another.
- **Concurrent by design** — every connection runs as its own async task, and shared state uses sharded concurrent maps so independent connections don't block each other.
- **JSON message protocol** — structured messages cleanly distinguish "join a room" from "send a chat message."
- **Input validation** — malformed JSON, empty messages, and attempting to chat before joining a room each get a clear reply instead of being silently dropped.

## Tech stack

- [Tokio](https://tokio.rs/) — async runtime
- [Axum](https://github.com/tokio-rs/axum) — HTTP / WebSocket server
- [serde](https://serde.rs/) / serde_json — JSON serialization and deserialization


## Add ons 

- [Rust and Cargo](https://www.rust-lang.org/tools/install)
- [websocat](https://github.com/vi/websocat) — for testing from the command line (`brew install websocat` on macOS)

## Build and run

```bash
cargo run
```

The server binds to `127.0.0.1:2345` and exposes a WebSocket endpoint at `/ws`.

## Usage

Connect a client:

```bash
websocat ws://127.0.0.1:2345/ws
```

The protocol is JSON. There are two kinds of message a client sends **to** the server.

Join (or create) a room:

```json
{"type":"join","room":"general"}
```

Send a chat message to your current room:

```json
{"type":"message","content":"hello"}
```

Chat messages are delivered **to** the other clients in the same room as:

```json
{"sender":"user_0","content":"hello"}
```

The sender does not receive an echo of their own message.

### Trying it out

Open three terminals, each running `websocat ws://127.0.0.1:2345/ws`.

1. In two of them, send `{"type":"join","room":"general"}`.
2. In the third, send `{"type":"join","room":"random"}`.
3. From one of the "general" clients, send `{"type":"message","content":"hi"}`.

The other "general" client receives the message; the "random" client does not.

### Server replies

The server sends short status replies back to the sending client:

| Situation                       | Reply           |
| ------------------------------- | --------------- |
| Joined a room                   | `joined <room>` |
| Sent invalid JSON               | `invalid JSON`  |
| Sent an empty message           | `Empty message` |
| Chatted before joining a room   | `Join room first` |

## Project structure

```
src/
├── main.rs           Entry point — binds the listener and starts the server
├── lib.rs            Exposes the server and state modules
├── state.rs          Shared server state: clients, rooms, and message types
└── server/
    ├── mod.rs        The Server struct and the Axum router
    └── handler.rs    WebSocket upgrade and the per-connection message loop
```

## How it works

Each client connection is handled by its own async task. On connect, the client is registered in a shared `ServerState` and given a per-connection channel. Messages flow in two directions independently:

- **Incoming:** the task reads frames from the client's socket, parses them, and either joins a room or broadcasts a chat message to the client's current room.
- **Outgoing:** when another client broadcasts to a room, the message is pushed into each recipient's channel; each connection's task drains its channel and writes to its own socket.

This channel-based design decouples senders from receivers — a client broadcasting a message doesn't touch anyone else's socket directly, it just drops the message into their channel and moves on.

## Possible future work

- Let clients choose their own username
- Notify a room when someone joins or leaves
- A command to list who's currently in a room
- Persistent message history per room
- Switch from unbounded to bounded channels for backpressure under load
