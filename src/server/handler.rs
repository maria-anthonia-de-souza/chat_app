use crate::state::{ClientReader, ClientWriter, Incoming, Outgoing, ServerState};
use anyhow;
use axum::{
    extract::{
        ConnectInfo, WebSocketUpgrade,
        ws::{Message, Utf8Bytes, WebSocket},
    },
    response::IntoResponse,
};
use axum_extra::{TypedHeader, headers};
use futures::StreamExt;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::UnboundedReceiver;

/// The handler for the HTTP request (this gets called when the HTTP request lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser etc.
pub(super) async fn ws_handler(
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    state: axum::extract::State<Arc<ServerState>>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("`{user_agent}` at {addr} connected.");
    // finalize the upgrade process by returning upgrade callback.
    // we can customize the callback by sending additional info such as address.
    ws.on_upgrade(move |socket| handle_socket(socket, addr, state.0))
}
///Per connection task, axum runs one for each connected client, sets up connection by spliting WS into r, w, sets up multi-prod,
///single consumer chanel, registers, and then select!
async fn handle_socket(socket: WebSocket, _who: SocketAddr, state: Arc<ServerState>) {
    let (writer, reader) = socket.split();
    let mut writer = ClientWriter::new(writer);
    let mut reader = ClientReader::new(reader);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let client_id = state.create_client(tx);
    let username = format!("user_{client_id}");
    let mut current_room: Option<String> = None; //what it means #TODO

    loop {
        // Awaiting 2 futures at the same time
        tokio::select! {
            res = send_messages_to_client(&mut rx, &mut writer) => {
                match res {
                    Ok(true) => {},
                    Ok(false) | Err(_) => break,
                }},
            res = receive_messages_from_client(&mut reader, &state, client_id, &username, &mut current_room) => {
                match res {
                    Ok(true) => {},
                    Ok(false) | Err(_) => break,
                }
            }
        }
    }
    if let Some(room) = current_room {
        state.leave_room(&room, client_id);
    }
    state.remove_client(client_id);
}

///Reads from clients channel receiver, and writes to the clients websocket
async fn send_messages_to_client(
    receiver: &mut UnboundedReceiver<Message>,
    client: &mut ClientWriter,
) -> anyhow::Result<bool> {
    match receiver.recv().await {
        // send message to client
        Some(msg) => {
            client.send(msg).await?;
            Ok(true)
        }
        None => Ok(false),
    }
}
///Handle one message coming from client per call and will signal to handle socket whether to keep looping or not
async fn receive_messages_from_client(
    reader: &mut ClientReader,
    state: &ServerState,
    client_id: u64,
    username: &str,
    current_room: &mut Option<String>,
) -> anyhow::Result<bool> {
    match reader.recv().await? {
        Some(Message::Text(text)) => {
            //json string -> rust struct
            let incoming: Incoming = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => {
                    state.send_to_client(client_id, Message::Text("invalid JSON".into()));
                    return Ok(true);
                }
            };

            //handle what type of message the text contained: a join room
            match incoming {
                Incoming::Join { room } => {
                    //leave prev room first (only 1 atp)
                    if let Some(old) = current_room.take() {
                        state.leave_room(&old, client_id);
                    }
                    state.join_room(room.clone(), client_id);
                    //why are we cloning
                    *current_room = Some(room.clone());
                    state.send_to_client(client_id, Message::Text(format!("joined {room}").into()));
                }
                //handle chat message
                Incoming::Message { content } => {
                    if content.trim().is_empty() {
                        state.send_to_client(client_id, Message::Text(Utf8Bytes::from_static("Empty message")),);
                        return Ok(true);
                    }
                    //can only chat if you've joined a room
                    //if val matches room bind room and carry on, else
                    let Some(room) = current_room.as_deref() else {
                        //reach through the option and borrow room name
                        state.send_to_client(
                            client_id,
                            Message::Text(Utf8Bytes::from_static("Join room first")),
                        );
                        return Ok(true);
                    };
                    //rust struct -> json string
                    let outgoing = Outgoing {
                        sender: username.to_string(),
                        content,
                    };
                    let out = serde_json::to_string(&outgoing)?;
                    state.broadcast_to_room(room, client_id, Message::Text(out.into()));
                }
            }

            Ok(true) //signal to keep looping
        }

        Some(_) => Ok(true), // non-text frames (binary, ping, pong) — ignore
        None => Ok(false),
    }
}
