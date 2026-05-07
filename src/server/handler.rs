use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{
        ConnectInfo, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::IntoResponse,
};
use axum_extra::{TypedHeader, headers};
use futures::StreamExt;
use tokio::{net::TcpStream, sync::mpsc::UnboundedReceiver};
use tokio_tungstenite::accept_async;

use crate::state::{ClientReader, ClientWriter, ServerState};

//convert TCP stream into websocket -> read message -> print
async fn process_socket(socket: TcpStream, state: Arc<ServerState>) {
    //take tcp and try to transform into websocket
    let ws_stream = match accept_async(socket).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("WebSocket handshake failed: {:?}", e);
            return;
        }
    };

    //splits ws stream into a read and a write, write will push messages into hashmap and sending. reader will loop over messages

    let (writer, reader) = ws_stream.split();
    let writer = ClientWriter::new(writer);
    let reader = ClientReader::new(reader);

    let client_id = state.create_client(writer, reader);

    //locks mutex, returns result(MutexGuard) which gives access to the Vec, then push writer into vec
    //registers the clients to shared list
    //since this tokio mutex is made for async use, use await to lock

    let mut guard = clients.lock().await;
    guard.writers.insert(id, writer);
    drop(guard);

    //first message = username

    //username becomes message
    let username_msg = match reader.next().await {
        Some(Ok(msg)) => msg,
        _ => return,
    };

    //message -> string
    let username = match username_msg {
        Message::Text(text) => text.trim().to_string(),
        _ => return,
    };

    //looping forever, reading incoming messages
    //client sends json string
    //retriving message string
    while let Some(msg) = reader.next().await {
        match msg {
            Ok(msg) => {
                println!("Received: {:?}", msg);
                match msg {
                    Message::Text(text) => {
                        //deserializes json text into rust struct (Incoming)
                        let incoming: Incoming = match serde_json::from_str(text.as_str()) {
                            Ok(parsed) => parsed,
                            Err(e) => {
                                //log the error
                                println!("{:?}", e);
                                //lock clients list
                                let mut guard = clients.lock().await;
                                // grab this clients by id
                                if let Some(writer) = guard.writers.get_mut(&id) {
                                    //send error to user
                                    let _ = writer.send(Message::Text("invalid JSON".into())).await;
                                }
                                //wait for next message
                                continue;
                            }
                        };

                        // instance of outgoing

                        let outgoing = Outgoing {
                            //.clone() allocates a fresh String with the same contents, hands that to the struct
                            sender: username.clone().to_string(), //independent copy of string so username is not gone and can be used in next iteration
                            content: incoming.content,
                        };

                        //serialize outgoing
                        let out = serde_json::to_string(&outgoing).unwrap();

                        //handle sending same messages back
                        //lock clients and access vec
                        let mut guard = clients.lock().await;
                        //which room this id is in
                        let Some(room_name) = guard.room_client.get(&id) else {
                            println!("No room found, exiting.");
                            return;
                        };
                        //list of ids in the room
                        let Some(ids) = guard.chat_room.get(room_name) else {
                            println!("No ids found, exiting.");
                            return;
                        };

                        //loop through and grab each writer
                        for (_, writer) in guard.chat_room.iter_mut() {
                            //sending serialized json to that client and handling if failed sending message
                            if let Err(e) = writer
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    out.clone().into(),
                                ))
                                .await
                            //cloning response for each writer so it does not get consumed by one only
                            {
                                println!("Error sending message: {:?}", e);

                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
            Err(e) => {
                println!("Error receiving messages: {:?}", e);
                break;
            }
        }
    }

    //remove this client from the shared list on disconnect
    let _ = clients.lock().await.remove(&id);
}

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

async fn handle_socket(mut socket: WebSocket, who: SocketAddr, state: Arc<ServerState>) {
    let (writer, reader) = socket.split();
    let mut writer = ClientWriter::new(writer);
    let mut reader = ClientReader::new(reader);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let client_id = state.create_client(tx);

    loop {
        // Awaiting 2 futures at the same time
        tokio::select! {
            res = send_messages_to_client(&mut rx, &mut writer) => {},
            res = receive_messages_from_client(&mut reader) => {}
        }
    }
}

async fn send_messages_to_client(
    receiver: &mut UnboundedReceiver<Message>,
    client: &mut ClientWriter,
) -> anyhow::Result<()> {
    if let Some(msg) = receiver.recv().await {
        // send message to client
        client.send(msg).await?;
    }
    Ok(())
}

async fn receive_messages_from_client(reader: &mut ClientReader) -> anyhow::Result<()> {
    if let Some(msg) = reader.recv().await? {}
    Ok(())
}
