use futures::stream::SplitSink;
use futures_util::SinkExt;
use futures_util::StreamExt;
use std::io;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

//convert TCP stream into websocket -> read message -> print
async fn process_socket(
    socket: TcpStream,
    clients: Arc<Mutex<Vec<SplitSink<WebSocketStream<TcpStream>, Message>>>>,
) {
    //take tcp and try to transform into websocket
    let ws_stream = match accept_async(socket).await {
        Ok(ws) => ws,
        Err(e) => {
            println!("WebSocket handshake failed: {:?}", e);
            return;
        }
    };
    //splits ws stream into a read and a write, write will push messages into vec and sending. reader will loop over messages

    let (writer, mut reader) = ws_stream.split();
    //locks mutex, returns result(MutexGuard) which gives access to the Vec, then push writer into vec
    //registers the clients to shared list
    //since this tokio mutex is made for async use, use await to lock

    let mut guard = clients.lock().await;
    guard.push(writer);
    //get the index of each client
    let index = guard.len() - 1;
    drop(guard);

    //looping forever, reading incoming messages
    while let Some(msg) = reader.next().await {
        match msg {
            Ok(msg) => {
                println!("Received: {:?}", msg);
                match msg {
                    //retriving message string using format
                    Message::Text(text) => {
                        let response = format!("client said: {}", text);

                        //handle sending same messages back
                        //lock clients and access vec
                        let mut guard = clients.lock().await;

                        //loop through and modify each writer
                        for writer in guard.iter_mut() {
                            //sending message and handling if failed sending message
                            if let Err(e) = writer
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    response.clone().into(),
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

    //cleanup and remove clients that are not connected anymore with swap_remove to bring the index to the last one in the list and remove 
    let _ = clients.lock().await.swap_remove(index);
}

#[tokio::main] //creates async runtime 
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:2345").await?;

    //vector that contains clients for broadcasting messages
    let v_clients: Arc<Mutex<Vec<SplitSink<WebSocketStream<TcpStream>, Message>>>> =
        Arc::new(Mutex::new(Vec::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        //cloning creates a new handle pointing to the same list so every task shares the same data without consuming it
        let clients = Arc::clone(&v_clients);
        //passing clients so that each task can have access to the shared list and add its own writer and broadcast their message. no isolation.
        tokio::spawn(process_socket(socket, clients)); //enables concurrency, dont have to wait to accept next client 
    }
}
