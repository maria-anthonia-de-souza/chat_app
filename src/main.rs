use chat_app::server::Server;
use chat_app::state::*;
use tokio::net::TcpListener;

#[tokio::main] //creates async runtime 
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:2345").await?;
    let state = ServerState::new(); 
    Server::new(listener, state).run().await
}
