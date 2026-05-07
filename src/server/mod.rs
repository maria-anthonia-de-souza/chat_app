mod handler;

use std::{os::macos::raw::stat, sync::Arc};

use axum::{Router, routing::any};
use tokio::net::{TcpListener, TcpStream};

use crate::{server::handler::ws_handler, state::ServerState};

pub struct Server {
    listener: TcpListener,
    state: Arc<ServerState>,
}

impl Server {
    pub fn new(listener: TcpListener, state: ServerState) -> Self {
        Self {
            listener,
            state: Arc::new(state),
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let app = Router::new()
            .route("/ws", any(ws_handler))
            .with_state(self.state);
        axum::serve(self.listener, app).await;
        Ok(())
    }
}
