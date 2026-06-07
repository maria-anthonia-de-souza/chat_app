mod handler;

use std::sync::Arc;

use axum::{Router, routing::any};
use std::net::SocketAddr;
use tokio::net::TcpListener;

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

        //wires the per-connection client socketAddr into the request, which is what ConnectInfo<SockerAddr> in ws_handler reaches for
        //make-service: for each incoming tcp connection, produces a service to handle that one connection 
        axum::serve(
            self.listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;
        Ok(())
    }
}
