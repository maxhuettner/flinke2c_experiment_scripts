use std::{env, time::Duration};

use ::tracing::{info, info_span, level_filters::LevelFilter};
use anyhow::Result;
use dotenv::dotenv;
use russh::Sig;
use tokio::{io::AsyncWriteExt, task::{self, JoinHandle}, time::sleep};

use crate::{
    graph::{build_graph_from_file, topo_node::TopoNode},
    ssh::{comm_session::SessionCommand, connect},
    tracing::init_tracing,
};

mod graph;
mod ssh;
mod tracing;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let log_level = env::var("LOG_LEVEL")
        .ok()
        .and_then(|val| val.parse::<LevelFilter>().ok())
        .unwrap_or(LevelFilter::INFO);

    init_tracing(log_level);

    let graph = build_graph_from_file("config/topologies/1.json").await;
    let node = graph.raw_nodes().iter().map(|node| node.weight.clone()).collect::<Vec<TopoNode>>();

    let user = env::var("SSH_USER").expect("ssh user required");
    let key_path = env::var("SSH_KEY_PATH").expect("ssh key path required");
    let pass_phrase = env::var("SSH_KEY_PASSPHRASE").ok();

    let node_2 = node.get(1).unwrap().clone();
    let user_2 = user.clone();
    let pass_phrase2 = pass_phrase.clone();
    let key_path2 = key_path.clone();
    let new_sess: JoinHandle<anyhow::Result<()>> = tokio::task::spawn(async move {
        let session = connect(node_2.address.as_str(), &user_2, pass_phrase2.as_deref(), &key_path2).await?;
        let session = session.start_session().await?;

        let session_cmds = session.command_tx();
        let session_resps = session.response_rx();

        session_cmds.send(SessionCommand::Data("cat /etc/os-release".to_string()))?;

        if let Ok(data) = session_resps.recv().await {
            info!(%data);
        }

        Ok(())
    });

    let session = connect(node.first().unwrap().address.as_str(), &user, pass_phrase.as_deref(), &key_path).await?;

    let session = session.start_session().await?;

    let session_cmds = session.command_tx();
    let session_resps = session.response_rx();

    session_cmds.send(SessionCommand::Data("tail -f /etc/os-release".to_string()))?;

    if let Ok(data) = session_resps.recv().await {
        info!(%data);
    }

    let session_cmds_clone = session_cmds.clone();
    task::spawn(async move {
        sleep(Duration::from_secs(5)).await;
        session_cmds_clone.send(SessionCommand::Signal(Sig::INT)).unwrap();
    });

    if let Ok(data) = session_resps.recv().await {
        info!(%data);
    }

    sleep(Duration::from_secs(1)).await;

    session.stop().await?;
    new_sess.await??;

    info!(
        "Loaded topology {:?} with {} nodes and {} edges",
        graph,
        graph.node_count(),
        graph.edge_count()
    );
    Ok(())
}
