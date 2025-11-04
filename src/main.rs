use std::{env, sync::Arc, time::Duration};

use anyhow::Result;
use dotenv::dotenv;
use russh::Sig;
use tokio::{io::AsyncWriteExt, task, time::sleep};

use crate::{graph::build_graph_from_file, ssh::{comm_session::SessionCommand, connect}};

mod graph;
mod ssh;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let graph = build_graph_from_file("config/topologies/1.json").await;
    let node = graph.raw_nodes().iter().map(|node| node.weight.clone()).next().unwrap();

    let pass_phrase = env::var("SSH_KEY_PASSPHRASE").ok();
    let session = connect(
        node.address.as_str(),
        "mhuttner",
        pass_phrase.as_deref(),
        "~/.ssh/id_ed25519",
    )
    .await?;

    let session = session.start_session().await?;

    let session_cmds = session.command_tx();
    let session_resps = session.response_rx();

    session_cmds.send(SessionCommand::Data("tail -f /etc/os-release".to_string()))?;

    let mut stdout = tokio::io::stdout();
    if let Ok(data) = session_resps.recv().await {
        stdout.write_all(data.as_bytes()).await?;
        stdout.flush().await?;
    }

    let session_cmds_clone = session_cmds.clone();
    task::spawn(async move {
        sleep(Duration::from_secs(5)).await;
        session_cmds_clone.send(SessionCommand::Signal(Sig::INT)).unwrap();
    });

    if let Ok(data) = session_resps.recv().await {
        stdout.write_all(data.as_bytes()).await?;
        stdout.flush().await?;
    }

    session.stop().await?;

    println!(
        "Loaded topology {:?} with {} nodes and {} edges",
        graph,
        graph.node_count(),
        graph.edge_count()
    );
    Ok(())
}
