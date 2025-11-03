use std::env;

use anyhow::Result;
use dotenv::dotenv;
use tokio::io::AsyncWriteExt;

use crate::{graph::build_graph_from_file, ssh::connect};

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

    let mut rx = session.send_command("cat /etc/os-release").await?;

    let mut stdout = tokio::io::stdout();
    while let Some(ref data) = rx.recv().await {
        stdout.write_all(data.as_bytes()).await?;
        stdout.flush().await?;
    }

    println!(
        "Loaded topology {:?} with {} nodes and {} edges",
        graph,
        graph.node_count(),
        graph.edge_count()
    );
    Ok(())
}
