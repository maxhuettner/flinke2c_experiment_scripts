use std::collections::HashMap;

use petgraph::{Graph, graph::NodeIndex};
use serde::Deserialize;
use tokio::fs;
use validator::Validate;

use crate::graph::{topo_edge::TopoEdge, topo_node::TopoNode};

pub mod topo_edge;
pub mod topo_node;

#[derive(Debug, Deserialize)]
pub struct TopologyConfig {
    nodes: Vec<TopoNode>,
    edges: Vec<TopologyConfigEdge>,
}

#[derive(Debug, Deserialize)]
struct TopologyConfigEdge {
    source: String,
    target: String,
    weight: TopoEdge,
}

pub async fn build_graph_from_file(file_name: &str) -> Graph<TopoNode, TopoEdge> {
    let topology_config_file = fs::read(file_name).await.unwrap();
    let topology_config: TopologyConfig = serde_json::from_slice(&topology_config_file).unwrap();
    build_graph(topology_config)
}

pub fn build_graph(config: TopologyConfig) -> Graph<TopoNode, TopoEdge> {
    let mut graph = Graph::<TopoNode, TopoEdge>::new();
    let mut node_mapping = HashMap::<String, NodeIndex>::new();

    for node in config.nodes {
        node.validate().unwrap();
        let node_id = node.id.clone();
        let node_idx = graph.add_node(node);
        node_mapping.insert(node_id, node_idx);
    }

    for edge in config.edges {
        let source = edge.source;
        let target = edge.target;
        let source_idx = *node_mapping.get(&source).expect("source must be in graph");
        let target_idx = *node_mapping.get(&target).expect("target must be in graph");
        graph.add_edge(source_idx, target_idx, edge.weight);
    }

    graph
}
