use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum TopoNodeType {
    Source,
    Sink,
    Compute,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Validate, Clone)]
pub struct TopoNode {
    pub id: String,
    pub node_type: TopoNodeType,

    #[validate(range(min = 1, max = 100))]
    pub speed: Option<u32>,

    pub address: String,
}

impl TopoNode {
    pub fn is_ss(&self) -> bool {
        self.node_type == TopoNodeType::Source || self.node_type == TopoNodeType::Sink
    }
}
