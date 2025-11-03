use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, PartialEq, Serialize, Deserialize, Validate)]
pub struct TopoEdge {
    pub speed: Option<u32>,
}
