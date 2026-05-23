use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use crate::id::Id;

// Represents another node stored inside the k-buckets
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Debug)]
pub struct Contact {
    pub addr: SocketAddr,
    pub id: Id,
}