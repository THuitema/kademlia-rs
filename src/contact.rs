use std::net::Ipv4Addr;
use serde::{Deserialize, Serialize};
use crate::id::Id;

// Represents another node stored inside the k-buckets
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Debug)]
pub struct Contact {
    pub ip: Ipv4Addr,
    pub port: u16,
    pub id: Id,
}