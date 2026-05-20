use std::net::Ipv4Addr;
use crate::id::Id;

// Represents another node stored inside the k-buckets
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Contact {
    pub ip: Ipv4Addr,
    pub port: u16,
    pub id: Id,
}