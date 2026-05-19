use std::net::Ipv4Addr;

// Represents another node stored inside the k-buckets
pub struct Contact {
    pub ip: Ipv4Addr,
    pub port: u16,
    pub id: Id,
}