use std::net::{UdpSocket, SocketAddr}; 
use serde_cbor::from_slice;
use crate::id::Id;
use crate::routing::RoutingTable;
use crate::protocol::{Packet};

const MAX_PACKET_SIZE: usize = 1024;
const K_DEFAULT: usize = 20;

struct KademliaNode {
    pub id: Id,
    pub routing_table: RoutingTable,
    pub k: usize,
    pub socket: UdpSocket,
}

impl KademliaNode {
    // Creates new KademliaNode with given id if Some, generates random id if None
    // Returns Err if couldn't bind socket to node_addr
    pub fn new(node_addr: SocketAddr, id: Option<Id>, k: Option<usize>) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(node_addr)?;
        let id = id.unwrap_or_else(Id::generate_id);
        let k = k.unwrap_or(K_DEFAULT);

        Ok(Self {
            id,
            routing_table: RoutingTable::new(id, k),
            k,
            socket
        })
    }

    pub fn listen(&self) {
        loop {
            let mut buffer = [0u8; MAX_PACKET_SIZE];
            let (num_bytes, src_addr) = match self.socket.recv_from(&mut buffer) {
                Ok(x) => x,
                Err(e) => {
                    eprintln!("[listen] recv_from failed: {e}");
                    continue;
                }
            };

            let packet: Packet = match from_slice(&buffer[..num_bytes]) {
                Ok(x) => x,
                Err(e) => {
                    eprintln!("[listen] failed to deserialize packet: {e}");
                    continue;
                }
            };

            println!("[listen] read {num_bytes} bytes!");

            match packet {
                Packet::PingRequest(req) => {
                    println!("[listen] ping request received!");
                },
                Packet::PingResponse(res) => {
                    println!("[listen] ping response received!");
                },
                _ => eprintln!("[listen] HANDLING FOR PACKET TYPE NOT IMPLEMENTED!")
            }
        }
    }
}
