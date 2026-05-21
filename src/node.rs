use std::net::{UdpSocket, SocketAddr};
use serde_cbor::from_slice;
use crate::id::Id;
use crate::routing::RoutingTable;
use crate::protocol::{Packet};
use crate::rpc::{send_ping, handle_ping};

const MAX_PACKET_SIZE: usize = 1200;
const K_DEFAULT: usize = 20;

pub struct KademliaNode {
    pub id: Id,
    pub routing_table: RoutingTable,
    pub k: usize,
    pub socket: UdpSocket,
}

impl KademliaNode {
    // If id is None, random one is generated
    // if k is None, default value is used (20)
    // Returns Err if can't bind socket to node_addr
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
                    handle_ping(&self.socket, src_addr, req, self.id).unwrap();
                },
                Packet::PingResponse(res) => {
                    println!("[listen] ping response received!");
                },
                _ => eprintln!("[listen] HANDLING FOR PACKET TYPE NOT IMPLEMENTED!")
            }
        }
    }
}
