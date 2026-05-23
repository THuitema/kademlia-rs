use std::collections::HashMap;
use std::net::{UdpSocket, SocketAddr};
use serde_cbor::from_slice;
use std::time::{Instant, Duration};
use crate::id::Id;
use crate::routing::{AddContactResult, RoutingTable};
use crate::protocol::{Packet};
use crate::rpc::{send_ping, handle_ping};
use crate::contact::Contact;

const MAX_PACKET_SIZE: usize = 1200;
const K_DEFAULT: usize = 20;
const RECV_TIMEOUT: Duration = Duration::from_secs(1); // so recv in listen loop blocks for a max of 1 sec
const REQ_TIMEOUT: Duration = Duration::from_secs(10); // timeout on pending requests
 
pub enum PendingRequest {
    Ping { recipient: Contact, sent_at: Instant },
    EvictionCheck { candidate: Contact, recipient: Contact, sent_at: Instant } // candidate is the contact we'd add if the least recently seen times out
}

pub struct KademliaNode {
    pub id: Id,
    pub routing_table: RoutingTable,
    pub k: usize,
    pub socket: UdpSocket,
    pub pending_requests: HashMap<Id, PendingRequest> // key is the nonce
}

impl KademliaNode {
    // If id is None, random one is generated
    // if k is None, default value is used (20)
    // Returns Err if can't bind socket to node_addr
    pub fn new(node_addr: SocketAddr, id: Option<Id>, k: Option<usize>) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(node_addr)?;
        socket.set_read_timeout(Some(RECV_TIMEOUT))?;

        let id = id.unwrap_or_else(Id::generate_id);
        let k = k.unwrap_or(K_DEFAULT);

        Ok(Self {
            id,
            routing_table: RoutingTable::new(id, k),
            k,
            socket,
            pending_requests: HashMap::new()
        })
    }

    pub fn listen(&mut self) {
        loop {
            self.check_pending_requests();

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

            // add/update node in routing table
            let sender_id = packet.header().sender_id;
            let nonce = packet.header().nonce;
            let sender_contact = Contact { addr: src_addr, id: sender_id };

            match self.routing_table.add_contact(sender_contact) {
                AddContactResult::PingRequired(least_recently_seen) => {
                    // ping contact
                    // if they respond, move them to tail
                    // if they timeout, evict them and add this contact
                    let evict_check_nonce = Id::generate_id();
                    match send_ping(&self.socket, least_recently_seen.addr, self.id, evict_check_nonce) {
                        Ok(()) => {
                            // add to our pending requests map
                            self.pending_requests.insert(evict_check_nonce, PendingRequest::EvictionCheck { candidate: sender_contact, recipient: least_recently_seen, sent_at: Instant::now() });
                        },
                        Err(_) => {
                            // evict and add contact if we couldn't even send the ping
                            self.routing_table.evict(least_recently_seen);
                            self.routing_table.add(sender_contact);
                        }
                    }
                },
                _ => {}
            }

            match packet {
                Packet::PingRequest(req) => {
                    println!("[listen] ping request received!");
                    handle_ping(&self.socket, src_addr, req, self.id).unwrap();
                },
                Packet::PingResponse(res) => {
                    println!("[listen] ping response received!");

                    // Find corresponding pending request and update peer
                    let pending_req = self.pending_requests.remove(&nonce);
                    match pending_req {
                        Some(PendingRequest::Ping { recipient: r, .. }) => {
                            if r.id != sender_id {
                                eprintln!("[listen] PingResponse sender ID mismatch");
                                return;
                            }
                            self.routing_table.evict(r);
                            self.routing_table.add(r);
                        },
                        Some(PendingRequest::EvictionCheck { recipient: r, .. }) => {
                            if r.id != sender_id {
                                eprintln!("[listen] PingResponse sender ID mismatch");
                                return;
                            }
                            self.routing_table.evict(r);
                            self.routing_table.add(r);
                        },
                        None => {
                            eprintln!("[listen] received PingResponse with no matching pending request for nonce");
                        }
                    }
                },
                Packet::FindNodeRequest(req) => {
                    // TODO
                },
                Packet::FindNodeResponse(res) => {
                    // TODO
                }
                _ => eprintln!("[listen] HANDLING FOR PACKET TYPE NOT IMPLEMENTED!")
            }
        }
    }

    // loop through all pending requests and handle timeouts
    pub fn check_pending_requests(&mut self) {
        let mut remove_nonces: Vec<Id> = Vec::new();

        for req in self.pending_requests.iter() {
            match req.1 {
                PendingRequest::Ping { recipient: r, sent_at: t } => {
                    // if ping times out, evict the recipient
                    if t.elapsed() > REQ_TIMEOUT {
                        self.routing_table.evict(*r);
                        remove_nonces.push(*req.0);
                    }
                },
                PendingRequest::EvictionCheck { candidate: c, recipient: r, sent_at: t } => {
                    // if eviction check times out, evict the recipient and add the candidate
                    if t.elapsed() > REQ_TIMEOUT {
                        self.routing_table.evict(*r);
                        self.routing_table.add(*c);
                        remove_nonces.push(*req.0);
                    }
                }
            }
        }

        // loop through and remove timed-out pending requests
        for n in remove_nonces {
            self.pending_requests.remove(&n);
        }
    }
}
