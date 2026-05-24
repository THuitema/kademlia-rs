use std::collections::HashMap;
use std::hash::Hash;
use std::net::{UdpSocket, SocketAddr};
use std::ops::Shl;
use serde_cbor::from_slice;
use std::time::{Instant, Duration};
use crate::id::Id;
use crate::routing::{AddContactResult, RoutingTable};
use crate::protocol::{Packet};
use crate::rpc::{handle_find_node, handle_ping, send_find_node, send_ping};
use crate::contact::Contact;
use crate::lookup::{self, NodeLookup};

const MAX_PACKET_SIZE: usize = 1200;
const K_DEFAULT: usize = 20;
const RECV_TIMEOUT: Duration = Duration::from_secs(1); // so recv in listen loop blocks for a max of 1 sec
const REQ_TIMEOUT: Duration = Duration::from_secs(10); // timeout on pending requests
const ALPHA: usize = 3;
const LOOKUP_ROUND_PERIOD: Duration = Duration::from_millis(500);
 
pub enum PendingRequest {
    Ping { recipient: Contact, sent_at: Instant },

    /*
    used for a ping to a head (least recently seen contact) of a full bucket when we want to add a contact
    candidate is the contact we'd add if the least recently seen times out
    recipient is the least recently seen node that's being pinged
     */
    EvictionCheck { candidate: Contact, recipient: Contact, sent_at: Instant },

    /*
    target is the key we're searching for
     */
    FindNode { target: Id, recipient: Contact, sent_at: Instant },
}

pub struct KademliaNode {
    pub id: Id,
    pub routing_table: RoutingTable,
    pub k: usize,
    pub socket: UdpSocket,
    pub pending_requests: HashMap<Id, PendingRequest>, // key is the nonce
    pub active_lookups: HashMap<Id, NodeLookup>,
    pub completed_lookups: HashMap<Id, Vec<Contact>>,
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
            pending_requests: HashMap::new(),
            active_lookups: HashMap::new(),
            completed_lookups: HashMap::new(),
        })
    }

    pub fn listen(&mut self) {
        loop {
            self.check_pending_requests();
            self.check_active_lookups();

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
                        _ => {
                            eprintln!("[listen] received PingResponse with no matching pending request for nonce");
                        }
                    }
                },
                Packet::FindNodeRequest(req) => {
                    handle_find_node(&self.socket, src_addr, req, self.id, &self.routing_table).unwrap();
                },
                Packet::FindNodeResponse(res) => {
                    // remove pending request
                    self.pending_requests.remove(&nonce);

                    if let Some(lookup_state) = self.active_lookups.get_mut(&res.target_id) {
                        lookup_state.pending.remove(&sender_id);

                        for contact in res.contacts {
                            // if this node is in contact list, remove that entry to avoid circularity
                            if contact.id == self.id {
                                continue;
                            }

                            // update closest_node
                            if contact.id.distance(res.target_id) < lookup_state.closest_node.id.distance(res.target_id) {
                                lookup_state.closest_node = contact;
                            }

                            // add to shortlist if not there
                            if !lookup_state.shortlist.iter().any(|c| c.id == contact.id) {
                                lookup_state.shortlist.push(contact);
                            }
                        }

                        // re-sort and re-truncate shortlist to limit to k elements
                        lookup_state.shortlist.sort_by_key(|c| c.id.distance(res.target_id));
                        lookup_state.shortlist.truncate(self.k);
                    }
                }
                _ => eprintln!("[listen] HANDLING FOR PACKET TYPE NOT IMPLEMENTED!")
            }
        }
    }

    pub fn lookup(&mut self, target: Id) {
        // find alpha closest contacts to target
        let closest_contacts = self.routing_table.get_closest_contacts(target, ALPHA);
        let mut lookup_state = NodeLookup::new(target, closest_contacts);

        let to_query: Vec<Contact> = lookup_state.shortlist.iter()
            .take(ALPHA)
            .copied()
            .collect();

        // send FIND_NODE messages to each of the alpha nodes (add PendingRequest::FindNode for each)
        for contact in to_query {
            let nonce = Id::generate_id();
            send_find_node(&self.socket, contact.addr, self.id, nonce, target).unwrap();
            self.pending_requests.insert(
                nonce, 
                PendingRequest::FindNode { target, recipient: contact, sent_at: Instant::now() }
            );

            // update NodeLookup state with pending nodes
            lookup_state.pending.insert(contact.id);
            lookup_state.queried.insert(contact.id);
        }

        // create new entry in active lookups
        self.active_lookups.insert(target, lookup_state);
    }

    pub fn check_active_lookups(&mut self) {
        let mut remove_lookups: Vec<Id> = Vec::new();

        for (target, lookup_state) in self.active_lookups.iter_mut() {
            // Check if current round is over
            if lookup_state.last_round_at.elapsed() > LOOKUP_ROUND_PERIOD {
                // check if termination condition hit
                if lookup_state.closest_node.id == lookup_state.old_closest_node.id && lookup_state.pending.is_empty() {
                    self.completed_lookups.insert(*target, lookup_state.shortlist.clone());
                    remove_lookups.push(*target);
                    continue;
                }

                lookup_state.last_round_at = Instant::now();
                lookup_state.old_closest_node = lookup_state.closest_node;

                let to_query: Vec<Contact> = lookup_state.shortlist.iter()
                    .filter(|c| !lookup_state.queried.contains(&c.id))
                    .take(ALPHA)
                    .copied()
                    .collect();

                // send FIND_NODE messages to each of the alpha nodes (add PendingRequest::FindNode for each)
                for contact in to_query {
                    let nonce = Id::generate_id();
                    send_find_node(&self.socket, contact.addr, self.id, nonce, *target).unwrap();
                    self.pending_requests.insert(
                        nonce, 
                        PendingRequest::FindNode { target: *target, recipient: contact, sent_at: Instant::now() }
                    );

                    // update NodeLookup state with pending nodes
                    lookup_state.pending.insert(contact.id);
                    lookup_state.queried.insert(contact.id);
                }
            }
        }

        for id in remove_lookups {
            self.active_lookups.remove(&id);
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
                },
                PendingRequest::FindNode { target: k, recipient: r, sent_at: t } => {
                    // if find node times out, evict recipient and remove from lookup shortlist if it's there
                    if t.elapsed() > REQ_TIMEOUT {
                        self.routing_table.evict(*r);
                        if let Some(lookup_state) = self.active_lookups.get_mut(k) {
                            lookup_state.pending.remove(&r.id);
                            if let Some(pos) = lookup_state.shortlist.iter().position(|c| c.id == r.id) {
                                lookup_state.shortlist.remove(pos);
                            }
                        }
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
