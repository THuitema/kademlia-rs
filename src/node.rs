use std::collections::HashMap;
use std::net::{UdpSocket, SocketAddr};
use serde_cbor::from_slice;
use std::time::{Instant, Duration};
use chrono::Utc;
use crate::id::Id;
use crate::routing::{AddContactResult, RoutingTable};
use crate::protocol::{LookupResult, MAX_VALUE_SIZE, Packet, StoreStatus};
use crate::rpc::{handle_find_node, handle_find_value, handle_ping, handle_store, send_find_node, send_find_value, send_ping, send_store};
use crate::contact::Contact;
use crate::lookup::{LookupType, NodeLookup};

const MAX_PACKET_SIZE: usize = 1200;
const K_DEFAULT: usize = 20;
const RECV_TIMEOUT: Duration = Duration::from_secs(1); // so recv in listen loop blocks for a max of 1 sec
const REQ_TIMEOUT: Duration = Duration::from_secs(10); // timeout on pending requests
const ALPHA: usize = 3;
const LOOKUP_ROUND_INTERVAL: Duration = Duration::from_millis(500);
const BUCKET_REFRESH_INTERVAL: Duration = Duration::from_hours(1);

/**
 * Used to keep track of sent requests that the node is expecting a response for
 */
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
    FindValue { target: Id, recipient: Contact, sent_at: Instant },

    Store { recipient: Contact, sent_at: Instant },
}

pub struct ValueEntry {
    pub value: Vec<u8>,
    pub is_original_publisher: bool,
    pub original_publish_time: i64, // UNIX timestamp
    pub last_republish_time: Instant,
    pub expiration: Duration,
}

pub struct ActiveStoreEntry {
    pub value: Vec<u8>,
    pub original_publish_time: i64,
}

pub struct KademliaNode {
    pub id: Id,
    pub routing_table: RoutingTable,
    pub k: usize,
    pub socket: UdpSocket,
    pub pending_requests: HashMap<Id, PendingRequest>, // key is the nonce
    pub active_lookups: HashMap<Id, NodeLookup>,
    pub completed_lookups: HashMap<Id, LookupResult>,
    pub store: HashMap<Id, ValueEntry>, // the DHT entries this node stores,
    pub active_stores: HashMap<Id, ActiveStoreEntry>, // stores the key-value pairs node is currently storing across multiple nodes
    pub completed_stores: HashMap<Id, Vec<Contact>>, // list of contacts that stored the key-value pair
    pub init_refresh_needed: bool, // for tracking when self-lookup completes in join() so we can perform bucket refreshes
}

impl KademliaNode {
    
    // *********
    // User-facing API
    // *********

    /**
     * Initializes a new node, including its UDP socket
     * If id is None, random one is generated
     * If k is None, default value is used (20)
     * Returns Err if system can't bind socket to node_addr
     */
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
            store: HashMap::new(),
            active_stores: HashMap::new(),
            completed_stores: HashMap::new(),
            init_refresh_needed: true,
        })
    }

    /**
     * Main loop for handling incoming messages and sending responses
     * Should be called after KademliaNode::new()
     */
    pub fn listen(&mut self) {
        loop {
            self.check_pending_requests();
            self.check_active_lookups();
            self.check_bucket_refresh();

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
                    match send_ping(self, least_recently_seen.addr, evict_check_nonce) {
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
                    println!("[listen] PingRequest received!");
                    handle_ping(self, src_addr, req).unwrap();
                },
                Packet::PingResponse(_) => {
                    println!("[listen] PingResponse received!");
                    let pending_req = self.pending_requests.remove(&nonce);
                    if pending_req.is_none() {
                        eprintln!("[listen] received PingResponse with no matching pending request");
                    }
                },
                Packet::FindNodeRequest(req) => {
                    handle_find_node(self, src_addr, req).unwrap();
                },
                Packet::FindNodeResponse(res) => {
                    self.pending_requests.remove(&nonce);

                    if let Some(lookup_state) = self.active_lookups.get_mut(&res.target) {
                        lookup_state.pending.remove(&sender_id);

                        for contact in res.contacts {
                            // if this node is in contact list, remove that entry to avoid circularity
                            if contact.id == self.id {
                                continue;
                            }

                            // update closest_node
                            if contact.id.distance(res.target) < lookup_state.closest_node.id.distance(res.target) {
                                lookup_state.closest_node = contact;
                            }

                            // add to shortlist if not there
                            if !lookup_state.shortlist.iter().any(|c| c.id == contact.id) {
                                lookup_state.shortlist.push(contact);
                            }
                        }

                        // re-sort and re-truncate shortlist to limit to k elements
                        lookup_state.shortlist.sort_by_key(|c| c.id.distance(res.target));
                        lookup_state.shortlist.truncate(self.k);
                    }
                },
                Packet::FindValueRequest(req) => {
                    handle_find_value(self, src_addr, req).unwrap();
                },
                Packet::FindValueResponse(res) => {
                    self.pending_requests.remove(&nonce);

                    let result = res.result.clone();
                    let mut store_contact: Option<Contact> = None;

                    if let Some(lookup_state) = self.active_lookups.get_mut(&res.target) {
                        lookup_state.pending.remove(&sender_id);

                        match &result {
                            LookupResult::Contacts(contacts) => {
                                // check if this is the closest node that didn't return value
                                if lookup_state.closest_without_value.is_none() || 
                                    sender_contact.id.distance(res.target) < lookup_state.closest_without_value.unwrap().id.distance(res.target) {
                                        lookup_state.closest_without_value = Some(sender_contact);
                                    }

                                for contact in contacts {
                                    // if this node is in contact list, remove that entry to avoid circularity
                                    if contact.id == self.id {
                                        continue;
                                    }

                                    // update closest_node
                                    if contact.id.distance(res.target) < lookup_state.closest_node.id.distance(res.target) {
                                        lookup_state.closest_node = *contact;
                                    }

                                    // add to shortlist if not there
                                    if !lookup_state.shortlist.iter().any(|c| c.id == contact.id) {
                                        lookup_state.shortlist.push(*contact);
                                    }
                                }

                                // re-sort and re-truncate shortlist to limit to k elements
                                lookup_state.shortlist.sort_by_key(|c| c.id.distance(res.target));
                                lookup_state.shortlist.truncate(self.k);
                            },
                            LookupResult::Value(value, timestamp) => {
                                // terminate lookup
                                self.completed_lookups.insert(res.target, LookupResult::Value(value.to_vec(), *timestamp));
                                store_contact = lookup_state.closest_without_value;
                            }
                        }
                    }

                    // If we terminated, send STORE to closest node that didn't return value
                    if let LookupResult::Value(value, timestamp) = &result {
                        self.active_lookups.remove(&res.target);
                        if let Some(c) = store_contact {
                            let nonce = Id::generate_id();
                            match send_store(self, c.addr, nonce, res.target, value.to_vec(), *timestamp) {
                                Ok(()) => {
                                    self.pending_requests.insert(
                                        nonce, 
                                        PendingRequest::Store { recipient: c, sent_at: Instant::now() }
                                    );
                                },
                                Err(_) => {}
                            }
                            
                        }
                    }
                },
                Packet::StoreRequest(req) => {
                    handle_store(self, src_addr, req).unwrap();
                },
                Packet::StoreResponse(res) => {
                    self.pending_requests.remove(&nonce);
                    match res.status {
                        StoreStatus::Error(e) => eprintln!("[listen] store failed: {e}"),
                        StoreStatus::Ok => {
                            // Add to list of contacts that successfully stored the value
                            if let Some(confirmed) = self.completed_stores.get_mut(&res.key) {
                                confirmed.push(sender_contact);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Join network given an existing node
     * Should be first thing called after ::new() and ::listen()
     * Adds join node to routing table
     * Performs lookup on its own ID to learn other nodes
     * Refreshes all unpopulated buckets after self-lookup completes
     */
    pub fn join(&mut self, join_addr: SocketAddr, join_id: Id) {
        let join_contact = Contact { addr: join_addr, id: join_id };
        self.routing_table.add(join_contact);
        self.lookup(LookupType::FindNode, self.id);
    }

    /**
     * Store a key-value pair in the distributed hash table
     * key-value pair is replicated to the k-closest nodes to to the key
     * If key is None, it will be the SHA1 hash of the value
     */
    pub fn store(&mut self, key: Option<Id>, value: Vec<u8>) {
        if value.len() > MAX_VALUE_SIZE {
            eprintln!("[send] value exceeds MAX_VALUE_SIZE");
            return;
        }
        
        let key = key.unwrap_or_else(|| Id::hash_value(value.clone()));
        let publish_time = Utc::now().timestamp();

        // store locally as original publisher
        self.store.insert(key, ValueEntry { 
            value: value.clone(), 
            is_original_publisher: true, 
            original_publish_time: publish_time, 
            last_republish_time: Instant::now(), 
            expiration: Duration::from_hours(24),
        });

        self.active_stores.insert(
            key, 
            ActiveStoreEntry { value, original_publish_time: publish_time }
        );
        self.lookup(LookupType::FindNode, key);
    }

    /**
     * Lookup a key in the distributed hash table
     * FindNode: returns list of k-closest contacts to target
     * FindValue: returns value of target if a node stores it, otherwise returns k-closest contacts to target
     * Result of lookup is stored in self.completed_lookups once completed
     */
    pub fn lookup(&mut self, lookup_type: LookupType, target: Id) {
        // If FIND_VALUE, see if this node stores the target
        // If so, we're done -- don't need to send any messages
        if lookup_type == LookupType::FindValue {
            if let Some(entry) = self.store.get(&target) {
                self.completed_lookups.insert(target, LookupResult::Value(entry.value.to_vec(), entry.original_publish_time));
                return;
            }
        }

        // find alpha closest contacts to target
        let closest_contacts = self.routing_table.get_closest_contacts(target, ALPHA);
        let mut lookup_state = NodeLookup::new(lookup_type, target, closest_contacts);

        let to_query: Vec<Contact> = lookup_state.shortlist.iter()
            .take(ALPHA)
            .copied()
            .collect();

        // send requests to each of the alpha nodes, and mark pending request for each
        for contact in to_query {
            let nonce: Id = Id::generate_id();

            match lookup_type {
                LookupType::FindNode => {
                    send_find_node(self, contact.addr, nonce, target).unwrap();
                    self.pending_requests.insert(
                        nonce, 
                        PendingRequest::FindNode { target, recipient: contact, sent_at: Instant::now() }
                    );
                },
                LookupType::FindValue => {
                    send_find_value(self, contact.addr, nonce, target).unwrap();
                    self.pending_requests.insert(
                        nonce,
                        PendingRequest::FindValue { target, recipient: contact, sent_at: Instant::now() }
                    );
                }
            };

            // update NodeLookup state with pending nodes
            lookup_state.pending.insert(contact.id);
            lookup_state.queried.insert(contact.id);
        }

        // create new entry in active lookups
        self.active_lookups.insert(target, lookup_state);
    }

    // *********
    // PRIVATE HELPER METHODS
    // *********

    /**
     * Handles sending FIND_* messages for active lookups and checking if lookups are complete
     * Utilizes loose parallelism: iterate periodically (500 ms), so that the number of messages in flight is some low multiple of alpha
     */
    fn check_active_lookups(&mut self) {
        let mut remove_lookups: Vec<Id> = Vec::new();

        let mut init_refresh = false;
        let mut lookup_tasks: Vec<(Id, Contact, Id, LookupType)> = Vec::new(); // nonce, Contact, target, type
        let mut store_tasks: Vec<(Id, ActiveStoreEntry, Vec<Contact>)> = Vec::new(); // key, value + original publish time, shortlist

        for (target, lookup_state) in self.active_lookups.iter_mut() {
            // Check if current round is over
            if lookup_state.last_round_at.elapsed() > LOOKUP_ROUND_INTERVAL {
                // check if termination condition hit
                if lookup_state.closest_node.id == lookup_state.old_closest_node.id && lookup_state.pending.is_empty() {
                    self.completed_lookups.insert(*target, LookupResult::Contacts(lookup_state.shortlist.clone()));
                    remove_lookups.push(*target);

                    // check if this was the initial self-lookup and now a full refresh is needed
                    if *target == self.id && self.init_refresh_needed {
                        self.init_refresh_needed = false;
                        init_refresh = true;
                    }

                    // check if this completed lookup is associated with a store
                    if let Some(entry) = self.active_stores.remove(target) {
                        // since lookup completed, move to next phase of STORE by sending STORE messages to shortlist
                        self.completed_stores.insert(*target, Vec::new());
                        store_tasks.push((*target, entry, lookup_state.shortlist.clone()));
                    }
                    continue;
                }

                lookup_state.last_round_at = Instant::now();
                lookup_state.old_closest_node = lookup_state.closest_node;

                // Get ALPHA closest contacts node has in routing table to target
                let to_query: Vec<Contact> = lookup_state.shortlist.iter()
                    .filter(|c| !lookup_state.queried.contains(&c.id))
                    .take(ALPHA)
                    .copied()
                    .collect();

                for contact in to_query {
                    let nonce = Id::generate_id();
                    lookup_state.pending.insert(contact.id);
                    lookup_state.queried.insert(contact.id);
                    lookup_tasks.push((nonce, contact, *target, lookup_state.lookup_type));
                }
            }
        }

        // check if we need to do init bucket refresh
        if init_refresh {
            self.init_refresh();
        }

        // send FIND_* messages
        for (nonce, recipient, target, lookup_type) in lookup_tasks {
            match lookup_type {
                LookupType::FindNode => {
                    send_find_node(self, recipient.addr, nonce, target).unwrap();
                    self.pending_requests.insert(
                        nonce,
                        PendingRequest::FindNode { target, recipient, sent_at: Instant::now() }
                    );
                },
                LookupType::FindValue => {
                    send_find_value(self, recipient.addr, nonce, target).unwrap();
                    self.pending_requests.insert(
                        nonce,
                        PendingRequest::FindValue { target, recipient, sent_at: Instant::now() }
                    );
                }
            };
        }

        // send STORE messages to lookups that were part of a STORE
        for (key, entry, contacts) in store_tasks {
            self.send_stores(contacts, key, entry);
        }

        for id in remove_lookups {
            self.active_lookups.remove(&id);
        }
    }

    /**
     * Loop through all pending requests and handle timeouts
     * For all request types, a timeout on the response requires node to evict the recipient from its routing table
     */
    fn check_pending_requests(&mut self) {
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
                    // if FIND_NODE times out, evict recipient and remove from lookup shortlist if it's there
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
                },
                PendingRequest::FindValue { target: k, recipient: r, sent_at: t } => {
                    // if FIND_VALUE times out, evict recipient and remove from lookup shortlist if it's there
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
                },
                PendingRequest::Store { recipient: r, sent_at: t } => {
                    // if STORE times out, evict recipient
                    if t.elapsed() > REQ_TIMEOUT {
                        self.routing_table.evict(*r);
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

    /**
     * Sends StoreRequest to each contact for key-value pair
     */
    fn send_stores(&mut self, contacts: Vec<Contact>, key: Id, active_store_entry: ActiveStoreEntry) {
        for c in contacts {
            let nonce = Id::generate_id();
            match send_store(self, c.addr, nonce, key, active_store_entry.value.clone(), active_store_entry.original_publish_time) {
                Ok(()) => {
                    self.pending_requests.insert(
                        nonce, 
                        PendingRequest::Store { recipient: c, sent_at: Instant::now() }
                    );
                },
                Err(e) => eprintln!("[send_stores] failed to send STORE to {}: {:?}", c.addr, e)
            }
        }
    }

    /**
     * Refreshes all unpopulated buckets in routing table
     * Last step of join logic, done once self-lookup completes
     */
    fn init_refresh(&mut self) {
        let mut refresh_tasks: Vec<usize> = Vec::new();
        for (i, bucket) in self.routing_table.buckets.iter().enumerate() {
            if bucket.contacts.is_empty() {
                refresh_tasks.push(i);
            }
        }
        for i in refresh_tasks {
            self.refresh_bucket(i);
        }
    }

    /**
     * Refreshes buckets which were last refreshed over one hour ago
     */
    fn check_bucket_refresh(&mut self) {
        let mut refresh_tasks: Vec<usize> = Vec::new();
        for (i, bucket) in self.routing_table.buckets.iter().enumerate() {
            if bucket.last_update.elapsed() > BUCKET_REFRESH_INTERVAL {
                refresh_tasks.push(i);
            }
        }
        for i in refresh_tasks {
            self.refresh_bucket(i);
        }
    }

    /**
     * Performs FIND_NODE lookup on random ID in bucket range
     */
    fn refresh_bucket(&mut self, bucket_index: usize) {
        let id = Id::generate_id_in_bucket(self.id, bucket_index);
        self.lookup(LookupType::FindNode, id);
        self.routing_table.buckets[bucket_index].last_update = Instant::now();
    }
}
