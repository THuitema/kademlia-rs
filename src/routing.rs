use crate::contact::Contact;

// Entry within the routing table
// Stores up to K contacts, sorted by time last seen (least recently seen at head)
// Similar to an LRU cache
pub struct KBucket {
    pub contacts: Vec<Contact>,
}

// Stores 160 KBuckets
// Index i stores contacts with IDs that have 2^i and 2^{i+1} distance from this node
pub struct RoutingTable {
    pub buckets: [KBucket; 160],
}