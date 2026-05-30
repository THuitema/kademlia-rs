use std::time::Instant;
use std::net::SocketAddr;
use crate::contact::Contact;
use crate::id::Id;

pub enum AddContactResult {
    Added,
    Updated,
    PingRequired(Contact),
}
// Entry within the routing table
// Stores up to K contacts, sorted by time last seen (least recently seen at head)
// Similar to an LRU cache
pub struct KBucket {
    pub contacts: Vec<Contact>,
    pub last_update: Instant,
}

// Stores 160 KBuckets
// Index i stores contacts with IDs that have 2^i and 2^{i+1} distance from the node_id
pub struct RoutingTable {
    pub buckets: [KBucket; 160],
    pub node_id: Id,
    pub k: usize, 
}

impl KBucket {
    pub fn new() -> Self {
        KBucket { 
            contacts: Vec::new(),
            last_update: Instant::now(),
        }
    }
}

impl RoutingTable {
    pub fn new(id: Id, k: usize) -> Self {
        RoutingTable { 
            buckets: std::array::from_fn(|_| KBucket::new()),
            node_id: id,
            k
        }
    }

    // Returns bucket index of target_id by taking XOR with node id and counting leading zeros
    pub fn get_bucket_index(&self, target_id: Id) -> usize {
        let dist = self.node_id.distance(target_id);
        dist.leading_zeros() as usize
    }

    // Adds contact to the corresponding bucket index, if it isn't full
    // If contact already in bucket, moves it to tail
    pub fn add_contact(&mut self, contact: Contact) -> AddContactResult {
        let index = self.get_bucket_index(contact.id);
        let bucket = &mut self.buckets[index];

        // If contact already in bucket, move it to the tail
        if let Some(pos) = bucket.contacts.iter().position(|c| c.id == contact.id) {
            bucket.contacts.remove(pos);
            bucket.contacts.push(contact);
            bucket.last_update = Instant::now();
            return AddContactResult::Updated;
        }

        // Add if bucket not full
        if bucket.contacts.len() < self.k {
            bucket.contacts.push(contact);
            bucket.last_update = Instant::now();
            return AddContactResult::Added
        }

        // notify caller to ping the least recently seen contact in bucket
        // if time out, evict and insert new contact at tail
        // if responds, move it to the tail and drop this contact
        let least_recently_seen = bucket.contacts[0];
        return AddContactResult::PingRequired(least_recently_seen);
    }

    // Removes contact at head of bucket
    // Contact is some node known to be in the bucket
    pub fn evict_head(&mut self, contact: Contact) {
        let index = self.get_bucket_index(contact.id);
        let bucket = &mut self.buckets[index];
        bucket.contacts.remove(0);
        bucket.last_update = Instant::now();
    }

    // removes contact from bucket, if it exists
    pub fn evict(&mut self, contact: Contact) {
        let index = self.get_bucket_index(contact.id);
        let bucket = &mut self.buckets[index];
        if let Some(pos) = bucket.contacts.iter().position(|c| c.id == contact.id) {
            bucket.contacts.remove(pos);
            bucket.last_update = Instant::now();
        }
    }

    // Adds new contact to tail
    pub fn add(&mut self, contact: Contact) {
        let index = self.get_bucket_index(contact.id);
        let bucket = &mut self.buckets[index];
        bucket.contacts.push(contact);
        bucket.last_update = Instant::now();
    }

    // Returns n-closest contacts to target
    // If node knows < n contacts, returns all of them
    // "closeness" determiend by XOR metric
    pub fn get_closest_contacts(&self, target: Id, n: usize) -> Vec<Contact> {
        let mut contacts: Vec<Contact> = Vec::new();

        for bucket in self.buckets.iter() {
            for c in bucket.contacts.iter() {
                contacts.push(*c);
            }
        }

        contacts.sort_by_key(|contact| target.distance(contact.id));
        contacts.truncate(n);
        contacts
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

use super::*;

    fn make_contact(id: Id) -> Contact {
        Contact {
            id,
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000)
        }
    }

    fn make_id_with_leading_zeros(zeros: usize) -> Id {
        let mut id = Id { id: [0u8; 20] };
        if zeros < 160 {
            let byte = zeros / 8;
            let bit = zeros % 8;
            id.id[byte] = 1 << (7 - bit);
        }
        id
    }

    fn make_routing_table(k: usize) -> RoutingTable {
        RoutingTable::new(Id { id: [0u8; 20] }, k)
    }

    // get_bucket_index()

    #[test]
    fn test_bucket_index_known_dist() {
        let rt = make_routing_table(20);
        let contact_id = make_id_with_leading_zeros(4);
        assert_eq!(rt.get_bucket_index(contact_id), 4); // bucket index == # leading zeros
    }

    #[test]
    fn test_bucket_index_zero_leading_zeros() {
        let rt = make_routing_table(20);
        let contact_id = Id { id: [0xFF; 20] };
        assert_eq!(rt.get_bucket_index(contact_id), 0);
    }

    // add_contact()

    #[test]
    fn test_add_contact_added() {
        let mut rt = make_routing_table(20);
        let id = make_id_with_leading_zeros(4);
        let contact = make_contact(id);
        let result = rt.add_contact(contact);
        assert!(matches!(result, AddContactResult::Added));
        assert_eq!(rt.buckets[4].contacts.len(), 1);
    }

    #[test]
    fn test_add_contact_updated_moves_to_tail() {
        let mut rt = make_routing_table(20);
        let id1 = make_id_with_leading_zeros(4);
        let id2 = Id::generate_id_in_bucket(rt.node_id, 4);
        let contact1 = make_contact(id1);
        let contact2 = make_contact(id2);
        
        let r1 = rt.add_contact(contact1);
        let r2 = rt.add_contact(contact2);
        assert!(matches!(r1, AddContactResult::Added));
        assert!(matches!(r2, AddContactResult::Added));

        // re-add contact 1, should add to tail
        let r3 = rt.add_contact(contact1);
        assert!(matches!(r3, AddContactResult::Updated));
        assert_eq!(rt.buckets[4].contacts.last().unwrap().id, id1);
    }

    #[test]
    fn test_add_contact_ping_required_when_full() {
        let mut rt = make_routing_table(2); // k = 2
        let id1 = make_id_with_leading_zeros(4);
        let id2 = Id::generate_id_in_bucket(rt.node_id, 4);
        let id3 = Id::generate_id_in_bucket(rt.node_id, 4);

        rt.add_contact(make_contact(id1));
        rt.add_contact(make_contact(id2));

        let r = rt.add_contact(make_contact(id3));
        assert!(matches!(r, AddContactResult::PingRequired(_)));

        if let AddContactResult::PingRequired(lrs) = r {
            assert_eq!(lrs.id, id1);
        } else {
            panic!("expected PingRequired");
        }
    }

    // evict()

    #[test]
    fn test_evict_removes_contact() {
        let mut rt = make_routing_table(20);
        let id = make_id_with_leading_zeros(4);
        let contact = make_contact(id);
        rt.add_contact(contact);
        rt.evict(contact);
        assert!(rt.buckets[4].contacts.is_empty());
    }

    #[test]
    fn test_evict_nonexistent_does_nothing() {
        let mut rt = make_routing_table(20);
        let id = make_id_with_leading_zeros(4);
        rt.evict(make_contact(id)); // shouldn't panic
        assert!(rt.buckets[4].contacts.is_empty());
    }

    // get_closest_contacts()

    #[test]
    fn test_get_closest_contacts_returns_n() {
        let mut rt = make_routing_table(20);
        for i in 0..10 {
            let id = Id::generate_id_in_bucket(rt.node_id, i);
            rt.add_contact(make_contact(id));
        }

        let target = Id::generate_id();
        let contacts = rt.get_closest_contacts(target, 5);
        assert_eq!(contacts.len(), 5);
    }

    #[test]
    fn test_get_closest_contacts_returns_all() {
        let mut rt = make_routing_table(20);
        for i in 0..10 {
            let id = Id::generate_id_in_bucket(rt.node_id, i);
            rt.add_contact(make_contact(id));
        }

        let target = Id::generate_id();
        let contacts = rt.get_closest_contacts(target, 20);
        assert_eq!(contacts.len(), 10);
    }

    #[test]
    fn test_get_closest_contacts_sorted() {
        let mut rt = make_routing_table(20);
        for i in 0..10 {
            let id = Id::generate_id_in_bucket(rt.node_id, i);
            rt.add_contact(make_contact(id));
        }

        let target = Id::generate_id();
        let contacts = rt.get_closest_contacts(target, 5);
        for i in 0..(contacts.len() - 1) {
            assert!(contacts[i].id.distance(target) <= contacts[i+1].id.distance(target));
        }
    }
}