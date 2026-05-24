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
        KBucket { contacts: Vec::new() }
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
    pub fn add_contact(&mut self, contact: Contact) -> AddContactResult {
        let index = self.get_bucket_index(contact.id);
        let bucket = &mut self.buckets[index];

        // If contact already in bucket, move it to the tail
        if let Some(pos) = bucket.contacts.iter().position(|c| c.id == contact.id) {
            bucket.contacts.remove(pos);
            bucket.contacts.push(contact);
            return AddContactResult::Updated;
        }

        // Add if bucket not full
        if bucket.contacts.len() < self.k {
            bucket.contacts.push(contact);
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
    }

    // removes contact from bucket, if it exists
    pub fn evict(&mut self, contact: Contact) {
        let index = self.get_bucket_index(contact.id);
        let bucket = &mut self.buckets[index];
        if let Some(pos) = bucket.contacts.iter().position(|c| c.id == contact.id) {
            bucket.contacts.remove(pos);
        }
    }

    // Adds new contact to tail
    pub fn add(&mut self, contact: Contact) {
        let index = self.get_bucket_index(contact.id);
        let bucket = &mut self.buckets[index];
        bucket.contacts.push(contact);
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