use std::collections::HashSet;
use std::time::Instant;
use crate::id::Id;
use crate::contact::Contact;

#[derive(Clone, Copy, PartialEq)]
pub enum LookupType {
    FindNode,
    FindValue
}

pub struct NodeLookup {
    pub lookup_type: LookupType,
    pub target: Id,
    pub closest_node: Contact,
    pub old_closest_node: Contact,
    pub shortlist: Vec<Contact>,
    pub queried: HashSet<Id>, // all nodes we've sent a message to
    pub pending: HashSet<Id>, // all nodes we've sent a message to and are waiting for a response on
    pub last_round_at: Instant,
}

impl NodeLookup {
    pub fn new(lookup_type: LookupType, target: Id, init_contacts: Vec<Contact>) -> Self {
        let closest_node = *init_contacts.iter()
            .min_by_key(|c| c.id.distance(target))
            .unwrap();

        NodeLookup { 
            lookup_type,
            target, 
            closest_node, 
            old_closest_node: closest_node,
            shortlist: init_contacts, 
            queried: HashSet::new(), 
            pending: HashSet::new(),
            last_round_at: Instant::now()
        }
    }
}