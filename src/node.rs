use crate::id::Id;
use crate::routing::RoutingTable;

struct KademliaNode {
    pub id: Id,
    pub routing_table: RoutingTable,
    pub k: usize,
}

impl KademliaNode {
    // Creates new KademliaNode with given id if Some, generates random id if None
    pub fn new(id: Option<Id>, k: usize) -> Self {
        let id = id.unwrap_or_else(Id::generate_id);
        Self {
            id,
            routing_table: RoutingTable::new(id, k),
            k
        }
    }
}
