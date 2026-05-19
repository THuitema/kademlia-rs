use crate::id::Id;
use crate::routing::RoutingTable;

struct KademliaNode {
    pub id: Id,
    pub routing_table: RoutingTable,
}