use std::net::SocketAddr;
use std::time::Instant;
use serde_cbor::to_vec;
use std::fmt;
use crate::node::{KademliaNode, ValueEntry};
use crate::protocol::*;
use crate::id::Id;

#[derive(Debug)]
pub enum PacketError {
    Serialize(String),
    Network(std::io::Error),
    PacketTooLarge
}

impl fmt::Display for PacketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketError::Serialize(s) => write!(f, "Serialization Error ({:?})", s),
            PacketError::Network(e) => write!(f, "Network Error ({:?})", e),
            PacketError::PacketTooLarge => write!(f, "Packet Too Large")
        }
    }
}

/**
 * Sends a ping message to dest_addr
 * Caller is responsible for generating and storing the nonce
 */
pub fn send_ping(node: &KademliaNode, dest_addr: SocketAddr, nonce: Id) -> Result<(), PacketError> {
    let request_packet = Packet::PingRequest(PingRequest { 
        header: Header { sender_id: node.id, nonce } 
    });

    let buffer = to_vec(&request_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, dest_addr)
        .map_err(PacketError::Network)?;

    Ok(())
}

/**
 * Sends ping reply to src_addr
 * request is the PingRequest received from src_addr
 */
pub fn handle_ping(node: &KademliaNode, src_addr: SocketAddr, request: PingRequest) -> Result<(), PacketError> {
    let response_packet = Packet::PingResponse(PingResponse { 
        header: Header { sender_id: node.id, nonce: request.header.nonce } 
    });

    let buffer = to_vec(&response_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, src_addr)
        .map_err(PacketError::Network)?;

    Ok(())
}

/**
 * Sends FIND_NODE request to dest_addr
 * Recipient replies with the k-closest contacts to key that it knows
 */
pub fn send_find_node(node: &KademliaNode, dest_addr: SocketAddr, nonce: Id, target: Id) -> Result<(), PacketError> {
    let request_packet = Packet::FindNodeRequest(FindNodeRequest { 
        header: Header { sender_id: node.id, nonce}, 
        target 
    });

    let buffer = to_vec(&request_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, dest_addr)
        .map_err(PacketError::Network)?;
    
    Ok(())
}

/**
 * Replies to src_addr with k-closest contacts to key provided in request
 */
pub fn handle_find_node(node: &KademliaNode, src_addr: SocketAddr, request: FindNodeRequest) -> Result<(), PacketError> {
    let contacts = node.routing_table.get_closest_contacts(request.target, node.k);
    
    let response_packet = Packet::FindNodeResponse(FindNodeResponse { 
        header: Header { sender_id: node.id, nonce: request.header.nonce }, 
        target: request.target, 
        contacts
    });

    let buffer = to_vec(&response_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, src_addr)
        .map_err(PacketError::Network)?;

    Ok(())
}

/**
 * Sends FIND_VALUE request to dest_addr
 */
pub fn send_find_value(node: &KademliaNode, dest_addr: SocketAddr, nonce: Id, target: Id) -> Result<(), PacketError> {
    let request_packet = Packet::FindValueRequest(FindValueRequest { 
        header: Header { sender_id: node.id, nonce}, 
        target 
    });

    let buffer = to_vec(&request_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

        node.socket.send_to(&buffer, dest_addr)
        .map_err(PacketError::Network)?;
    Ok(())
}

/**
 * If node has key-value pair stored, replies with the value
 * Otherwise, replies with k-closest contacts to the target key, just like FIND_NODE
 */
pub fn handle_find_value(node: &KademliaNode, src_addr: SocketAddr, request: FindValueRequest) -> Result<(), PacketError> {
    let response_packet = if let Some(entry) = node.store.get(&request.target) {
        Packet::FindValueResponse(FindValueResponse {
            header: Header { sender_id: node.id, nonce: request.header.nonce },
            target: request.target,
            result: LookupResult::Value(entry.value.to_vec(), entry.original_publish_time)
        })
    } else {
        let contacts = node.routing_table.get_closest_contacts(request.target, node.k);

        Packet::FindValueResponse(FindValueResponse {
            header: Header { sender_id: node.id, nonce: request.header.nonce },
            target: request.target,
            result: LookupResult::Contacts(contacts)
        })
    };

    let buffer = to_vec(&response_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, src_addr)
        .map_err(PacketError::Network)?;

    Ok(())
}


/**
 * Sends request to dest_addr to store key-value pair
 * No guarantee that it will be stored, node must look at the status contained in the response
 */
pub fn send_store(node: &KademliaNode, dest_addr: SocketAddr, nonce: Id, key: Id, value: Vec<u8>, original_publish_time: i64) -> Result<(), PacketError> {
    // limiting value size to avoid packet fragmentation
    if value.len() > MAX_VALUE_SIZE {
        return Err(PacketError::PacketTooLarge);
    }
    
    let request_packet = Packet::StoreRequest(StoreRequest { 
        header: Header { sender_id: node.id, nonce}, 
        key,
        value,
        original_publish_time
    });

    let buffer = to_vec(&request_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

        node.socket.send_to(&buffer, dest_addr)
        .map_err(PacketError::Network)?;
    
    Ok(())
}

/**
 * Attempt to store key-value pair and reply with status of it was successfully stored or not
 * If node is already storing key, it overwrite the old value with new one contained in request
 */
pub fn handle_store(node: &mut KademliaNode, src_addr: SocketAddr, request: StoreRequest) -> Result<(), PacketError> {
    // insert or overwrite key-value pair
    node.store.insert(
        request.key, 
        ValueEntry {
            value: request.value,
            is_original_publisher: false,
            original_publish_time: request.original_publish_time,
            last_republish_time: Instant::now(),
            expiration: node.calculate_expiration(request.key)
        }
    );

    let response_packet = Packet::StoreResponse(StoreResponse { 
        header: Header { sender_id: node.id, nonce: request.header.nonce}, 
        key: request.key,
        status: StoreStatus::Ok 
    });

    let buffer = to_vec(&response_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, src_addr)
        .map_err(PacketError::Network)?;

    Ok(())
}