use std::net::SocketAddr;
use serde_cbor::to_vec;
use crate::node::KademliaNode;
use crate::protocol::*;
use crate::id::Id;

#[derive(Debug)]
pub enum PacketError {
    Serialize(String),
    Network(std::io::Error),
}

/**
 * Sends a ping message to target_addr
 * Caller is responsible for generating and storing the nonce
 */
pub fn send_ping(node: &KademliaNode, target_addr: SocketAddr, nonce: Id) -> Result<(), PacketError> {
    let request_packet = Packet::PingRequest(PingRequest { 
        header: Header { sender_id: node.id, nonce } 
    });

    let buffer = to_vec(&request_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, target_addr)
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
 * Sends FIND_NODE request to target_addr
 * Recipient replies with the k-closest contacts to key that it knows
 */
pub fn send_find_node(node: &KademliaNode, target_addr: SocketAddr, nonce: Id, target: Id) -> Result<(), PacketError> {
    let request_packet = Packet::FindNodeRequest(FindNodeRequest { 
        header: Header { sender_id: node.id, nonce}, 
        target 
    });

    let buffer = to_vec(&request_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, target_addr)
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
        target_id: request.target, 
        contacts
    });

    let buffer = to_vec(&response_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, src_addr)
        .map_err(PacketError::Network)?;

    Ok(())
}

/**
 * Sends FIND_VALUE request to target_addr
 */
pub fn send_find_value(node: &KademliaNode, target_addr: SocketAddr, nonce: Id, target: Id) -> Result<(), PacketError> {
    let request_packet = Packet::FindValueRequest(FindValueRequest { 
        header: Header { sender_id: node.id, nonce}, 
        target 
    });

    let buffer = to_vec(&request_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;
    //hey
    node.socket.send_to(&buffer, target_addr)
        .map_err(PacketError::Network)?;
    //hey
    Ok(())
}

/**
 * If node has key-value pair stored, replies with the value
 * Otherwise, replies with k-closest contacts to the target key, just like FIND_NODE
 */
pub fn handle_find_value(node: &KademliaNode, src_addr: SocketAddr, request: FindValueRequest) -> Result<(), PacketError> {
    let response_packet = if let Some(value) = node.store.get(&request.target) {
        Packet::FindValueResponse(FindValueResponse {
            header: Header { sender_id: node.id, nonce: request.header.nonce },
            result: FindValueResult::Value(value.to_vec())
        })
    } else {
        let contacts = node.routing_table.get_closest_contacts(request.target, node.k);

        Packet::FindValueResponse(FindValueResponse {
            header: Header { sender_id: node.id, nonce: request.header.nonce },
            result: FindValueResult::Contacts(contacts)
        })
    };

    let buffer = to_vec(&response_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    node.socket.send_to(&buffer, src_addr)
        .map_err(PacketError::Network)?;
    // hey
    Ok(())
}