use std::net::{UdpSocket, SocketAddr};
use serde_cbor::to_vec;
use crate::protocol::*;
use crate::id::Id;
use crate::routing::RoutingTable;

#[derive(Debug)]
pub enum PacketError {
    Serialize(String),
    Network(std::io::Error),
}

/**
 * Sends a ping message to target_addr
 * Caller is responsible for generating and storing the nonce
 */
pub fn send_ping(socket: &UdpSocket, target_addr: SocketAddr, node_id: Id, nonce: Id) -> Result<(), PacketError> {
    let request_packet = Packet::PingRequest(PingRequest { 
        header: Header { sender_id: node_id, nonce } 
    });

    let buffer = to_vec(&request_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    socket.send_to(&buffer, target_addr)
        .map_err(PacketError::Network)?;

    Ok(())
}

/**
 * Sends ping reply to src_addr
 * request is the PingRequest received from src_addr
 */
pub fn handle_ping(socket: &UdpSocket, src_addr: SocketAddr, request: PingRequest, node_id: Id) -> Result<(), PacketError> {
    let response_packet = Packet::PingResponse(PingResponse { 
        header: Header { sender_id: node_id, nonce: request.header.nonce } 
    });

    let buffer = to_vec(&response_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    socket.send_to(&buffer, src_addr)
        .map_err(PacketError::Network)?;

    Ok(())
}

/**
 * Sends FIND_NODE request to target_addr
 * Recipient replies with the k-closest contacts to key that it knows
 */
pub fn send_find_node(socket: &UdpSocket, target_addr: SocketAddr, node_id: Id, nonce: Id, key: Id) -> Result<(), PacketError> {
    let request_packet = Packet::FindNodeRequest(FindNodeRequest { 
        header: Header { sender_id: node_id, nonce}, 
        key 
    });

    let buffer = to_vec(&request_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    socket.send_to(&buffer, target_addr)
        .map_err(PacketError::Network)?;
    
    Ok(())
}

/**
 * Replies to src_addr with k-closest contacts to key provided in request
 */
pub fn handle_find_node(socket: &UdpSocket, src_addr: SocketAddr, request: FindNodeRequest, node_id: Id, routing_table: &RoutingTable) -> Result<(), PacketError> {
    let contacts = routing_table.get_closest_contacts(request.key);
    
    let response_packet = Packet::FindNodeResponse(FindNodeResponse { 
        header: Header { sender_id: node_id, nonce: request.header.nonce }, 
        target_id: request.key, 
        contacts
    });

    let buffer = to_vec(&response_packet)
        .map_err(|e| PacketError::Serialize(e.to_string()))?;

    socket.send_to(&buffer, src_addr)
        .map_err(PacketError::Network)?;

    Ok(())
}