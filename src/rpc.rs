use std::net::{UdpSocket, SocketAddr};
use serde_cbor::to_vec;
use crate::{node, protocol::*};
use crate::id::Id;

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