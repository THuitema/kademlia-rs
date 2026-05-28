use serde::{Deserialize, Serialize};
use crate::id::Id;
use crate::contact::Contact;

pub const MAX_VALUE_SIZE: usize = 1000; // max number of bytes

#[derive(Serialize, Deserialize, Debug)]
pub enum Packet {
    PingRequest(PingRequest),
    PingResponse(PingResponse),

    StoreRequest(StoreRequest),
    StoreResponse(StoreResponse),

    FindNodeRequest(FindNodeRequest),
    FindNodeResponse(FindNodeResponse),

    FindValueRequest(FindValueRequest),
    FindValueResponse(FindValueResponse),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Header {
    pub sender_id: Id,
    pub nonce: Id
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PingRequest {
    pub header: Header,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PingResponse {
    pub header: Header,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoreRequest {
    pub header: Header,
    pub key: Id,
    pub value: Vec<u8>,
    pub original_publish_time: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum StoreStatus {
    Ok,
    Error(String)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoreResponse {
    pub header: Header,
    pub key: Id,
    pub status: StoreStatus
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FindNodeRequest {
    pub header: Header,
    pub target: Id
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FindNodeResponse {
    pub header: Header,
    pub target: Id,
    pub contacts: Vec<Contact>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FindValueRequest {
    pub header: Header,
    pub target: Id 
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FindValueResponse {
    pub header: Header,
    pub target: Id,
    pub result: LookupResult
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum LookupResult {
    Contacts(Vec<Contact>),
    Value(Vec<u8>, i64), // value, UNIX original publication time
}

impl Packet {
    // returns packet header
    pub fn header(&self) -> &Header {
        match self {
            Packet::PingRequest(p) => &p.header,
            Packet::PingResponse(p) => &p.header,
            Packet::StoreRequest(p) => &p.header,
            Packet::StoreResponse(p) => &p.header,
            Packet::FindNodeRequest(p) => &p.header,
            Packet::FindNodeResponse(p) => &p.header,
            Packet::FindValueRequest(p) => &p.header,
            Packet::FindValueResponse(p) => &p.header,
        }
    }
}

