use crate::id::Id;
use crate::contact::Contact;

#[derive(Debug)]
pub enum Packet {
    PingRequest(PingRequest),
    PingResponse(PingResponse),

    KeyExistsRequest(KeyExistsRequest),
    KeyExistsResponse(KeyExistsResponse),

    StoreRequest(StoreRequest),
    StoreResponse(StoreResponse),

    FindNodeRequest(FindNodeRequest),
    FindNodeResponse(FindNodeResponse),

    FindValueRequest(FindValueRequest),
    FindValueResponse(FindValueResponse),
}

#[derive(Debug)]
pub struct Header {
    pub sender_id: Id,
    pub nonce: Id
}

#[derive(Debug)]
pub struct PingRequest {
    pub header: Header,
}

#[derive(Debug)]
pub struct PingResponse {
    pub header: Header,
}

#[derive(Debug)]
pub struct KeyExistsRequest {
    pub header: Header,
    pub key: Id
}

#[derive(Debug)]
pub struct KeyExistsResponse {
    pub header: Header,
    pub key: Id,
    pub has_value: bool
}

#[derive(Debug)]
pub struct StoreRequest {
    pub header: Header,
    pub key: Id,
    pub value: Vec<u8>
}

#[derive(Debug)]
pub struct StoreResponse {
    pub header: Header,
    pub key: Id,
    pub status_code: u8,
    pub status_msg: String
}

#[derive(Debug)]
pub struct FindNodeRequest {
    pub header: Header,
    pub key: Id
}

#[derive(Debug)]
pub struct FindNodeResponse {
    pub header: Header,
    pub key: Id,
    pub contacts: Vec<Contact>
}

#[derive(Debug)]
pub struct FindValueRequest {
    pub header: Header,
    pub key: Id 
}

#[derive(Debug)]
pub struct FindValueResponse {
    pub header: Header,
    pub result: FindValueResult
}

#[derive(Debug)]
pub enum FindValueResult {
    Contacts(Vec<Contact>),
    Value(Vec<u8>)
}

