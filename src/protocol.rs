use crate::id::Id;
use crate::contact::Contact;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
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
pub struct KeyExistsRequest {
    pub header: Header,
    pub key: Id
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KeyExistsResponse {
    pub header: Header,
    pub has_value: bool
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoreRequest {
    pub header: Header,
    pub key: Id,
    pub value: Vec<u8>
}

#[derive(Serialize, Deserialize, Debug)]
pub enum StoreStatus {
    Ok,
    AlreadyExists,
    BucketFull,
    InsufficientStorage,
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
    pub key: Id
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FindNodeResponse {
    pub header: Header,
    pub target_id: Id,
    pub contacts: Vec<Contact>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FindValueRequest {
    pub header: Header,
    pub key: Id 
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FindValueResponse {
    pub header: Header,
    pub result: FindValueResult
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FindValueResult {
    Contacts(Vec<Contact>),
    Value(Vec<u8>)
}

