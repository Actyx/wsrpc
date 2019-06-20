use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Incoming<'a> {
    Request(#[serde(borrow)] RequestBody<'a>),
    Cancel {
        request_id: ReqId,
    },
}

#[derive(Serialize, Deserialize)]
pub struct RequestBody<'a> {
    pub service_id: &'a str,
    pub request_id: ReqId,
    pub payload: &'a RawValue,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Outgoing {
    Next { request_id: ReqId, payload: Box<RawValue> },
    Complete { request_id: ReqId },
    Error { request_id: ReqId },
}

impl Outgoing {
    pub fn request_id(&self) -> ReqId {
        match self {
            Outgoing::Next { request_id, .. } => *request_id,
            Outgoing::Complete { request_id, .. } => *request_id,
            Outgoing::Error { request_id, .. } => *request_id,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ReqId(pub u64);
