use serde::{Deserialize, Serialize};
use serde_json::value::Value;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Incoming<'a> {
    Request(#[serde(borrow)] RequestBody<'a>),
    Cancel { request_id: ReqId },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestBody<'a> {
    pub service_id: &'a str,
    pub request_id: ReqId,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum Outgoing {
    Next { request_id: ReqId, payload: Value },
    Complete { request_id: ReqId },
    Error { request_id: ReqId, kind: ErrorKind },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ErrorKind {
    UnknownEndpoint { endpoint: String },
    InternalError,
    BadRequest,
    ServiceError { value: Value },
}

impl Outgoing {
    #[cfg(test)]
    pub fn request_id(&self) -> ReqId {
        match self {
            Outgoing::Next { request_id, .. } => *request_id,
            Outgoing::Complete { request_id, .. } => *request_id,
            Outgoing::Error { request_id, .. } => *request_id,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ReqId(pub u64);
