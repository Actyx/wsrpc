/*
 * Copyright 2021 Actyx AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use serde::{Deserialize, Serialize};
use serde_json::value::{RawValue, Value};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Incoming<'a> {
    Request(#[serde(borrow)] RequestBody<'a>),
    #[serde(rename_all = "camelCase")]
    Cancel {
        request_id: ReqId,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestBody<'a> {
    pub service_id: &'a str,
    pub request_id: ReqId,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Outgoing {
    #[serde(rename_all = "camelCase")]
    Next {
        request_id: ReqId,
        payload: Box<RawValue>,
    },
    #[serde(rename_all = "camelCase")]
    Complete { request_id: ReqId },
    #[serde(rename_all = "camelCase")]
    Error { request_id: ReqId, kind: ErrorKind },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum ErrorKind {
    UnknownEndpoint {
        endpoint: String,
        valid_endpoints: Vec<String>,
    },
    InternalError,
    BadRequest {
        message: String,
    },
    ServiceError {
        value: Value,
    },
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
