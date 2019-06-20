extern crate bytes;
extern crate chashmap;
extern crate futures;
extern crate futures01;
extern crate log;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate warp;

mod formats;

use formats::*;
use futures::channel::mpsc;
use futures::compat::{Executor01CompatExt, Future01CompatExt, Stream01CompatExt};
use futures::stream;
use futures::stream::BoxStream;
use futures::task::SpawnExt;
use futures::{future, Future, Sink};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use futures01::{Future as Future01, Stream as Stream01};
use log::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::executor::DefaultExecutor;
use warp::filters::ws::{Message, WebSocket, Ws2};
use warp::{Filter, Rejection};

const WS_SEND_BUFFER_SIZE: usize = 1024;

pub trait Service {
    type Req: DeserializeOwned;
    type Resp: Serialize + 'static;

    fn id(&self) -> &'static str;

    fn serve(&self, req: Self::Req) -> BoxStream<'static, Self::Resp>;

    fn boxed(self) -> BoxedService
    where
        Self: Send + WebsocketService + Sized + Sync + 'static,
    {
        Box::new(self)
    }
}

pub trait WebsocketService {
    fn id_ws(&self) -> &str;

    fn serve_ws<'a>(&self, raw_req: &'a RawValue) -> BoxStream<'static, String>;
}

impl<Req, Resp, S> WebsocketService for S
where
    S: Service<Req = Req, Resp = Resp>,
    Req: DeserializeOwned,
    Resp: Serialize + 'static,
{
    fn id_ws(&self) -> &str {
        self.id()
    }

    fn serve_ws<'a>(&self, raw_req: &'a RawValue) -> BoxStream<'static, String> {
        if let Ok(req) = serde_json::from_str(raw_req.get()) {
            self.serve(req)
                .map(|resp| {
                    // TODO: errors
                    serde_json::to_string(&resp).unwrap()
                })
                .boxed()
        } else {
            error!(
                "Error deserializing request for service {}: {}",
                raw_req.get(),
                self.id()
            );
            stream::empty().boxed()
        }
    }
}

pub type BoxedService = Box<dyn WebsocketService + Send + Sync>;

pub fn serve(services: Vec<BoxedService>) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let services_index: BTreeMap<String, BoxedService> =
        services.into_iter().map(|srv| (srv.id_ws().to_string(), srv)).collect();

    let services = Arc::new(services_index);
    warp::ws2().map(move |ws: Ws2| {
        let services_clone = services.clone();
        ws.on_upgrade(move |socket| client_connected(socket, services_clone))
    })
}

// TODO: Cancel previous requests
fn client_connected(
    ws: WebSocket,
    services: Arc<BTreeMap<String, BoxedService>>,
) -> impl Future01<Item = (), Error = ()> {
    let (ws_out, ws_in) = ws.split();

    // Create an MPSC channel to merge outbound WS messages
    let (mux_in, mux_out) = mpsc::channel::<Result<Message, warp::Error>>(WS_SEND_BUFFER_SIZE);

    let mut executor = DefaultExecutor::current().compat();

    // Pipe the merged stream into the websocket output;
    // TODO: log the error here
    executor
        .spawn(mux_out.compat().forward(ws_out).compat().map(|_| ()))
        .expect("Could not spawn multiplex task into executor");

    ws_in
        .compat()
        .try_for_each(move |raw_msg| {
            // Do some parsing first...
            if let Ok(text_msg) = raw_msg.to_str() {
                println!("WS: {}", text_msg);
                serde_json::from_str::<Incoming>(text_msg).unwrap();
                if let Ok(req_env) = serde_json::from_str::<Incoming>(text_msg) {
                    match req_env {
                        Incoming::Request(body) => {
                            // Locate the service matching the request
                            if let Some(srv) = services.get(body.service_id) {
                                executor
                                    .spawn(serve_request(srv, body.request_id, body.payload, mux_in.clone()))
                                    .expect("Could not spawn response stream task into executor");
                            } else {
                                error!("Service is unknown: {}", body.service_id);
                            }
                        }
                        Incoming::Cancel { .. } => {} // TODO:
                    }
                } else {
                    error!("Could not deserialize client request {}", text_msg);
                }
            } else {
                error!("Expected TEXT Websocket message but got binary");
            };
            future::ok(())
        })
        .map_err(|err| {
            error!("Websocket closed with error {}", err);
        })
        .compat()
}

fn serve_request_stream(
    srv: &BoxedService,
    req_id: ReqId,
    payload: &RawValue,
) -> impl Stream<Item = Result<Message, warp::Error>> {
    srv.serve_ws(payload)
        .map(move |payload| Outgoing::Next {
            request_id: req_id,
            payload: RawValue::from_string(payload).unwrap(),
        })
        .chain(stream::once(future::ready(Outgoing::Complete { request_id: req_id })))
        .map(|env| Ok(Message::text(serde_json::to_string(&env).unwrap())))
}

fn serve_request(
    srv: &BoxedService,
    req_id: ReqId,
    payload: &RawValue,
    output: impl Sink<Result<Message, warp::Error>>,
) -> impl Future<Output = ()> {
    serve_request_stream(srv, req_id, payload)
        .map(|err| {
            // We need to re-wrap in an outer result because Sink requires SinkError as the error type
            // but it will pass our inner error unmodified
            Ok(err)
        })
        .forward(output)
        .map(|result| {
            if result.is_err() {
                error!("Multiplexing error");
            };
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Service;
    use futures::stream::BoxStream;
    use futures::stream::StreamExt;
    use futures::{stream, Poll};
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};
    use warp::Filter;
    use websocket::{ClientBuilder, OwnedMessage};

    #[derive(Serialize, Deserialize)]
    struct Request(u64);
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct Response(u64);

    struct TestService {
        ctr: Arc<Mutex<u64>>,
    }

    impl TestService {
        fn new() -> TestService {
            TestService {
                ctr: Arc::new(Mutex::new(0)),
            }
        }
    }

    impl Service for TestService {
        type Req = Request;
        type Resp = Response;

        fn id(&self) -> &'static str {
            "test"
        }

        fn serve(&self, _req: Request) -> BoxStream<'static, Response> {
            let ctr = self.ctr.clone();
            stream::poll_fn(move |_| {
                let mut lock = ctr.lock().unwrap();
                let output = *lock;
                *lock += 1;
                Poll::Ready(Some(Response(output)))
            })
            .take(5)
            .boxed()
        }
    }

    fn test_client<Req: Serialize, Resp: DeserializeOwned>(endpoint: &str, id: u64, req: Req) -> Vec<Resp> {
        let client = ClientBuilder::new("ws://127.0.0.1:3030/test_ws")
            .expect("Could not setup client")
            .connect_insecure()
            .expect("Could not connect to test server");

        let (mut receiver, mut sender) = client.split().unwrap();

        println!("Connected");

        let req = serde_json::to_string(&req).expect("Could not serialize request");
        let payload = &RawValue::from_string(req).unwrap();

        let req_env = Incoming::Request(
            RequestBody {
            service_id: endpoint,
            request_id: ReqId(id),
            payload: &payload,
            }
        );

        let req_env_json = serde_json::to_string(&req_env).expect("Could not serialize request envelope");

        sender
            .send_message(&OwnedMessage::Text(req_env_json))
            .expect("Could not send request");

        println!("Sent message");

        receiver
            .incoming_messages()
            .filter_map(move |msg| {
                println!("Received {:?}", msg);
                if let Ok(OwnedMessage::Text(raw_resp)) = msg {
                    let resp_env: Outgoing =
                        serde_json::from_str(&*raw_resp).expect("Could not deserialize response envelope");
                    if resp_env.request_id().0 == id {
                        Some(resp_env)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .take_while(|env| if let Outgoing::Next { .. } = env { true } else { false })
            .map(|env| {
                if let Outgoing::Next { payload, .. } = env {
                    serde_json::from_str::<Resp>(payload.get()).expect("Could not deserialize response")
                } else {
                    unreachable!()
                }
            })
            .collect()
    }

    #[test]
    fn properly_serve_single_request() {
        let ws = warp::path("test_ws").and(super::serve(vec![TestService::new().boxed()]));

        let mut rt = tokio::runtime::Runtime::new().expect("could not start tokio runtime");
        let task = warp::serve(ws).bind(([127, 0, 0, 1], 3030));
        rt.spawn(task);

        assert_eq!(
            test_client::<Request, Response>("testy", 0, Request(5)),
            vec![Response(0), Response(1), Response(2), Response(3), Response(4)]
        );
        rt.shutdown_now();
    }

}
