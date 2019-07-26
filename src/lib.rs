extern crate bytes;
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
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::executor::DefaultExecutor;
use warp::filters::ws::{Message, WebSocket, Ws2};
use warp::{Filter, Rejection};

const WS_SEND_BUFFER_SIZE: usize = 1024;
const REQUEST_GC_THRESHOLD: usize = 64;

pub trait Service {
    type Req: DeserializeOwned;
    type Resp: Serialize + 'static;
    type Error: Serialize + 'static;

    fn id(&self) -> &'static str;

    fn serve(&self, req: Self::Req) -> BoxStream<'static, Result<Self::Resp, Self::Error>>;

    fn boxed(self) -> BoxedService
    where
        Self: Send + WebsocketService + Sized + Sync + 'static,
    {
        Box::new(self)
    }
}

pub trait WebsocketService {
    fn id_ws(&self) -> &str;

    fn serve_ws(&self, raw_req: Value) -> BoxStream<'static, Result<Value, ErrorKind>>;
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

    fn serve_ws(&self, raw_req: Value) -> BoxStream<'static, Result<Value, ErrorKind>> {
        if let Ok(req) = serde_json::from_value(raw_req) {
            self.serve(req)
                .map(|resp_result| {
                    resp_result
                        .map(|resp| serde_json::to_value(&resp).expect("Could not serialize service response"))
                        .map_err(|err| ErrorKind::ServiceError {
                            value: serde_json::to_value(&err).expect("Could not serialize service error response"),
                        })
                })
                .boxed()
        } else {
            warn!("Error deserializing request for service {}", self.id());
            stream::once(future::err(ErrorKind::BadRequest)).boxed()
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
    let (mut mux_in, mux_out) = mpsc::channel::<Result<Message, warp::Error>>(WS_SEND_BUFFER_SIZE);

    // Map of request IDs to the reference counted boolean that will terminate the response
    // stream upon cancellation. There is no need for a concurrent map because we simply share
    // the entries with the running streams. This also means that the running response stream
    // does not need to actually look up the entry every time.
    let mut active_responses: HashMap<ReqId, Arc<AtomicBool>> = HashMap::new();

    let mut executor = DefaultExecutor::current().compat();

    // Pipe the merged stream into the websocket output;
    // TODO: log the error here
    executor
        .spawn(mux_out.compat().forward(ws_out).compat().map(|_| ()))
        .expect("Could not spawn multiplex task into executor");

    ws_in
        .compat()
        .try_for_each(move |raw_msg| {
            if active_responses.len() > REQUEST_GC_THRESHOLD {
                active_responses.retain(|_, canceled| !canceled.load(Ordering::SeqCst));
            }

            // Do some parsing first...
            if let Ok(text_msg) = raw_msg.to_str() {
                if let Ok(req_env) = serde_json::from_str::<Incoming>(text_msg) {
                    match req_env {
                        Incoming::Request(body) => {
                            // Locate the service matching the request
                            if let Some(srv) = services.get(body.service_id) {
                                // Set up cancellation signal
                                let canceled = Arc::new(AtomicBool::new(false));
                                active_responses.insert(body.request_id, canceled.clone());
                                executor
                                    .spawn(serve_request(
                                        canceled,
                                        srv,
                                        body.request_id,
                                        body.payload,
                                        mux_in.clone(),
                                    ))
                                    .expect("Could not spawn response stream task into executor");
                            } else {
                                executor
                                    .spawn(serve_error(
                                        body.request_id,
                                        ErrorKind::UnknownEndpoint {
                                            endpoint: body.service_id.to_string(),
                                        },
                                        mux_in.clone(),
                                    ))
                                    .expect("Cound not spawn error response stream into executor");
                                warn!("Client tried to access unknown service: {}", body.service_id);
                            }
                        }
                        Incoming::Cancel { request_id } => {
                            if let Some(canceled) = active_responses.remove(&request_id) {
                                canceled.store(true, Ordering::SeqCst);
                            }
                        }
                    }
                } else {
                    error!("Could not deserialize client request {}", text_msg);
                    mux_in.close_channel();
                }
            } else {
                error!("Expected TEXT Websocket message but got binary");
                mux_in.close_channel();
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
    payload: Value,
) -> impl Stream<Item = Result<Message, warp::Error>> {
    let resp_stream = srv.serve_ws(payload).map(move |payload_result| match payload_result {
        Ok(payload) => Outgoing::Next {
            request_id: req_id,
            payload,
        },
        Err(kind) => Outgoing::Error {
            request_id: req_id,
            kind,
        },
    });

    AssertUnwindSafe(resp_stream)
        .catch_unwind()
        .map(move |msg_result| match msg_result {
            Ok(msg) => msg,
            Err(_) => Outgoing::Error {
                request_id: req_id,
                kind: ErrorKind::InternalError,
            },
        })
        .chain(stream::once(future::ready(Outgoing::Complete { request_id: req_id })))
        .map(|env| Ok(Message::text(serde_json::to_string(&env).unwrap())))
}

fn serve_request(
    canceled: Arc<AtomicBool>,
    srv: &BoxedService,
    req_id: ReqId,
    payload: Value,
    output: impl Sink<Result<Message, warp::Error>>,
) -> impl Future<Output = ()> {
    serve_request_stream(srv, req_id, payload)
        .take_while(move |_| future::ready(!canceled.load(Ordering::SeqCst)))
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

fn serve_error(
    req_id: ReqId,
    error_kind: ErrorKind,
    output: impl Sink<Result<Message, warp::Error>>,
) -> impl Future<Output = ()> {
    let msg = Outgoing::Error {
        request_id: req_id,
        kind: error_kind,
    };

    let raw_msg = Message::text(serde_json::to_string(&msg).unwrap());

    stream::once(future::ok(Ok(raw_msg))).forward(output).map(|result| {
        if result.is_err() {
            error!("Could not send Error message");
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
    use std::net::SocketAddr;
    use std::thread::JoinHandle;
    use warp::Filter;
    use websocket::{ClientBuilder, OwnedMessage};

    #[derive(Serialize, Deserialize)]
    enum Request {
        Count(u64),   // Returns numbers 0..N
        Fail(String), // Fails the service normally with given reason
        Panic,        // Panics the service
    }
    #[derive(Serialize, Deserialize)]
    struct BadRequest {
        bad_field: String,
    }
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct Response(u64);

    struct TestService();

    impl TestService {
        fn new() -> TestService {
            TestService()
        }
    }

    impl Service for TestService {
        type Req = Request;
        type Resp = Response;
        type Error = String;

        fn id(&self) -> &'static str {
            "test"
        }

        fn serve(&self, req: Request) -> BoxStream<'static, Result<Response, String>> {
            match req {
                Request::Count(cnt) => {
                    let mut ctr = 0;
                    stream::poll_fn(move |_| {
                        let output = ctr;
                        ctr += 1;
                        if ctr <= cnt {
                            Poll::Ready(Some(Ok(Response(output))))
                        } else {
                            Poll::Ready(None)
                        }
                    })
                    .boxed()
                }
                Request::Fail(reason) => stream::once(future::err(reason)).boxed(),
                Request::Panic => stream::poll_fn(|_| panic!("Test panic")).boxed(),
            }
        }
    }

    fn test_client<Req: Serialize, Resp: DeserializeOwned>(
        addr: SocketAddr,
        endpoint: &str,
        id: u64,
        req: Req,
    ) -> (Vec<Resp>, Outgoing) {
        let addr = format!("ws://{}/test_ws", addr);
        let client = ClientBuilder::new(&*addr)
            .expect("Could not setup client")
            .connect_insecure()
            .expect("Could not connect to test server");

        let (mut receiver, mut sender) = client.split().unwrap();

        let payload = serde_json::to_value(req).expect("Could not serialize request");
        let req_env = Incoming::Request(RequestBody {
            service_id: endpoint,
            request_id: ReqId(id),
            payload,
        });
        let req_env_json = serde_json::to_string(&req_env).expect("Could not serialize request envelope");

        sender
            .send_message(&OwnedMessage::Text(req_env_json))
            .expect("Could not send request");

        let mut completion: Option<Outgoing> = None;

        let msgs = receiver
            .incoming_messages()
            .filter_map(move |msg| {
                let msg_ok = msg.expect("Expected message but got websocket error");
                if let OwnedMessage::Text(raw_resp) = msg_ok {
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
            .take_while(|env| {
                if let Outgoing::Next { .. } = env {
                    true
                } else {
                    completion = Some(env.clone());
                    false
                }
            }) // if let Outgoing::Next { .. } = env { true } else { false })
            .filter_map(|env| {
                if let Outgoing::Next { payload, .. } = env {
                    Some(serde_json::from_value::<Resp>(payload).expect("Could not deserialize response"))
                } else {
                    None
                }
            })
            .collect();
        (msgs, completion.expect("Expected a completion message"))
    }

    fn start_test_service() -> (SocketAddr, tokio::runtime::Runtime) {
        let ws = warp::path("test_ws").and(super::serve(vec![TestService::new().boxed()]));

        let mut rt = tokio::runtime::Runtime::new().expect("could not start tokio runtime");
        let (addr, task) = warp::serve(ws).bind_ephemeral(([127, 0, 0, 1], 0));
        rt.spawn(task);
        (addr, rt)
    }

    #[test]
    fn properly_serve_single_request() {
        let (addr, rt) = start_test_service();

        assert_eq!(
            test_client::<Request, Response>(addr, "test", 0, Request::Count(5)).0,
            vec![Response(0), Response(1), Response(2), Response(3), Response(4)]
        );
        rt.shutdown_now();
    }

    #[test]
    fn multiplex_multiple_queries() {
        let (addr, rt) = start_test_service();

        let client_cnt = 50;
        let request_cnt = 100;
        let start_barrier = Arc::new(std::sync::Barrier::new(client_cnt));

        let join_handles: Vec<JoinHandle<Vec<Response>>> = (0..client_cnt)
            .map(|i| {
                let b = start_barrier.clone();
                std::thread::spawn(move || {
                    b.wait();
                    test_client::<Request, Response>(addr, "test", i as u64, Request::Count(request_cnt)).0
                })
            })
            .collect();
        let expected: Vec<Response> = (0..request_cnt).map(|i| Response(i as u64)).collect();

        for handle in join_handles {
            assert_eq!(handle.join().unwrap(), expected)
        }

        rt.shutdown_now();
    }

    #[test]
    fn report_wrong_endpoint() {
        let (addr, rt) = start_test_service();

        let (msgs, completion) = test_client::<Request, Response>(addr, "no_such_service", 49, Request::Count(5));

        assert_eq!(msgs, vec![]);

        assert_eq!(
            completion,
            Outgoing::Error {
                request_id: ReqId(49),
                kind: ErrorKind::UnknownEndpoint {
                    endpoint: "no_such_service".to_string()
                }
            }
        );

        rt.shutdown_now();
    }

    #[test]
    fn report_badly_formatted_request() {
        let (addr, rt) = start_test_service();

        let (msgs, completion) = test_client::<BadRequest, Response>(
            addr,
            "test",
            49,
            BadRequest {
                bad_field: "xzy".to_string(),
            },
        );

        assert_eq!(msgs, vec![]);

        assert_eq!(
            completion,
            Outgoing::Error {
                request_id: ReqId(49),
                kind: ErrorKind::BadRequest,
            }
        );

        rt.shutdown_now();
    }

    #[test]
    fn report_service_error() {
        let (addr, rt) = start_test_service();

        let (msgs, completion) =
            test_client::<Request, Response>(addr, "test", 49, Request::Fail("Test reason".to_string()));

        assert_eq!(msgs, vec![]);

        assert_eq!(
            completion,
            Outgoing::Error {
                request_id: ReqId(49),
                kind: ErrorKind::ServiceError {
                    value: Value::String("Test reason".to_string())
                },
            }
        );

        rt.shutdown_now();
    }

    #[test]
    fn report_service_panic() {
        let (addr, rt) = start_test_service();

        let (msgs, completion) = test_client::<Request, Response>(addr, "test", 49, Request::Panic);

        assert_eq!(msgs, vec![]);

        assert_eq!(
            completion,
            Outgoing::Error {
                request_id: ReqId(49),
                kind: ErrorKind::InternalError,
            }
        );

        rt.shutdown_now();
    }

    // Handle service panic

}
