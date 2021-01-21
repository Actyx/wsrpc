mod formats;

use actyxos_sdk::{app_id, tagged::AppId};
use ax_futures_util::prelude::*;
use formats::*;
use futures::channel::{mpsc, oneshot};
use futures::stream;
use futures::stream::BoxStream;
use futures::{future, Future, Sink};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use tracing::*;
use util::yield_after::yield_after;
use warp::filters::ws::{Message, WebSocket, Ws};
use warp::{Filter, Rejection};

const WS_SEND_BUFFER_SIZE: usize = 1024;
const REQUEST_GC_THRESHOLD: usize = 64;
const INTER_STREAM_FAIRNESS: u64 = 64;

pub trait Service {
    type Req: DeserializeOwned;
    type Resp: Serialize + 'static;
    type Error: Serialize + 'static;

    fn id(&self) -> &'static str;

    fn serve(&self, app_id: AppId, req: Self::Req) -> BoxStream<'static, Result<Self::Resp, Self::Error>>;

    fn boxed(self) -> BoxedService
    where
        Self: Send + WebsocketService + Sized + Sync + 'static,
    {
        Box::new(self)
    }
}

pub trait WebsocketService {
    fn id_ws(&self) -> &str;

    fn serve_ws(&self, app_id: AppId, raw_req: Value) -> BoxStream<'static, Result<Value, ErrorKind>>;
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

    fn serve_ws(&self, app_id: AppId, raw_req: Value) -> BoxStream<'static, Result<Value, ErrorKind>> {
        trace!("Serving raw request for service {}: {:?}", self.id(), raw_req);
        match serde_json::from_value(raw_req) {
            Ok(req) => self
                .serve(app_id, req)
                .map(|resp_result| {
                    resp_result
                        .map(|resp| serde_json::to_value(&resp).expect("Could not serialize service response"))
                        .map_err(|err| ErrorKind::ServiceError {
                            value: serde_json::to_value(&err).expect("Could not serialize service error response"),
                        })
                })
                .boxed(),
            Err(cause) => {
                let message = format!("{}", cause);
                warn!("Error deserializing request for service {}: {}", self.id(), message);
                stream::once(future::err(ErrorKind::BadRequest { message })).boxed()
            }
        }
    }
}

pub type BoxedService = Box<dyn WebsocketService + Send + Sync>;

pub fn serve(
    services: Vec<BoxedService>,
    app_id_filter: impl Filter<Extract = (AppId,), Error = Rejection> + Clone,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let services_index: BTreeMap<String, BoxedService> =
        services.into_iter().map(|srv| (srv.id_ws().to_string(), srv)).collect();

    let services = Arc::new(services_index);

    app_id_filter.and(warp::ws()).map(move |app_id: AppId, ws: Ws| {
        let services_clone = services.clone();
        // Set the max frame size to 64 MB (defaults to 16 MB which we have hit at CTA)
        ws.max_frame_size(64 << 20)
            // Set the max message size to 128 MB (defaults to 64 MB which we have hit for an humongous snapshot)
            .max_message_size(128 << 20)
            .on_upgrade(move |socket| client_connected(socket, app_id, services_clone).map(|_| ()))
        // on_upgrade does not take in errors any longer
    })
}

#[allow(clippy::cognitive_complexity)]
fn client_connected(
    ws: WebSocket,
    app_id: AppId,
    services: Arc<BTreeMap<String, BoxedService>>,
) -> impl Future<Output = Result<(), ()>> {
    let (ws_out, ws_in) = ws.split();

    // Create an MPSC channel to merge outbound WS messages
    let (mut mux_in, mux_out) = mpsc::channel::<Result<Message, warp::Error>>(WS_SEND_BUFFER_SIZE);

    // Map of request IDs to the reference counted boolean that will terminate the response
    // stream upon cancellation. There is no need for a concurrent map because we simply share
    // the entries with the running streams. This also means that the running response stream
    // does not need to actually look up the entry every time.
    let mut active_responses: HashMap<ReqId, oneshot::Sender<()>> = HashMap::new();

    // Pipe the merged stream into the websocket output;
    tokio::spawn(mux_out.fuse().forward(ws_out).map(|_| ()));

    ws_in
        .try_for_each(move |raw_msg| {
            if active_responses.len() > REQUEST_GC_THRESHOLD {
                active_responses.retain(|_, canceled| !canceled.is_canceled());
            }

            // Do some parsing first...
            if let Ok(text_msg) = raw_msg.to_str() {
                match serde_json::from_str::<Incoming>(text_msg) {
                    Ok(req_env) => match req_env {
                        Incoming::Request(body) => {
                            // Locate the service matching the request
                            if let Some(srv) = services.get(body.service_id) {
                                // Set up cancellation signal
                                let (snd_cancel, rcv_cancel) = oneshot::channel();

                                if let Some(previous) = active_responses.insert(body.request_id, snd_cancel) {
                                    cancel_response_stream(previous);
                                };

                                tokio::spawn(serve_request(
                                    rcv_cancel,
                                    srv,
                                    app_id.clone(),
                                    body.request_id,
                                    body.payload,
                                    mux_in.clone(),
                                ));
                            } else {
                                tokio::spawn(serve_error(
                                    body.request_id,
                                    ErrorKind::UnknownEndpoint {
                                        endpoint: body.service_id.to_string(),
                                        valid_endpoints: services.keys().cloned().collect::<Vec<String>>(),
                                    },
                                    mux_in.clone(),
                                ));
                                warn!("Client tried to access unknown service: {}", body.service_id);
                            }
                        }
                        Incoming::Cancel { request_id } => {
                            if let Some(snd_cancel) = active_responses.remove(&request_id) {
                                cancel_response_stream(snd_cancel);
                            }
                        }
                    },
                    Err(cause) => {
                        error!("Could not deserialize client request {}: {}", text_msg, cause);
                        cancel_response_streams_close_channel(&mut active_responses, &mut mux_in);
                    }
                }
            } else if raw_msg.is_ping() {
                // No way to send pong??
            } else if raw_msg.is_close() {
                info!("Closing websocket connection (client disconnected)");
                cancel_response_streams_close_channel(&mut active_responses, &mut mux_in);
            } else {
                error!("Expected TEXT Websocket message but got binary");
                cancel_response_streams_close_channel(&mut active_responses, &mut mux_in);
            };
            future::ok(())
        })
        .map_err(|err| {
            error!("Websocket closed with error {}", err);
        })
}

// Wtf, clippy?
#[allow(clippy::cognitive_complexity)]
fn cancel_response_stream(snd_cancel: oneshot::Sender<()>) {
    if snd_cancel.is_canceled() {
        trace!("Not trying to cancel response stream whose cancel rcv has already dropped")
    } else {
        // Let it be said that we could just as well just drop the Sender here,
        // which would also signal the Receiver (with a 'Cancel' error).
        match snd_cancel.send(()) {
            Ok(_) => debug!("Merged Cancel signal into ongoing response stream"),
            Err(_) => debug!("Response stream we are trying to stop has already stopped"),
        }
    }
}

fn cancel_response_streams_close_channel(
    active_responses: &mut HashMap<ReqId, oneshot::Sender<()>>,
    mux_in: &mut mpsc::Sender<Result<Message, warp::Error>>,
) {
    for (_, snd_cancel) in active_responses.drain() {
        cancel_response_stream(snd_cancel);
    }
    mux_in.close_channel();
}

fn serve_request_stream(
    srv: &BoxedService,
    app_id: AppId,
    req_id: ReqId,
    payload: Value,
) -> impl Stream<Item = Result<Message, warp::Error>> {
    let resp_stream = srv
        .serve_ws(app_id, payload)
        .map(move |payload_result| match payload_result {
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

fn serve_request<T: std::fmt::Debug>(
    canceled: oneshot::Receiver<()>,
    srv: &BoxedService,
    app_id: AppId,
    req_id: ReqId,
    payload: Value,
    output: impl Sink<Result<Message, warp::Error>, Error = T>,
) -> impl Future<Output = ()> {
    let response_stream = serve_request_stream(srv, app_id, req_id, payload)
        .take_until_signaled(canceled)
        .map(|item| {
            // We need to re-wrap in an outer result because Sink requires SinkError as the error type
            // but it will pass our inner error unmodified
            Ok(item)
        });

    yield_after(response_stream, INTER_STREAM_FAIRNESS)
        .forward(output)
        .map(|result| {
            if let Err(cause) = result {
                error!("Multiplexing error {:?}", cause);
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

    let raw_msg = Message::text(serde_json::to_string_pretty(&msg).unwrap());

    stream::once(future::ok(Ok(raw_msg))).forward(output).map(|result| {
        if result.is_err() {
            error!("Could not send Error message");
        };
    })
}

pub fn static_app_id() -> impl Filter<Extract = (AppId,), Error = Rejection> + Clone {
    warp::any().and_then(move || {
        future::ok(app_id!("UNUSED")).map_err(|_: std::convert::Infallible| -> Rejection { warp::reject::reject() })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Service;
    use futures::stream;
    use futures::stream::BoxStream;
    use futures::stream::StreamExt;
    use futures::task::Poll;
    use serde::{Deserialize, Serialize};
    use std::net::SocketAddr;
    use std::thread::JoinHandle;
    use warp::Filter;
    use websocket::{ClientBuilder, OwnedMessage};

    #[derive(Serialize, Deserialize)]
    enum Request {
        Count(u64),   // Returns numbers 0..N
        Size(String), // returns data size
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

        fn serve(&self, _app_id: AppId, req: Request) -> BoxStream<'static, Result<Response, String>> {
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
                Request::Size(data) => stream::once(future::ok(Response(data.len() as u64))).boxed(),
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
            })
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
        let rt = tokio::runtime::Runtime::new().expect("could not start tokio runtime");

        let (addr, task) = rt.block_on(async move {
            let ws = warp::path("test_ws").and(super::serve(vec![TestService::new().boxed()], static_app_id()));
            warp::serve(ws).bind_ephemeral(([127, 0, 0, 1], 0))
        });
        rt.spawn(task);
        (addr, rt)
    }

    #[test]
    fn properly_serve_single_request() {
        let (addr, _rt) = start_test_service();

        assert_eq!(
            test_client::<Request, Response>(addr, "test", 0, Request::Count(5)).0,
            vec![Response(0), Response(1), Response(2), Response(3), Response(4)]
        );
    }

    #[test]
    fn properly_serve_large_request() {
        let (addr, _rt) = start_test_service();
        let len = 20_000_000;
        let data: String = std::iter::repeat('x').take(len).collect::<String>();

        assert_eq!(
            test_client::<Request, Response>(addr, "test", 0, Request::Size(data)).0,
            vec![Response(len as u64)]
        );
    }

    #[test]
    fn multiplex_multiple_queries() {
        let (addr, _rt) = start_test_service();

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
    }

    #[test]
    fn report_wrong_endpoint() {
        let (addr, _rt) = start_test_service();

        let (msgs, completion) = test_client::<Request, Response>(addr, "no_such_service", 49, Request::Count(5));

        assert_eq!(msgs, vec![]);

        assert_eq!(
            completion,
            Outgoing::Error {
                request_id: ReqId(49),
                kind: ErrorKind::UnknownEndpoint {
                    endpoint: "no_such_service".to_string(),
                    valid_endpoints: vec!["test".to_string()],
                }
            }
        );
    }

    #[test]
    fn report_badly_formatted_request() {
        let (addr, _rt) = start_test_service();

        let (msgs, completion) = test_client::<BadRequest, Response>(
            addr,
            "test",
            49,
            BadRequest {
                bad_field: "xzy".to_string(),
            },
        );

        assert_eq!(msgs, vec![]);

        if let Outgoing::Error {
            request_id: ReqId(49),
            kind: ErrorKind::BadRequest { message },
        } = completion
        {
            assert!(message.starts_with("unknown variant"));
        } else {
            panic!();
        }
    }

    #[test]
    fn report_service_error() {
        let (addr, _rt) = start_test_service();

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
    }

    #[test]
    fn report_service_panic() {
        let (addr, _rt) = start_test_service();

        let (msgs, completion) = test_client::<Request, Response>(addr, "test", 49, Request::Panic);

        assert_eq!(msgs, vec![]);

        assert_eq!(
            completion,
            Outgoing::Error {
                request_id: ReqId(49),
                kind: ErrorKind::InternalError,
            }
        );
    }

    // Handle service panic
}
