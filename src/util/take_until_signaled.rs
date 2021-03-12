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
use core::fmt;
use core::pin::Pin;
use futures::future::Future;
use futures::stream::{FusedStream, Stream};
use futures::task::{Context, Poll};
use pin_project_lite::pin_project;

pin_project! {
    /// Stream for the [`take_until_signaled`](super::AxStreamExt::take_until_signaled) method.
    #[must_use = "streams do nothing unless polled"]
    pub struct TakeUntilSignaled<St: Stream, F: Future> {
        #[pin]
        stream: St,
        #[pin]
        signal_future: F,
        signaled: bool,
    }
}

impl<St, F> fmt::Debug for TakeUntilSignaled<St, F>
where
    St: Stream + fmt::Debug,
    F: Future + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TakeUntilSignaled")
            .field("stream", &self.stream)
            .field("signal_future", &self.signal_future)
            .field("signaled", &self.signaled)
            .finish()
    }
}

impl<St, F> TakeUntilSignaled<St, F>
where
    St: Stream,
    F: Future,
{
    pub fn new(stream: St, signal_future: F) -> TakeUntilSignaled<St, F> {
        TakeUntilSignaled {
            stream,
            signal_future,
            signaled: false,
        }
    }
}

impl<St, F> Stream for TakeUntilSignaled<St, F>
where
    St: Stream,
    F: Future,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<St::Item>> {
        let this = self.project();

        if this.signal_future.poll(cx).is_ready() {
            *this.signaled = true;
            return Poll::Ready(None);
        }

        this.stream.poll_next(cx)
    }
}

impl<St, F> FusedStream for TakeUntilSignaled<St, F>
where
    St: FusedStream,
    F: Future,
{
    fn is_terminated(&self) -> bool {
        self.signaled || self.stream.is_terminated()
    }
}

#[cfg(test)]
mod tests {
    use crate::util::*;
    use futures::channel::{mpsc, oneshot};
    use futures::stream::{self, StreamExt};

    pub fn wait_for<T: Send + 'static>(
        fut: impl futures::future::Future<Output = T> + Unpin + Send + 'static,
    ) -> T {
        use futures::FutureExt;
        let rt = tokio::runtime::Runtime::new().expect("Could not start tokio runtime");
        rt.block_on(fut.map(Result::<T, ()>::Ok)).expect("boo")
    }

    #[test]
    fn should_work_with_empty_stream() {
        let (s, r) = oneshot::channel::<()>();
        let res = wait_for(
            stream::empty::<u32>()
                .take_until_signaled(r)
                .collect::<Vec<_>>(),
        );
        assert_eq!(res, vec![] as Vec<u32>);
        // If s is dropped too early, r emits an error.
        drop(s);
    }

    #[test]
    fn should_work_with_immediately_true_predicate() {
        let (s, r) = oneshot::channel();
        s.send(()).unwrap();
        let res = wait_for(
            stream::iter(vec![1, 2, 3])
                .take_until_signaled(r)
                .collect::<Vec<_>>(),
        );
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn should_immediately_cancel_if_sender_dropped() {
        // Drop Sender straight away.
        let (_, r) = oneshot::channel::<()>();
        let res = wait_for(
            stream::iter(vec![1, 2, 3])
                .take_until_signaled(r)
                .collect::<Vec<_>>(),
        );
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn should_work_with_later_true_predicate() {
        let (mut s, r) = mpsc::channel(1);
        let res = wait_for(
            stream::iter(vec![1, 2, 3])
                .map(move |x| {
                    if x == 2 {
                        s.try_send(()).unwrap();
                    }
                    x
                })
                .take_until_signaled(r.into_future())
                .collect::<Vec<_>>(),
        );
        assert_eq!(res, vec![1, 2]);
    }

    #[test]
    fn should_work_with_predicate_true_on_last() {
        let (mut s, r) = mpsc::channel(1);
        let res = wait_for(
            stream::iter(vec![1, 2, 3])
                .map(move |x| {
                    if x == 3 {
                        s.try_send(()).unwrap();
                    }
                    x
                })
                .take_until_signaled(r.into_future())
                .collect::<Vec<_>>(),
        );
        assert_eq!(res, vec![1, 2, 3]);
    }

    #[test]
    fn should_work_with_never_true_predicate() {
        let (s, r) = oneshot::channel::<()>();
        let res = wait_for(
            stream::iter(vec![1, 2, 3])
                .take_until_signaled(r)
                .collect::<Vec<_>>(),
        );
        assert_eq!(res, vec![1, 2, 3]);
        // If s is dropped too early, r emits an error.
        drop(s);
    }
}
