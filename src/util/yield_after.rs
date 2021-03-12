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
use futures::stream::StreamExt;
use futures::task::Context;
use futures::task::Poll;
use futures::Stream;
use pin_project_lite::pin_project;
use std::pin::Pin;

pin_project! {
    /// A stream combinator that resubmits the task after a certain number of elements
    /// have been successfully polled, even if the stream would be ready for further polls.
    ///
    /// This structure is produced by the `Stream::yield_after` method.
    #[must_use = "streams do nothing unless polled"]
    pub struct YieldAfter<S> {
        #[pin]
        stream: S,
        yield_after: u64,
        remaining: u64,
    }
}

impl<S: Stream> YieldAfter<S> {
    pub fn new(stream: S, n: u64) -> Self {
        Self {
            stream,
            yield_after: n,
            remaining: n,
        }
    }
}

impl<S, T> Stream for YieldAfter<S>
where
    S: Stream<Item = T>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut this = self.project();
        if *this.remaining == 0 {
            *this.remaining = *this.yield_after;
            // Immediately reschedule the task. If the executor has fairness, than so
            // does this stream.
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            match this.stream.poll_next_unpin(cx) {
                Poll::Ready(next) => {
                    *this.remaining -= 1;
                    Poll::Ready(next)
                }
                Poll::Pending => {
                    *this.remaining = *this.yield_after;
                    Poll::Pending
                }
            }
        }
    }
}
