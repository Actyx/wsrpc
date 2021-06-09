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
mod take_until_condition;
mod take_until_signaled;
mod yield_after;

use futures::{Future, Stream};
use take_until_condition::TakeUntilCondition;
use take_until_signaled::TakeUntilSignaled;
use yield_after::YieldAfter;

pub trait UtilStreamExt: Stream + Sized {
    /// Take from this stream until the given future completes.
    fn take_until_signaled<F>(self, f: F) -> TakeUntilSignaled<Self, F>
    where
        F: Future,
    {
        TakeUntilSignaled::new(self, f)
    }

    /// Take from this stream up to and including the element on which the predicate turns true.
    fn take_until_condition<Fut, F>(self, f: F) -> TakeUntilCondition<Self, Fut, F>
    where
        F: FnMut(&Self::Item) -> Fut,
        Fut: Future<Output = bool>,
    {
        TakeUntilCondition::new(self, f)
    }

    /// Creates a new stream that will resubmit its `Task` after a certain number
    /// of successfully polled elements. The purpose of this combinator is to
    /// ensure fairness between competing `Stream` instances on the same
    /// executor, especially on event loops. Without yielding a long-running
    /// stream (one that can be polled successfully for a large number of elements),
    /// it can cause other, unrelated streams to starve for execution resources.
    ///
    /// Using this combinator the stream will produce only up to `yield_after`
    /// elements before it returns `Async::NotReady`, then it immediately
    /// unparks its `Task` so the executor can continue the stream later.
    ///
    /// If the original stream suspends itself then the yield counter is
    /// reset, i.e. this limit only takes effect if the original stream
    /// does not suspend itself after the specified elements have been polled.
    /// For example if `yield_after` is set to 100, but the original stream
    /// always returns `Async::NotReady` after 10 elements then this
    /// combinator will not intervene as its counter is reset to 100 every
    /// time the original stream signals it is not ready.
    ///
    /// Please note that this combinator can only ensure fairness if the
    /// underlying executor is fair.
    fn yield_after(self, items: u64) -> YieldAfter<Self> {
        YieldAfter::new(self, items)
    }
}

impl<T: Sized + Stream> UtilStreamExt for T {}
