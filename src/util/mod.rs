mod take_until_signaled;
mod yield_after;

use futures::{Future, Stream};
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
