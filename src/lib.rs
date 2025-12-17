//! A channel backed by a binary heap.
//!
//! This crate provides a priority queue channel implementation where items are ordered
//! by their natural ordering (using `Ord`). Messages sent through the channel are
//! automatically prioritized, with the highest priority items being received first.
//!
//! # Examples
//!
//! ```
//! use cheap::channel;
//!
//! let (sender, receiver) = channel(10);
//! sender.offer(5).unwrap();
//! sender.offer(3).unwrap();
//! sender.offer(8).unwrap();
//!
//! // Items are received in priority order (highest first)
//! assert_eq!(receiver.poll().unwrap(), 8);
//! assert_eq!(receiver.poll().unwrap(), 5);
//! assert_eq!(receiver.poll().unwrap(), 3);
//! ```

mod heap;

use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::MutexGuard;
use std::sync::{atomic::AtomicUsize, Arc, Condvar, Mutex};
use std::time::Duration;

use crate::heap::FixedHeap;

/// Create a channel with a fixed capacity that is backed by a heap
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>)
where
    T: Ord,
{
    let buffer: FixedHeap<T> = FixedHeap::with_capacity(capacity);
    let shared = Shared {
        buffer: Mutex::new(buffer),
        receivers: Condvar::new(),
        senders: Condvar::new(),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
    };
    let shared = Arc::new(shared);

    let sender = Sender {
        shared: shared.clone(),
    };

    let reciever = Receiver {
        shared: shared.clone(),
    };

    log::debug!("Sender and receiver created");

    (sender, reciever)
}

/// Error type returned when sending to a channel fails.
#[derive(Debug, PartialEq, Eq)]
pub enum SendError<T> {
    /// The channel has been closed and cannot be used for sending any more elements.
    /// This is a terminal and permanent state.
    Closed(T),
    /// The channel is currently locked and in use by another thread.
    /// Attempting to send the element later may yield a different outcome.
    Locked(T),
    /// The channel is full and cannot receive any more elements until at least one element in the
    /// channel has been removed.
    /// Once the channel is no longer full it may be possible to send a new element to the channel.
    Full(T),
}

/// Error type returned when receiving from a channel fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// The channel is currently locked and in use by another thread.
    /// Attempting to receive later may yield a different outcome.
    Locked,
    /// The channel has been closed and no more elements can be received.
    /// This is a terminal and permanent state.
    Closed,
    /// The channel is empty and contains no elements to receive.
    /// Once an element is added to the channel it may be possible to receive.
    Empty,
}

/// The sending side of a channel.
///
/// Values can be sent into the channel using [`Sender::offer`] or [`Sender::offer_timeout`].
/// Multiple senders can be created by cloning this struct.
/// The channel is closed when all senders are dropped.
pub struct Sender<T>
where
    T: Ord,
{
    shared: Arc<Shared<T>>,
}

impl<T> Sender<T>
where
    T: Ord,
{
    /// Attempts to send an item into the channel without blocking.
    ///
    /// This method will return immediately, either successfully sending the item
    /// or returning an error if the channel is full, locked, or closed.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::Closed`] if the channel has been closed.
    /// Returns [`SendError::Locked`] if the channel is currently locked by another thread.
    /// Returns [`SendError::Full`] if the channel is at capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use cheap::channel;
    ///
    /// let (sender, receiver) = channel(10);
    /// sender.offer(42).unwrap();
    /// ```
    pub fn offer(&self, item: T) -> Result<(), SendError<T>> {
        if self.shared.is_closed() {
            log::info!("Sender: Offer failed, channel is closed");
            return Err(SendError::Closed(item));
        }
        match self.shared.buffer.try_lock() {
            Ok(mut buffer) => match buffer.offer(item) {
                Ok(_) => {
                    log::debug!("Sender: Item sent to channel");
                    self.shared.receivers.notify_one();
                    log::trace!("Sender: Notified one receiver");
                    Ok(())
                }
                Err(item) => {
                    log::debug!("Sender: Offer failed, channel was full");
                    Err(SendError::Full(item))
                }
            },
            Err(e) => match e {
                std::sync::TryLockError::Poisoned(_) => {
                    log::error!("Sender: Lock is poisoned");
                    panic!("Lock should never be poisioned")
                }
                std::sync::TryLockError::WouldBlock => {
                    log::debug!("Sender: Unable to acquire lock");
                    Err(SendError::Locked(item))
                }
            },
        }
    }

    /// Attempts to send an item into the channel, waiting up to the specified duration.
    ///
    /// This method will wait for the channel to have capacity if it is currently full or locked.
    /// If the channel does not become available within the specified duration, it returns an error.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::Closed`] if the channel has been closed.
    /// Returns [`SendError::Full`] if the channel is still full after waiting for the duration.
    ///
    /// # Examples
    ///
    /// ```
    /// use cheap::channel;
    /// use std::time::Duration;
    ///
    /// let (sender, receiver) = channel(10);
    /// sender.offer_timeout(42, Duration::from_secs(1)).unwrap();
    /// ```
    pub fn offer_timeout(&self, item: T, duration: Duration) -> Result<(), SendError<T>> {
        match self.offer(item) {
            Ok(_) => Ok(()),
            Err(error) => match error {
                SendError::Closed(_) => Err(error),
                SendError::Locked(item) | SendError::Full(item) => self.wait(item, duration),
            },
        }
    }

    fn wait(&self, item: T, duration: Duration) -> Result<(), SendError<T>> {
        log::trace!("Sender: Waiting for channel to free up capacity or become unlocked");
        let guard = self.shared.buffer.lock().unwrap();
        let mut guard: MutexGuard<'_, FixedHeap<T>> = if !guard.is_full() {
            guard
        } else {
            let (guard, timed_out) = self.shared.senders.wait_timeout(guard, duration).unwrap();
            if timed_out.timed_out() {
                return Err(SendError::Full(item));
            } else if self.shared.is_closed() {
                return Err(SendError::Closed(item));
            } else if guard.is_full() {
                return Err(SendError::Full(item));
            }
            guard
        };

        guard
            .offer(item)
            .map_err(|_| "Unable to offer despite condvar indicating that it should be possible")
            .unwrap();

        drop(guard);
        self.shared.receivers.notify_one();
        Ok(())
    }

    /// Closes the sender by dropping it.
    ///
    /// This is equivalent to dropping the sender, but makes the intent explicit.
    /// When all senders are closed, receivers will eventually receive a [`RecvError::Closed`].
    pub fn close(self) {
        drop(self)
    }
}

impl<T> Clone for Sender<T>
where
    T: Ord,
{
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Relaxed);

        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T>
where
    T: Ord,
{
    fn drop(&mut self) {
        self.shared.sender_count.fetch_sub(1, Relaxed);
        if self.shared.sender_count.load(Acquire) == 0 {
            self.shared.receivers.notify_all();
        }
        log::info!("Sender: Drop invoked");
    }
}

/// The receiving side of a channel.
///
/// Values can be received from the channel using [`Receiver::poll`] or [`Receiver::poll_timeout`].
/// Multiple receivers can be created by cloning this struct.
/// The channel is closed when all receivers are dropped.
pub struct Receiver<T>
where
    T: Ord,
{
    shared: Arc<Shared<T>>,
}

impl<T> Receiver<T>
where
    T: Ord,
{
    /// Attempts to receive an item from the channel without blocking.
    ///
    /// This method will return immediately, either successfully receiving an item
    /// or returning an error if the channel is empty, locked, or closed.
    /// Items are received in priority order, with the highest priority item returned first.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Closed`] if the channel has been closed.
    /// Returns [`RecvError::Locked`] if the channel is currently locked by another thread.
    /// Returns [`RecvError::Empty`] if the channel contains no items.
    ///
    /// # Examples
    ///
    /// ```
    /// use cheap::channel;
    ///
    /// let (sender, receiver) = channel(10);
    /// sender.offer(42).unwrap();
    /// assert_eq!(receiver.poll().unwrap(), 42);
    /// ```
    pub fn poll(&self) -> Result<T, RecvError> {
        log::trace!("Receiver: Polling");
        match self.shared.buffer.try_lock() {
            Ok(mut buffer) => match buffer.poll() {
                Some(item) => {
                    log::trace!(
                        "Receiver: Polled item, notifying one sender about freed up capacity"
                    );
                    self.shared.senders.notify_one();
                    Ok(item)
                }
                None => {
                    if self.shared.is_closed() {
                        log::info!("Receiver: Channel is closed");
                        Err(RecvError::Closed)
                    } else {
                        log::debug!("Receiver: Channel is empty");
                        Err(RecvError::Empty)
                    }
                }
            },
            Err(e) => match e {
                std::sync::TryLockError::Poisoned(_) => {
                    log::error!("Receiver: Lock is poisoned");
                    panic!("Lock should never be poisioned")
                }
                std::sync::TryLockError::WouldBlock => {
                    log::debug!("Receiver: Unable to acquire lock");
                    Err(RecvError::Locked)
                }
            },
        }
    }

    /// Attempts to receive an item from the channel, waiting up to the specified duration.
    ///
    /// This method will wait for the channel to have an item available if it is currently empty or locked.
    /// If an item does not become available within the specified duration, it returns an error.
    /// Items are received in priority order, with the highest priority item returned first.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Closed`] if the channel has been closed.
    /// Returns [`RecvError::Empty`] if the channel is still empty after waiting for the duration.
    ///
    /// # Examples
    ///
    /// ```
    /// use cheap::channel;
    /// use std::time::Duration;
    ///
    /// let (sender, receiver) = channel(10);
    /// sender.offer(42).unwrap();
    /// assert_eq!(receiver.poll_timeout(Duration::from_secs(1)).unwrap(), 42);
    /// ```
    pub fn poll_timeout(&self, duration: Duration) -> Result<T, RecvError> {
        match self.poll() {
            Ok(item) => Ok(item),
            Err(error) => match error {
                RecvError::Closed => Err(RecvError::Closed),
                RecvError::Locked | RecvError::Empty => self.wait(duration),
            },
        }
    }

    fn wait(&self, duration: Duration) -> Result<T, RecvError> {
        let guard = self.shared.buffer.lock().unwrap();
        let mut guard = if !guard.is_empty() {
            guard
        } else {
            let (guard, timed_out) = self.shared.receivers.wait_timeout(guard, duration).unwrap();
            if timed_out.timed_out() {
                return Err(RecvError::Empty);
            }
            guard
        };

        match guard.poll() {
            Some(item) => {
                drop(guard);
                self.shared.senders.notify_one();
                Ok(item)
            }
            None => Err(RecvError::Closed),
        }
    }

    /// Closes the receiver by dropping it.
    ///
    /// This is equivalent to dropping the receiver, but makes the intent explicit.
    /// When all receivers are closed, senders will eventually receive a [`SendError::Closed`].
    pub fn close(self) {
        drop(self)
    }
}

impl<T> Clone for Receiver<T>
where
    T: Ord,
{
    fn clone(&self) -> Self {
        self.shared.receiver_count.fetch_add(1, Relaxed);

        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Receiver<T>
where
    T: Ord,
{
    fn drop(&mut self) {
        self.shared.receiver_count.fetch_sub(1, Relaxed);
        if self.shared.receiver_count.load(Acquire) == 0 {
            self.shared.senders.notify_all();
        }
        log::info!("Receiver: Drop invoked");
    }
}

struct Shared<T>
where
    T: Ord,
{
    buffer: Mutex<FixedHeap<T>>,
    receivers: Condvar,
    senders: Condvar,
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,
}

impl<T> Shared<T>
where
    T: Ord,
{
    fn is_closed(&self) -> bool {
        self.receiver_count.load(Relaxed) == 0 || self.sender_count.load(Relaxed) == 0
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use crate::{RecvError, SendError};

    use super::channel;

    #[test]
    fn test_different_threads_offer_poll() {
        let (send, rec) = channel::<usize>(4);

        thread::spawn(move || {
            send.offer(32).unwrap();
        })
        .join()
        .unwrap();

        thread::spawn(move || {
            assert_eq!(Ok(32), rec.poll());
        })
        .join()
        .unwrap();
    }

    #[test]
    fn test_multiple_producers() {
        let (send0, rec) = channel::<usize>(4);

        let send1 = send0.clone();

        thread::spawn(move || {
            send0.clone().offer(1).unwrap();
        })
        .join()
        .unwrap();

        thread::spawn(move || {
            send1.clone().offer(2).unwrap();
        })
        .join()
        .unwrap();

        thread::spawn(move || {
            assert_eq!(Ok(2), rec.poll());
            assert_eq!(Ok(1), rec.poll());
        })
        .join()
        .unwrap();
    }

    #[test]
    fn test_multiple_consumers() {
        let (send, rec0) = channel::<usize>(4);

        let rec1 = rec0.clone();

        thread::spawn(move || {
            send.offer(32).unwrap();
        })
        .join()
        .unwrap();

        thread::spawn(move || {
            assert_eq!(Ok(32), rec0.poll());
        })
        .join()
        .unwrap();

        thread::spawn(move || {
            assert_eq!(Err(RecvError::Closed), rec1.poll());
        })
        .join()
        .unwrap();
    }

    #[test]
    fn test_send_to_closed_channel() {
        let (send, rec) = channel::<usize>(4);
        drop(rec);

        thread::spawn(move || {
            assert_eq!(Err(SendError::Closed(0)), send.offer(0));
        })
        .join()
        .unwrap();
    }

    #[test]
    fn test_send_to_full_and_closed_channel() {
        let (send, rec) = channel::<usize>(3);
        send.offer(0).unwrap();
        send.offer(1).unwrap();
        send.offer(2).unwrap();
        rec.close();
        assert_eq!(Err(SendError::Closed(4)), send.offer(4));
    }

    #[test]
    fn test_different_threads_with_timeouts() {
        let (send, rec) = channel::<usize>(4);

        thread::spawn(move || {
            send.offer_timeout(32, Duration::from_secs(10)).unwrap();
        });

        thread::spawn(move || {
            assert_eq!(Ok(32), rec.poll_timeout(Duration::from_secs(5)));
        })
        .join()
        .unwrap();
    }
}
