use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{atomic::AtomicUsize, Arc, Condvar, Mutex};
use std::time::Duration;

use crate::PrioQueue;

pub fn prio_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>)
where
    T: Ord,
{
    let buffer: PrioQueue<T> = PrioQueue::with_capacity(capacity);
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

    (sender, reciever)
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendError<T> {
    Closed(T),
    Locked(T),
    Full(T),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    Locked,
    Closed,
    Empty,
}

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
    pub fn offer(&self, item: T) -> Result<(), SendError<T>> {
        if self.shared.is_closed() {
            return Err(SendError::Closed(item));
        }
        match self.shared.buffer.try_lock() {
            Ok(mut buffer) => match buffer.offer(item) {
                Ok(_) => {
                    self.shared.receivers.notify_one();
                    Ok(())
                }
                Err(item) => Err(SendError::Full(item)),
            },
            Err(e) => match e {
                std::sync::TryLockError::Poisoned(_) => panic!("Lock should never be poisioned"),
                std::sync::TryLockError::WouldBlock => Err(SendError::Locked(item)),
            },
        }
    }

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
        let guard = self.shared.buffer.lock().unwrap();
        let mut guard = if !guard.is_full() {
            guard
        } else {
            let (guard, timed_out) = self.shared.senders.wait_timeout(guard, duration).unwrap();
            if timed_out.timed_out() {
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
    }
}

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
    pub fn poll(&self) -> Result<T, RecvError> {
        match self.shared.buffer.try_lock() {
            Ok(mut buffer) => match buffer.poll() {
                Some(item) => {
                    self.shared.senders.notify_one();
                    Ok(item)
                }
                None => {
                    if self.shared.is_closed() {
                        Err(RecvError::Closed)
                    } else {
                        Err(RecvError::Empty)
                    }
                }
            },
            Err(e) => match e {
                std::sync::TryLockError::Poisoned(_) => panic!("Lock should never be poisioned"),
                std::sync::TryLockError::WouldBlock => Err(RecvError::Locked),
            },
        }
    }

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
    }
}

struct Shared<T>
where
    T: Ord,
{
    buffer: Mutex<PrioQueue<T>>,
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

    use crate::channel::{RecvError, SendError};

    use super::prio_channel;

    #[test]
    fn test_different_threads_offer_poll() {
        let (send, rec) = prio_channel::<usize>(4);

        thread::spawn(move || {
            send.offer(32).unwrap();
        })
        .join()
        .unwrap();

        thread::spawn(move || {
            assert_eq!(Ok(32), rec.poll());
        });
    }

    #[test]
    fn test_multiple_producers() {
        let (send0, rec) = prio_channel::<usize>(4);

        let send1 = send0.clone();

        thread::spawn(move || {
            send0.clone().offer(32).unwrap();
        })
        .join()
        .unwrap();

        thread::spawn(move || {
            send1.clone().offer(32).unwrap();
        })
        .join()
        .unwrap();

        thread::spawn(move || {
            assert_eq!(Ok(32), rec.poll());
            assert_eq!(Ok(32), rec.poll());
        });
    }

    #[test]
    fn test_multiple_consumers() {
        let (send, rec0) = prio_channel::<usize>(4);

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
        let (send, rec) = prio_channel::<usize>(4);
        drop(rec);

        thread::spawn(move || {
            assert_eq!(Err(SendError::Closed(0)), send.offer(0));
        });
    }

    #[test]
    fn test_different_threads_with_timeouts() {
        let (send, rec) = prio_channel::<usize>(4);

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