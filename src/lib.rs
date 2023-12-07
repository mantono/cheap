pub mod channel;

use std::collections::BinaryHeap;

/// A priority queue with a fixed capacity
pub struct PrioQueue<T>
where
    T: Ord,
{
    data: BinaryHeap<T>,
    capacity: usize,
}

impl<T> PrioQueue<T>
where
    T: Ord,
{
    pub fn new() -> PrioQueue<T> {
        Self::with_capacity(32)
    }

    pub fn with_capacity(capacity: usize) -> PrioQueue<T> {
        if capacity == 0 {
            panic!("Tried to create an empty queue")
        }
        let data = BinaryHeap::with_capacity(capacity);
        PrioQueue { data, capacity }
    }

    pub fn offer(&mut self, item: T) -> Result<(), T> {
        if self.is_full() {
            Err(item)
        } else {
            self.data.push(item);
            Ok(())
        }
    }

    pub fn poll(&mut self) -> Option<T> {
        self.data.pop()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.size() == self.capacity
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl<T> Default for PrioQueue<T>
where
    T: Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::PrioQueue;

    #[test]
    fn test_new() {
        let _: PrioQueue<char> = PrioQueue::new();
    }

    #[test]
    fn test_is_empty() {
        let buffer: PrioQueue<char> = PrioQueue::new();
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_capacity() {
        let buffer: PrioQueue<char> = PrioQueue::with_capacity(4);
        assert_eq!(4, buffer.capacity());
    }

    #[test]
    fn test_offer() {
        let mut buffer: PrioQueue<usize> = PrioQueue::with_capacity(1);
        assert!(buffer.offer(0).is_ok());
    }

    #[test]
    fn test_offer_full() {
        let mut buffer: PrioQueue<usize> = PrioQueue::with_capacity(1);
        assert!(buffer.offer(0).is_ok());
        assert!(buffer.offer(0).is_err());
    }

    #[test]
    fn test_is_full() {
        let mut buffer: PrioQueue<usize> = PrioQueue::with_capacity(1);
        assert!(!buffer.is_full());
        assert!(buffer.offer(0).is_ok());
        assert!(buffer.is_full());
    }

    #[test]
    fn test_size() {
        let mut buffer: PrioQueue<()> = PrioQueue::new();
        assert_eq!(0, buffer.size());
        assert!(buffer.offer(()).is_ok());
        assert_eq!(1, buffer.size());
        assert!(buffer.poll().is_some());
        assert_eq!(0, buffer.size());
    }

    #[test]
    fn test_priority() {
        let mut buffer: PrioQueue<usize> = PrioQueue::new();
        buffer.offer(1).unwrap();
        buffer.offer(3).unwrap();
        buffer.offer(2).unwrap();
        assert_eq!(Some(3), buffer.poll());
        assert_eq!(Some(2), buffer.poll());
        assert_eq!(Some(1), buffer.poll());
    }
}
