//! One queue slot per vertex; no unbounded stale heap entries.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
const ABSENT: usize = usize::MAX;

pub(super) struct Queue {
    heap: Vec<usize>,
    position: Vec<usize>,
    // Distance atomics change concurrently outside this queue. Comparing them
    // directly while repairing one decreased key would invalidate the heap
    // precondition: several other keys have already changed but not been
    // repaired. Publish each final distance into queue-owned keys in improve.
    keys: Vec<u64>,
}
impl Queue {
    pub(super) fn new(n: usize) -> Result<Self> {
        let mut heap = Vec::new();
        heap.try_reserve_exact(n).map_err(err)?;
        Ok(Self {
            heap,
            position: allocated(n, ABSENT)?,
            keys: allocated(n, f64::INFINITY.to_bits())?,
        })
    }
    fn less(&self, a: usize, b: usize) -> bool {
        (self.keys[a], a) < (self.keys[b], b)
    }
    fn swap(&mut self, a: usize, b: usize) {
        self.heap.swap(a, b);
        self.position[self.heap[a]] = a;
        self.position[self.heap[b]] = b;
    }
    pub(super) fn improve(&mut self, id: usize, dist: &[AtomicU64]) {
        self.keys[id] = dist[id].load(Ordering::Relaxed);
        if self.position[id] == ABSENT {
            self.position[id] = self.heap.len();
            self.heap.push(id);
        }
        let mut i = self.position[id];
        while i > 0 {
            let p = (i - 1) / 2;
            if !self.less(self.heap[i], self.heap[p]) {
                break;
            }
            self.swap(i, p);
            i = p;
        }
    }
    pub(super) fn peek(&self) -> Option<usize> {
        self.heap.first().copied()
    }
    pub(super) fn pop(&mut self) -> Option<usize> {
        let first = self.peek()?;
        let last = self.heap.pop()?;
        self.position[first] = ABSENT;
        if !self.heap.is_empty() {
            self.heap[0] = last;
            self.position[last] = 0;
            let mut i = 0;
            loop {
                let left = 2 * i + 1;
                if left >= self.heap.len() {
                    break;
                }
                let right = left + 1;
                let child =
                    if right < self.heap.len() && self.less(self.heap[right], self.heap[left]) {
                        right
                    } else {
                        left
                    };
                if !self.less(self.heap[child], self.heap[i]) {
                    break;
                }
                self.swap(i, child);
                i = child;
            }
        }
        Some(first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batched_decreases_keep_minimum_order_independent_of_notification_order() {
        // Simultaneous changes must not leak into comparisons before their own
        // decrease-key operation. All widths can produce this notification
        // order, so this is deterministic rather than a timing-based race test.
        let n = 64;
        let mut random = 42u64;
        let mut next = || {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            random
        };
        for _ in 0..100 {
            let distance: Vec<_> = (0..n)
                .map(|_| AtomicU64::new(((next() % 100_000) as f64).to_bits()))
                .collect();
            let mut queue = Queue::new(n).unwrap();
            for id in 0..n {
                queue.improve(id, &distance);
            }
            for _ in 0..20 {
                let mut updates = Vec::new();
                for (id, d) in distance.iter().enumerate() {
                    if next() % 3 != 0 {
                        let old = f64::from_bits(d.load(Ordering::Relaxed));
                        d.store(
                            (old * (next() % 100) as f64 / 100.0).to_bits(),
                            Ordering::Relaxed,
                        );
                        updates.push(id);
                    }
                }
                for i in (1..updates.len()).rev() {
                    updates.swap(i, (next() as usize) % (i + 1));
                }
                for id in updates {
                    queue.improve(id, &distance);
                }
                for i in 1..queue.heap.len() {
                    assert!(!queue.less(queue.heap[i], queue.heap[(i - 1) / 2]));
                }
            }
            let mut expected: Vec<_> = (0..n).collect();
            expected.sort_by_key(|&id| (distance[id].load(Ordering::Relaxed), id));
            for id in expected {
                assert_eq!(queue.pop(), Some(id));
            }
            assert_eq!(queue.pop(), None);
        }
    }
}
