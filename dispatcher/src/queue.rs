//! Priority-aware MPSC job queue.
//!
//! Jobs are bucketed by priority class. The scheduler drains higher buckets
//! preferentially but uses a weighted lottery so that lower-priority jobs
//! cannot be starved indefinitely.

use std::collections::VecDeque;

use parking_lot::Mutex;
use rand::Rng;

use crate::types::Job;

#[derive(Debug, Default)]
pub struct PriorityQueue {
    buckets: Mutex<[VecDeque<Job>; 5]>, // 0=low ... 4=critical
}

impl PriorityQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(&self, job: Job) {
        let mut buckets = self.buckets.lock();
        let p = job.priority.clamp(0, 4) as usize;
        buckets[p].push_back(job);
    }

    /// Returns the next job using a weighted lottery: critical=16, high=8,
    /// normal=4, low=2, idle=1. This caps starvation: at worst, an idle job
    /// waits ~31 critical jobs before its expected pick.
    pub fn dequeue(&self) -> Option<Job> {
        const WEIGHTS: [u32; 5] = [1, 2, 4, 8, 16];
        let mut buckets = self.buckets.lock();

        let mut roll: i64 = rand::thread_rng().gen_range(0..WEIGHTS.iter().sum::<u32>()) as i64;
        let mut pick = 4usize;
        for (i, w) in WEIGHTS.iter().enumerate().rev() {
            roll -= *w as i64;
            if roll < 0 {
                pick = i;
                break;
            }
        }
        // Walk down from the chosen bucket; first non-empty wins.
        for i in (0..=pick).rev() {
            if let Some(j) = buckets[i].pop_front() {
                return Some(j);
            }
        }
        for i in (pick + 1)..5 {
            if let Some(j) = buckets[i].pop_front() {
                return Some(j);
            }
        }
        None
    }

    pub fn depth(&self) -> usize {
        let b = self.buckets.lock();
        b.iter().map(|q| q.len()).sum()
    }
}
