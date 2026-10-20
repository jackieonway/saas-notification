//! Bounded, recipient-ordered scheduling of disposable notification hints.
//!
//! The database remains authoritative. Overload and shutdown discard hints;
//! they never mutate the persisted inbox. A single coordinator owns at most
//! `concurrency` tasks, while admission checks happen before any task is spawned.

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    hash::Hash,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};
use tokio::{sync::Notify, task::JoinSet};

/// Independent limits on recipient identities, queued hints and active handlers.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub(crate) max_partitions: usize,
    pub(crate) per_partition_queue: usize,
    pub(crate) total_queue: usize,
    pub(crate) concurrency: usize,
}

/// Consistent gauges and monotonically increasing, saturating counters.
///
/// `queued` excludes active work. Memory is bounded by `total_queue` queued
/// hints plus `concurrency` active hints, and by `max_partitions` identities.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) partitions: usize,
    pub(crate) queued: usize,
    pub(crate) active: usize,
    pub(crate) accepted: u64,
    pub(crate) dropped: u64,
    pub(crate) completed: u64,
    pub(crate) failed: u64,
    pub(crate) cancelled: u64,
    pub(crate) queue_wait_micros: u64,
    pub(crate) max_queue_wait_micros: u64,
}

struct Queued<V> {
    value: V,
    admitted: Instant,
}

struct Partition<V> {
    queue: VecDeque<Queued<V>>,
    active: bool,
}

struct State<K, V> {
    partitions: HashMap<K, Partition<V>>,
    ready: VecDeque<K>,
    snapshot: Snapshot,
    closed: bool,
}

struct Shared<K, V> {
    limits: Limits,
    state: Mutex<State<K, V>>,
    changed: Notify,
}

impl<K, V> Shared<K, V> {
    fn lock(&self) -> MutexGuard<'_, State<K, V>> {
        // No application code or await runs in this critical section. A panic
        // elsewhere must still allow shutdown to release bounded queued values.
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        state.snapshot.dropped = state
            .snapshot
            .dropped
            .saturating_add(state.snapshot.queued as u64);
        state.snapshot.queued = 0;
        state.ready.clear();
        state.partitions.retain(|_, partition| {
            partition.queue.clear();
            partition.active
        });
        state.snapshot.partitions = state.partitions.len();
        drop(state);
        // notify_one stores a permit when the coordinator is between awaits.
        self.changed.notify_one();
    }
}

/// Admission handle. Cloning it does not create workers or an ingress queue.
pub(crate) struct Sender<K, V> {
    shared: Arc<Shared<K, V>>,
}

impl<K, V> Clone for Sender<K, V> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

/// The sole coordinator; dropping it also closes admission and releases queues.
pub(crate) struct Runner<K, V> {
    shared: Arc<Shared<K, V>>,
}

type Channel<K, V> = (Sender<K, V>, Runner<K, V>);

/// Create a scheduler without spawning any tasks.
pub(crate) fn channel<K, V>(limits: Limits) -> Result<Channel<K, V>, &'static str> {
    if limits.max_partitions == 0
        || limits.per_partition_queue == 0
        || limits.total_queue == 0
        || limits.concurrency == 0
    {
        return Err("notification scheduler limits must be positive");
    }
    let shared = Arc::new(Shared {
        limits,
        state: Mutex::new(State {
            partitions: HashMap::new(),
            ready: VecDeque::new(),
            snapshot: Snapshot::default(),
            closed: false,
        }),
        changed: Notify::new(),
    });
    Ok((
        Sender {
            shared: Arc::clone(&shared),
        },
        Runner { shared },
    ))
}

impl<K: Clone + Eq + Hash, V> Sender<K, V> {
    /// Queue one hint, or immediately discard it when a limit is reached.
    ///
    /// No producer waits for a slow recipient. This function must receive a
    /// complete user/tenant/organization key; there is no identity inference.
    pub(crate) fn try_enqueue(&self, key: K, value: V) -> bool {
        let mut state = self.shared.lock();
        let limits = self.shared.limits;
        let full = state.closed
            || state.snapshot.queued >= limits.total_queue
            || state
                .partitions
                .get(&key)
                .is_some_and(|partition| partition.queue.len() >= limits.per_partition_queue)
            || (!state.partitions.contains_key(&key)
                && state.partitions.len() >= limits.max_partitions);
        if full {
            state.snapshot.dropped = state.snapshot.dropped.saturating_add(1);
            return false;
        }
        let partition = state
            .partitions
            .entry(key.clone())
            .or_insert_with(|| Partition {
                queue: VecDeque::new(),
                active: false,
            });
        let ready = !partition.active && partition.queue.is_empty();
        partition.queue.push_back(Queued {
            value,
            admitted: Instant::now(),
        });
        if ready {
            state.ready.push_back(key);
        }
        state.snapshot.partitions = state.partitions.len();
        state.snapshot.queued += 1;
        state.snapshot.accepted = state.snapshot.accepted.saturating_add(1);
        drop(state);
        self.shared.changed.notify_one();
        true
    }
}

impl<K, V> Sender<K, V> {
    pub(crate) fn snapshot(&self) -> Snapshot {
        self.shared.lock().snapshot
    }

    /// Stop admission, drop queued hints and request cancellation of active work.
    /// Await the runner to ensure all active handler futures have been dropped.
    pub(crate) fn close(&self) {
        self.shared.close();
    }
}

enum Outcome {
    Completed,
    Failed,
    Cancelled,
}

impl<K: Clone + Eq + Hash + Send + 'static, V: Send + 'static> Runner<K, V> {
    fn take_ready(&self) -> Option<(K, V)> {
        let mut state = self.shared.lock();
        if state.closed {
            return None;
        }
        let key = state.ready.pop_front()?;
        let partition = state.partitions.get_mut(&key)?;
        let queued = partition.queue.pop_front()?;
        partition.active = true;
        state.snapshot.active += 1;
        state.snapshot.queued -= 1;
        let waited = u64::try_from(queued.admitted.elapsed().as_micros()).unwrap_or(u64::MAX);
        state.snapshot.queue_wait_micros = state.snapshot.queue_wait_micros.saturating_add(waited);
        state.snapshot.max_queue_wait_micros = state.snapshot.max_queue_wait_micros.max(waited);
        Some((key, queued.value))
    }

    fn finished(&self, key: K, outcome: Outcome) {
        let mut state = self.shared.lock();
        state.snapshot.active = state.snapshot.active.saturating_sub(1);
        match outcome {
            Outcome::Completed => {
                state.snapshot.completed = state.snapshot.completed.saturating_add(1);
            }
            Outcome::Failed => state.snapshot.failed = state.snapshot.failed.saturating_add(1),
            Outcome::Cancelled => {
                state.snapshot.cancelled = state.snapshot.cancelled.saturating_add(1);
            }
        }
        let ready = state.partitions.get_mut(&key).is_some_and(|partition| {
            partition.active = false;
            !partition.queue.is_empty()
        });
        if ready {
            state.ready.push_back(key);
        } else {
            state.partitions.remove(&key);
        }
        state.snapshot.partitions = state.partitions.len();
    }

    /// Run one bounded task per active recipient, with FIFO work inside each key.
    ///
    /// A key re-enters the tail of the ready list after completion, so a busy
    /// recipient cannot monopolize a worker. The handler must enforce its own
    /// external HTTP/DB timeout and per-recipient connection limits.
    pub(crate) async fn run<F, Fut>(self, handler: F)
    where
        F: Fn(K, V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let mut workers = JoinSet::new();
        let mut running = HashMap::new();
        loop {
            if self.shared.lock().closed {
                workers.abort_all();
                while let Some(result) = workers.join_next_with_id().await {
                    let (id, outcome) = match result {
                        Ok((id, ())) => (id, Outcome::Completed),
                        Err(error) => (
                            error.id(),
                            if error.is_cancelled() {
                                Outcome::Cancelled
                            } else {
                                Outcome::Failed
                            },
                        ),
                    };
                    if let Some(key) = running.remove(&id) {
                        self.finished(key, outcome);
                    }
                }
                return;
            }
            while workers.len() < self.shared.limits.concurrency {
                let Some((key, value)) = self.take_ready() else {
                    break;
                };
                let handler = Arc::clone(&handler);
                let handler_key = key.clone();
                let worker = workers.spawn(async move { handler(handler_key, value).await });
                running.insert(worker.id(), key);
            }
            tokio::select! {
                _ = self.shared.changed.notified() => {},
                result = workers.join_next_with_id(), if !workers.is_empty() => {
                    if let Some(result) = result {
                        let (id, outcome) = match result {
                            Ok((id, ())) => (id, Outcome::Completed),
                            Err(error) => (error.id(), if error.is_cancelled() { Outcome::Cancelled } else { Outcome::Failed }),
                        };
                        if let Some(key) = running.remove(&id) {
                            self.finished(key, outcome);
                        }
                    }
                }
            }
        }
    }
}

impl<K, V> Drop for Runner<K, V> {
    fn drop(&mut self) {
        self.shared.close();
        let mut state = self.shared.lock();
        // JoinSet drops/aborts worker futures before this coordinator is dropped.
        state.snapshot.cancelled = state
            .snapshot
            .cancelled
            .saturating_add(state.snapshot.active as u64);
        state.snapshot.active = 0;
        state.partitions.clear();
        state.snapshot.partitions = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, Semaphore};

    fn limits() -> Limits {
        Limits {
            max_partitions: 4,
            per_partition_queue: 4,
            total_queue: 8,
            concurrency: 2,
        }
    }

    async fn received<T>(receiver: &mut mpsc::UnboundedReceiver<T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .expect("handler should make progress")
            .expect("handler event channel remains open")
    }

    async fn completed<K, V>(sender: &Sender<K, V>, expected: u64) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while sender.snapshot().completed != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("coordinator must account for completed handlers");
    }

    #[tokio::test]
    async fn slow_recipient_does_not_block_another_and_each_key_remains_ordered() {
        let (sender, runner) = channel(limits()).unwrap();
        let (events, mut receiver) = mpsc::unbounded_channel();
        let slow = Arc::new(Semaphore::new(0));
        let release = Arc::clone(&slow);
        let task = tokio::spawn(runner.run(move |key: &'static str, value: usize| {
            let slow = Arc::clone(&slow);
            let events = events.clone();
            async move {
                events.send((key, value, "start")).unwrap();
                if key == "slow" && value == 1 {
                    slow.acquire().await.unwrap().forget();
                }
                events.send((key, value, "end")).unwrap();
            }
        }));
        assert!(sender.try_enqueue("slow", 1));
        assert_eq!(received(&mut receiver).await, ("slow", 1, "start"));
        assert!(sender.try_enqueue("slow", 2));
        assert!(sender.try_enqueue("fast", 1));
        assert_eq!(received(&mut receiver).await, ("fast", 1, "start"));
        assert_eq!(received(&mut receiver).await, ("fast", 1, "end"));
        assert!(
            receiver.try_recv().is_err(),
            "same-key second hint must still wait"
        );
        release.add_permits(1);
        assert_eq!(received(&mut receiver).await, ("slow", 1, "end"));
        assert_eq!(received(&mut receiver).await, ("slow", 2, "start"));
        assert_eq!(received(&mut receiver).await, ("slow", 2, "end"));
        completed(&sender, 3).await;
        sender.close();
        task.await.unwrap();
        let snapshot = sender.snapshot();
        assert_eq!(
            (snapshot.partitions, snapshot.queued, snapshot.active),
            (0, 0, 0)
        );
        assert_eq!(snapshot.accepted, 3);
        assert_eq!(snapshot.completed, 3);
        assert_eq!(snapshot.failed, 0);
    }

    #[tokio::test]
    async fn flood_respects_every_admission_limit_and_global_concurrency() {
        let config = Limits {
            max_partitions: 3,
            per_partition_queue: 2,
            total_queue: 4,
            concurrency: 2,
        };
        let (sender, runner) = channel(config).unwrap();
        assert!(sender.try_enqueue("a", 1));
        assert!(sender.try_enqueue("a", 2));
        assert!(!sender.try_enqueue("a", 3)); // per-partition limit
        assert!(sender.try_enqueue("b", 1));
        assert!(sender.try_enqueue("c", 1));
        assert!(!sender.try_enqueue("d", 1)); // total queue and partition limits
        let snapshot = sender.snapshot();
        assert_eq!(
            (snapshot.partitions, snapshot.queued, snapshot.active),
            (3, 4, 0)
        );
        assert_eq!((snapshot.accepted, snapshot.dropped), (4, 2));

        let (events, mut receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(runner.run(move |key: &'static str, _value: usize| {
            let events = events.clone();
            async move {
                events.send(key).unwrap();
                std::future::pending::<()>().await;
            }
        }));
        let _ = received(&mut receiver).await;
        let _ = received(&mut receiver).await;
        assert!(!sender.try_enqueue("new-partition", 1)); // partition cap alone
        for _ in 0..10_000 {
            let _ = sender.try_enqueue("a", 9);
            let snapshot = sender.snapshot();
            assert!(snapshot.partitions <= config.max_partitions);
            assert!(snapshot.queued <= config.total_queue);
            assert!(snapshot.active <= config.concurrency);
        }
        let snapshot = sender.snapshot();
        assert_eq!(snapshot.active, 2);
        assert_eq!(snapshot.queued, 3);
        assert_eq!(snapshot.accepted, 5);
        assert_eq!(snapshot.dropped, 10_002);
        sender.close();
        task.await.unwrap();
        let snapshot = sender.snapshot();
        assert_eq!(
            (snapshot.partitions, snapshot.queued, snapshot.active),
            (0, 0, 0)
        );
        assert_eq!(snapshot.cancelled, 2);
        assert_eq!(snapshot.dropped, 10_005);
        assert!(!sender.try_enqueue("a", 10));
        assert_eq!(sender.snapshot().dropped, 10_006);
    }

    #[test]
    fn total_queue_limit_applies_even_with_free_partitions_and_per_key_capacity() {
        let (sender, _runner) = channel(Limits {
            total_queue: 2,
            ..limits()
        })
        .unwrap();
        assert!(sender.try_enqueue("a", 1));
        assert!(sender.try_enqueue("b", 1));
        assert!(!sender.try_enqueue("a", 2));
        assert!(!sender.try_enqueue("c", 1));
        let snapshot = sender.snapshot();
        assert_eq!(
            (snapshot.queued, snapshot.partitions, snapshot.active),
            (2, 2, 0)
        );
        assert_eq!((snapshot.accepted, snapshot.dropped), (2, 2));
    }

    #[tokio::test]
    async fn shutdown_drops_pending_values_and_awaits_active_future_cancellation() {
        struct Value(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Value {
            fn drop(&mut self) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let (sender, runner) = channel(limits()).unwrap();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (events, mut receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(runner.run(move |key: usize, value: Value| {
            let events = events.clone();
            async move {
                events.send(key).unwrap();
                std::future::pending::<()>().await;
                drop(value);
            }
        }));
        assert!(sender.try_enqueue(1, Value(Arc::clone(&dropped))));
        assert_eq!(received(&mut receiver).await, 1);
        assert!(sender.try_enqueue(1, Value(Arc::clone(&dropped))));
        sender.close();
        task.await.unwrap();
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 2);
        let snapshot = sender.snapshot();
        assert_eq!(
            (snapshot.queued, snapshot.active, snapshot.partitions),
            (0, 0, 0)
        );
        assert_eq!(
            (snapshot.dropped, snapshot.cancelled, snapshot.completed),
            (1, 1, 0)
        );
    }

    #[tokio::test]
    async fn a_panicking_handler_releases_its_partition_and_other_work_continues() {
        let (sender, runner) = channel(Limits {
            concurrency: 1,
            ..limits()
        })
        .unwrap();
        let (events, mut receiver) = mpsc::unbounded_channel();
        assert!(sender.try_enqueue("a", 1));
        assert!(sender.try_enqueue("a", 2));
        assert!(sender.try_enqueue("b", 1));
        let task = tokio::spawn(runner.run(move |key: &'static str, value: usize| {
            let events = events.clone();
            async move {
                if key == "a" && value == 1 {
                    panic!("injected handler failure");
                }
                events.send((key, value)).unwrap();
            }
        }));
        assert_eq!(received(&mut receiver).await, ("b", 1));
        assert_eq!(received(&mut receiver).await, ("a", 2));
        completed(&sender, 2).await;
        sender.close();
        task.await.unwrap();
        let snapshot = sender.snapshot();
        assert_eq!((snapshot.failed, snapshot.completed), (1, 2));
        assert_eq!(
            (snapshot.partitions, snapshot.queued, snapshot.active),
            (0, 0, 0)
        );
    }

    #[test]
    fn invalid_limits_and_unstarted_runner_shutdown_are_explicit() {
        assert!(channel::<usize, usize>(Limits {
            total_queue: 0,
            ..limits()
        })
        .is_err());
        let (sender, runner) = channel(limits()).unwrap();
        assert!(sender.try_enqueue(1, 1));
        drop(runner);
        assert!(!sender.try_enqueue(1, 2));
        let snapshot = sender.snapshot();
        assert_eq!(
            (snapshot.queued, snapshot.active, snapshot.partitions),
            (0, 0, 0)
        );
        assert_eq!(snapshot.dropped, 2);
    }
}
