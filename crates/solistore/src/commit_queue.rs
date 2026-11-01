//! The two protocols the commit path stands on, apart from what they carry,
//! so `loom` can check them under every interleaving (`--cfg loom`, see the
//! tests at the end):
//!
//! - [`GroupQueue`]: writers queue up; the one at the head becomes the
//!   leader, takes the batches waiting behind it as one group, does the
//!   group's work and tells every member what to do next. Writers arriving
//!   meanwhile form the next group. There is one leader at a time.
//! - [`Publisher`]: groups finish their memtable inserts in any order, and
//!   the visible sequence number only moves over groups that are finished
//!   and have no unfinished group before them. A writer returns once its
//!   group is visible.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::Ordering;

use crate::sync::{spin_until, Arc, AtomicBool, AtomicU64, AtomicUsize, Condvar, Mutex};

/// One writer's place in the queue: its item, and the slot its group's
/// leader fills in for it.
pub struct Ticket<T, S> {
    pub item: T,
    /// Set by the leader under the queue lock.
    slot: Mutex<Option<S>>,
    /// `slot` is set: lets a waiting writer spin without the queue lock.
    ready: AtomicBool,
}

struct QueueState<T, S> {
    waiting: VecDeque<Arc<Ticket<T, S>>>,
    leader_active: bool,
}

pub struct GroupQueue<T, S> {
    state: Mutex<QueueState<T, S>>,
    cv: Condvar,
    /// Mirrors of the queue's state, readable while spinning.
    queued: AtomicUsize,
    leader_busy: AtomicBool,
    /// Members of the last group, to gather as many for the next one.
    last_group: AtomicUsize,
}

/// What a writer entering the queue is to do.
pub enum Turn<'a, T, S: Clone> {
    /// A leader took this writer's item into its group and says `slot`;
    /// the ticket gives the item back.
    Followed(S, Arc<Ticket<T, S>>),
    /// This writer leads a group, its own item first.
    Lead(Leadership<'a, T, S>),
}

/// The leader's hold on its group. [`Leadership::finish`] hands out the
/// slots and frees the leader's place; dropped without it — unwinding out
/// of the group's work — `abandoned` is called once, even for a leader
/// alone in its group, and every other member gets a copy of what it
/// returns, so the queue cannot wedge.
pub struct Leadership<'a, T, S: Clone> {
    queue: &'a GroupQueue<T, S>,
    pub group: Vec<Arc<Ticket<T, S>>>,
    abandoned: &'a dyn Fn() -> S,
    finished: bool,
}

impl<T, S> Default for GroupQueue<T, S> {
    fn default() -> Self {
        GroupQueue {
            state: Mutex::new(QueueState {
                waiting: VecDeque::new(),
                leader_active: false,
            }),
            cv: Condvar::new(),
            queued: AtomicUsize::new(0),
            leader_busy: AtomicBool::new(false),
            last_group: AtomicUsize::new(1),
        }
    }
}

impl<T, S: Clone> GroupQueue<T, S> {
    /// Queue `item` and wait for a leader to handle it, or to lead.
    ///
    /// A leader takes every item waiting behind it, up to `max_bytes` by
    /// `size` (its own item always). With `gather`, a leader that finds
    /// fewer waiting than its predecessor's group first spins a moment for
    /// them: a group that will be fsynced is worth gathering, since when
    /// the last group's writers come back the first one would otherwise pay
    /// a whole fsync alone while the rest queue up behind it.
    pub fn enter<'a>(
        &'a self,
        item: T,
        gather: bool,
        max_bytes: usize,
        size: impl Fn(&T) -> usize,
        abandoned: &'a dyn Fn() -> S,
    ) -> Turn<'a, T, S> {
        let me = Arc::new(Ticket {
            item,
            slot: Mutex::new(None),
            ready: AtomicBool::new(false),
        });
        let mut queue = self.state.lock();
        queue.waiting.push_back(me.clone());
        self.queued.fetch_add(1, Ordering::Relaxed);
        let mut spun = false;
        loop {
            if me.ready.load(Ordering::Acquire) {
                let slot = me.slot.lock().take().expect("ready means set");
                return Turn::Followed(slot, me);
            }
            let at_head = queue.waiting.front().is_some_and(|p| Arc::ptr_eq(p, &me));
            if at_head && !queue.leader_active {
                break;
            }
            if !spun {
                // Most waits end within microseconds: spin before sleeping.
                spun = true;
                drop(queue);
                spin_until(|| {
                    me.ready.load(Ordering::Acquire) || !self.leader_busy.load(Ordering::Acquire)
                });
                queue = self.state.lock();
                continue;
            }
            spun = false;
            self.cv.wait(&mut queue);
        }
        queue.leader_active = true;
        self.leader_busy.store(true, Ordering::Release);
        let want = self.last_group.load(Ordering::Relaxed);
        if gather && queue.waiting.len() < want {
            drop(queue);
            spin_until(|| self.queued.load(Ordering::Acquire) >= want);
            queue = self.state.lock();
        }
        let mut group = Vec::new();
        let mut bytes = 0;
        while let Some(next) = queue.waiting.front() {
            let next_size = size(&next.item);
            if !group.is_empty() && bytes + next_size > max_bytes {
                break;
            }
            bytes += next_size;
            group.push(queue.waiting.pop_front().expect("front exists"));
        }
        self.queued.fetch_sub(group.len(), Ordering::Relaxed);
        self.last_group.store(group.len(), Ordering::Relaxed);
        drop(queue);
        debug_assert!(Arc::ptr_eq(&group[0], &me), "the leader heads its group");
        Turn::Lead(Leadership {
            queue: self,
            group,
            abandoned,
            finished: false,
        })
    }
}

impl<T, S: Clone> Leadership<'_, T, S> {
    /// Give each member `slot(index, ticket)`, free the leader's place for
    /// the next group, and return the leader's own slot with the group,
    /// whose first ticket is the leader's own.
    pub fn finish(
        mut self,
        mut slot: impl FnMut(usize, &Ticket<T, S>) -> S,
    ) -> (S, Vec<Arc<Ticket<T, S>>>) {
        let mut mine = None;
        let mut queue = self.queue.state.lock();
        for (i, ticket) in self.group.iter().enumerate() {
            let s = slot(i, ticket);
            if i == 0 {
                mine = Some(s);
            } else {
                *ticket.slot.lock() = Some(s);
                ticket.ready.store(true, Ordering::Release);
            }
        }
        // Only now: should `slot` panic, the drop still frees the leader's
        // place and tells every member (the queue lock, a local here, is
        // released first by the unwinding).
        self.finished = true;
        queue.leader_active = false;
        self.queue.leader_busy.store(false, Ordering::Release);
        drop(queue);
        self.queue.cv.notify_all();
        (
            mine.expect("the leader is in its own group"),
            std::mem::take(&mut self.group),
        )
    }
}

impl<T, S: Clone> Drop for Leadership<'_, T, S> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let slot = (self.abandoned)();
        let mut queue = self.queue.state.lock();
        for ticket in &self.group[1..] {
            *ticket.slot.lock() = Some(slot.clone());
            ticket.ready.store(true, Ordering::Release);
        }
        queue.leader_active = false;
        self.queue.leader_busy.store(false, Ordering::Release);
        drop(queue);
        self.queue.cv.notify_all();
    }
}

/// The visible sequence number, and the groups done but not yet visible.
pub struct Publisher {
    /// The highest sequence number a reader may see. Every read loads it:
    /// on a cache line of its own, away from `done`, which every
    /// publication locks. Next to it, the update phase ran 3.5% slower than
    /// when it sat apart in `Inner`.
    visible: CacheLine<AtomicU64>,
    /// Groups fully inserted but not yet visible, `first -> last`: a group
    /// can finish before an earlier one.
    done: Mutex<BTreeMap<u64, u64>>,
    cv: Condvar,
}

/// A value alone on its cache line (128 bytes: adjacent-line prefetch on
/// x86, the line size on Apple's cores).
#[repr(align(128))]
struct CacheLine<T>(T);

impl<T> std::ops::Deref for CacheLine<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl Publisher {
    pub fn new(visible: u64) -> Self {
        Publisher {
            visible: CacheLine(AtomicU64::new(visible)),
            done: Mutex::new(BTreeMap::new()),
            cv: Condvar::new(),
        }
    }

    pub fn visible(&self) -> u64 {
        self.visible.load(Ordering::Acquire)
    }

    /// Record the group `first..=last` as fully inserted, and make visible
    /// every group up to the first one still being inserted. Returns once
    /// `last` is visible, so a writer always reads its own write.
    ///
    /// Whichever thread finishes the oldest pending group publishes those
    /// after it that are already done: publication used to be one thread
    /// per group in order, each woken by the one before, and with more
    /// writers than cores (SoliDB's SDBQL writes) that chain of wake-ups
    /// capped commits at ~50k a second with groups of one batch.
    pub fn publish(&self, first: u64, last: u64) {
        let mut done = self.done.lock();
        done.insert(first, last);
        let mut visible = self.visible.load(Ordering::Acquire);
        let start = visible;
        while let Some(last) = done.remove(&(visible + 1)) {
            visible = last;
        }
        if visible == start {
            // An earlier group is still being inserted; whoever finishes it
            // publishes this one.
            drop(done);
            self.wait(last);
            return;
        }
        // Under the lock, so a writer that just found the old value is
        // already waiting when the notification goes out.
        self.visible.store(visible, Ordering::Release);
        drop(done);
        self.cv.notify_all();
    }

    /// Wait until `seq` is visible.
    pub fn wait(&self, seq: u64) {
        if spin_until(|| self.visible.load(Ordering::Acquire) >= seq) {
            return;
        }
        let mut done = self.done.lock();
        while self.visible.load(Ordering::Acquire) < seq {
            self.cv.wait(&mut done);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_finished_out_of_order_become_visible_in_order() {
        let p = Arc::new(Publisher::new(0));
        let late = {
            let p = p.clone();
            // Group 3..=4 finishes first and waits for 1..=2.
            std::thread::spawn(move || p.publish(3, 4))
        };
        while p.done.lock().is_empty() {
            std::thread::yield_now();
        }
        assert_eq!(p.visible(), 0, "nothing visible over an unfinished group");
        p.publish(1, 2);
        late.join().unwrap();
        assert_eq!(p.visible(), 4);
    }

    #[test]
    fn a_leader_hands_its_group_their_slots() {
        let q: Arc<GroupQueue<u32, u32>> = Arc::new(GroupQueue::default());
        let abandoned = || u32::MAX;
        let turn = q.enter(7, false, usize::MAX, |_| 1, &abandoned);
        let mine = match turn {
            Turn::Lead(lead) => {
                assert_eq!(lead.group.len(), 1);
                lead.finish(|i, t| t.item * 10 + i as u32).0
            }
            Turn::Followed(..) => panic!("alone, it leads"),
        };
        assert_eq!(mine, 70);
    }
}

/// Model checks: `RUSTFLAGS="--cfg loom" cargo test --release --lib loom`.
/// Each runs its threads under every interleaving `loom` explores, with a
/// bound on preemptions so the runs end.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    fn model(f: impl Fn() + Sync + Send + 'static) {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(f);
    }

    /// Readers never see a group before it and every group before it are
    /// fully inserted, and each writer returns with its group visible,
    /// whatever order the groups finish in.
    #[test]
    fn visibility_never_runs_ahead_of_inserts() {
        model(|| {
            let p = Arc::new(Publisher::new(0));
            let inserted: Arc<[AtomicBool; 3]> = Arc::new([
                AtomicBool::new(false),
                AtomicBool::new(false),
                AtomicBool::new(false),
            ]);
            let writers: Vec<_> = (1..=2u64)
                .map(|seq| {
                    let (p, inserted) = (p.clone(), inserted.clone());
                    thread::spawn(move || {
                        inserted[seq as usize].store(true, Ordering::Release);
                        p.publish(seq, seq);
                        // A writer reads at least its own store: what it
                        // sees visible must be inserted, its own group
                        // and every one before it.
                        let v = p.visible();
                        assert!(v >= seq, "returned before its write was visible");
                        for s in 1..=v {
                            assert!(
                                inserted[s as usize].load(Ordering::Acquire),
                                "saw {s} early"
                            );
                        }
                    })
                })
                .collect();
            let v = p.visible();
            for seq in 1..=v {
                assert!(
                    inserted[seq as usize].load(Ordering::Acquire),
                    "saw {seq} before insert"
                );
            }
            for w in writers {
                w.join().unwrap();
            }
            assert_eq!(p.visible(), 2);
        });
    }

    /// Every writer's item is handled exactly once, by one leader at a
    /// time, and every writer gets the slot its leader meant for it.
    #[test]
    fn one_leader_at_a_time_and_every_item_handled_once() {
        model(|| {
            let q: Arc<GroupQueue<u32, (u32, u32)>> = Arc::new(GroupQueue::default());
            let leading = Arc::new(AtomicBool::new(false));
            let handled = Arc::new(Mutex::new(Vec::new()));
            let writers: Vec<_> = (0..3u32)
                .map(|id| {
                    let (q, leading, handled) = (q.clone(), leading.clone(), handled.clone());
                    thread::spawn(move || {
                        let abandoned = || (u32::MAX, u32::MAX);
                        let slot = match q.enter(id, false, 2, |_| 1, &abandoned) {
                            Turn::Lead(lead) => {
                                assert!(!leading.swap(true, Ordering::AcqRel), "two leaders");
                                handled.lock().extend(lead.group.iter().map(|t| t.item));
                                let n = lead.group.len();
                                // Cleared as the last slot goes out, still
                                // under the queue lock: the check covers the
                                // hand-out too, and not the next leader.
                                lead.finish(|i, t| {
                                    if i + 1 == n {
                                        leading.store(false, Ordering::Release);
                                    }
                                    (t.item, i as u32)
                                })
                                .0
                            }
                            Turn::Followed(slot, _) => slot,
                        };
                        assert_eq!(slot.0, id, "a slot meant for another writer");
                    })
                })
                .collect();
            for w in writers {
                w.join().unwrap();
            }
            let mut handled = handled.lock().clone();
            handled.sort();
            assert_eq!(handled, vec![0, 1, 2]);
        });
    }

    /// The whole commit path in small: two writers through the queue, each
    /// group's members inserting in parallel and the last one publishing,
    /// and a leader that switches memtables first waiting for every group
    /// already handed out, as `switch_memtable` does. Nothing deadlocks, a
    /// group goes in the memtable it was handed, and every write ends
    /// visible.
    #[test]
    fn groups_publish_and_a_switch_waits_for_them() {
        struct Group {
            first: u64,
            last: u64,
            memtable: u64,
            remaining: AtomicUsize,
        }
        struct Commit {
            last_seq: u64,
            memtable: u64,
        }
        model(|| {
            let q: Arc<GroupQueue<u64, Option<(Arc<Group>, u64)>>> =
                Arc::new(GroupQueue::default());
            let p = Arc::new(Publisher::new(0));
            let commit = Arc::new(Mutex::new(Commit {
                last_seq: 0,
                memtable: 0,
            }));
            let current_memtable = Arc::new(AtomicU64::new(0));
            let writers: Vec<_> = (0..2u64)
                .map(|_| {
                    let (q, p, commit, current) = (
                        q.clone(),
                        p.clone(),
                        commit.clone(),
                        current_memtable.clone(),
                    );
                    thread::spawn(move || {
                        let abandoned = || None;
                        let slot = match q.enter(1, false, usize::MAX, |_| 1, &abandoned) {
                            Turn::Lead(lead) => {
                                let mut c = commit.lock();
                                if c.last_seq > 0 {
                                    // A switch: every group handed out is
                                    // published before the new memtable.
                                    p.wait(c.last_seq);
                                    c.memtable += 1;
                                    current.store(c.memtable, Ordering::Release);
                                }
                                let first = c.last_seq + 1;
                                c.last_seq += lead.group.len() as u64;
                                let group = Arc::new(Group {
                                    first,
                                    last: c.last_seq,
                                    memtable: c.memtable,
                                    remaining: AtomicUsize::new(lead.group.len()),
                                });
                                drop(c);
                                lead.finish(|i, _| Some((group.clone(), first + i as u64)))
                                    .0
                            }
                            Turn::Followed(slot, _) => slot,
                        };
                        let (group, seq) = slot.expect("not abandoned");
                        // The insert: into the memtable the group was handed.
                        assert_eq!(group.memtable, current.load(Ordering::Acquire));
                        assert!(seq <= group.last);
                        if group.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                            p.publish(group.first, group.last);
                        } else {
                            p.wait(group.last);
                        }
                        assert!(p.visible() >= seq);
                    })
                })
                .collect();
            for w in writers {
                w.join().unwrap();
            }
            assert_eq!(p.visible(), 2);
        });
    }

    /// The path `Durability::Full` takes: a leader that finds fewer writers
    /// waiting than the last group had spins for them outside the queue
    /// lock. Writers arriving meanwhile join its group or the next one,
    /// every item is handled once, and nobody is left behind.
    #[test]
    fn gathering_a_group_loses_and_doubles_nothing() {
        model(|| {
            let q: Arc<GroupQueue<u32, u32>> = Arc::new(GroupQueue::default());
            q.last_group.store(2, Ordering::Relaxed);
            let handled = Arc::new(Mutex::new(Vec::new()));
            let writers: Vec<_> = (0..3u32)
                .map(|id| {
                    let (q, handled) = (q.clone(), handled.clone());
                    thread::spawn(move || {
                        let abandoned = || u32::MAX;
                        let turn = q.enter(id, true, usize::MAX, |_| 1, &abandoned);
                        let slot = match turn {
                            Turn::Lead(lead) => {
                                handled.lock().extend(lead.group.iter().map(|t| t.item));
                                lead.finish(|_, t| t.item).0
                            }
                            Turn::Followed(slot, _) => slot,
                        };
                        assert_eq!(slot, id);
                    })
                })
                .collect();
            for w in writers {
                w.join().unwrap();
            }
            let mut handled = handled.lock().clone();
            handled.sort();
            assert_eq!(handled, vec![0, 1, 2]);
            assert_eq!(q.queued.load(Ordering::Relaxed), 0);
        });
    }

    /// A leader that unwinds out of its group's work frees its place and
    /// tells its members: the next writer still gets through.
    #[test]
    fn an_abandoned_group_does_not_wedge_the_queue() {
        model(|| {
            let q: Arc<GroupQueue<u32, bool>> = Arc::new(GroupQueue::default());
            let writers: Vec<_> = (0..2u32)
                .map(|id| {
                    let q = q.clone();
                    thread::spawn(move || {
                        let abandoned = || false;
                        let turn = q.enter(id, false, usize::MAX, |_| 1, &abandoned);
                        match turn {
                            // The first leader gives up; a later one finishes.
                            Turn::Lead(lead) if id == 0 => drop(lead),
                            Turn::Lead(lead) => {
                                let _ = lead.finish(|_, _| true);
                            }
                            Turn::Followed(..) => {}
                        }
                    })
                })
                .collect();
            for w in writers {
                w.join().unwrap();
            }
        });
    }
}
