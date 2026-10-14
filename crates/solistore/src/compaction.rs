//! Leveled compaction.
//!
//! Level 0 holds flushed memtables, which overlap. Each deeper level is one
//! sorted run of disjoint files, ten times larger than the one above it. A
//! compaction merges files of one level into the next: all of level 0 once
//! it has `l0_trigger` files, otherwise one file of the level most over its
//! target size, chosen round-robin.
//!
//! The merge keeps only the newest version of each key (see
//! [`crate::version`] for why no older one can still be needed), drops what
//! a range tombstone covers and what belongs to a dropped column family, and
//! drops a delete marker once no deeper level can hold the key it shadows. A
//! file that overlaps nothing in the next level is moved there without being
//! rewritten.
//!
//! # Range tombstones
//!
//! Every file records the highest tombstone sequence applied when it was
//! written (its watermark). A file that overlaps a tombstone above its
//! watermark, and holds entries older than it, may hold covered data: such
//! files are compacted before anything else — level 0 into level 1 as
//! usual, a deeper file rewritten in place. The output's watermark covers the
//! tombstone, so a file is never picked twice for one tombstone, and once no
//! file is left that may hold covered data, the tombstone itself is dropped.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::fs::Fs;
use crate::iter::{InternalIterator, MergingIterator};
use crate::key::{self, Kind};
use crate::memtable::{covering_seq, RangeTombstone};
use crate::sst::{TableBuilder, TableIter, TableProps};
use crate::version::{table_name, LevelIter, TableFile, Version, NUM_LEVELS};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub l0_trigger: usize,
    pub level_base_bytes: u64,
    pub level_multiplier: u64,
    pub target_file_size: u64,
    /// Ranges one compaction is split into and merged in parallel.
    pub subcompactions: usize,
}

pub struct Compaction {
    /// The level the inputs come from.
    pub level: usize,
    pub inputs: Vec<Arc<TableFile>>,
    /// Files of the output level overlapping the inputs.
    pub overlapping: Vec<Arc<TableFile>>,
    /// `level + 1`, or `level` for a file rewritten in place.
    pub output_level: usize,
}

impl Compaction {
    /// One file, nothing under it: move it down instead of rewriting it.
    pub fn is_trivial_move(&self) -> bool {
        self.output_level == self.level + 1 && self.inputs.len() == 1 && self.overlapping.is_empty()
    }

    pub fn all_inputs(&self) -> impl Iterator<Item = (usize, &Arc<TableFile>)> {
        self.inputs
            .iter()
            .map(move |f| (self.level, f))
            .chain(self.overlapping.iter().map(move |f| (self.output_level, f)))
    }

    /// The largest key compacted, where the next pick in this level starts.
    pub fn largest(&self) -> Vec<u8> {
        self.inputs
            .iter()
            .map(|f| &f.props.largest)
            .max_by(|a, b| key::compare(a, b))
            .cloned()
            .unwrap_or_default()
    }
}

pub fn level_bytes(files: &[Arc<TableFile>]) -> u64 {
    files.iter().map(|f| f.props.size).sum()
}

fn target_bytes(cfg: &Config, level: usize) -> u64 {
    let mut target = cfg.level_base_bytes;
    for _ in 1..level {
        target = target.saturating_mul(cfg.level_multiplier);
    }
    target
}

/// `(cf ‖ user key)` bounds of a set of files.
fn key_range<'a>(
    files: impl IntoIterator<Item = &'a Arc<TableFile>>,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut range: Option<(Vec<u8>, Vec<u8>)> = None;
    for f in files {
        let (lo, hi) = (
            key::cf_and_user_key(&f.props.smallest),
            key::cf_and_user_key(&f.props.largest),
        );
        range = Some(match range {
            None => (lo.to_vec(), hi.to_vec()),
            Some((a, b)) => (a.min(lo.to_vec()), b.max(hi.to_vec())),
        });
    }
    range
}

fn overlaps(f: &TableFile, lo: &[u8], hi: &[u8]) -> bool {
    key::cf_and_user_key(&f.props.smallest) <= hi && key::cf_and_user_key(&f.props.largest) >= lo
}

/// A table's column family and user-key bounds. Flushes and compactions
/// cut files at column family boundaries, so a file has one.
pub fn file_bounds(f: &TableFile) -> Option<(u32, &[u8], &[u8])> {
    let (lo, hi) = (
        key::parse(&f.props.smallest)?,
        key::parse(&f.props.largest)?,
    );
    (lo.cf == hi.cf).then_some((lo.cf, lo.user_key, hi.user_key))
}

/// Whether `f` may hold an entry `t` deletes.
pub fn may_hold_covered(f: &TableFile, t: &RangeTombstone) -> bool {
    if f.props.tombstone_watermark >= t.seq || f.props.smallest_seq >= t.seq {
        return false;
    }
    match file_bounds(f) {
        Some((cf, lo, hi)) => cf == t.cf && lo < &t.end[..] && hi >= &t.start[..],
        None => true,
    }
}

/// Whether every entry of `f` is deleted by `t`: the file can go as is.
pub fn fully_covered(f: &TableFile, t: &RangeTombstone) -> bool {
    f.props.largest_seq < t.seq
        && file_bounds(f)
            .is_some_and(|(cf, lo, hi)| cf == t.cf && lo >= &t.start[..] && hi < &t.end[..])
}

/// Whether a file is already being compacted. Compactions run concurrently
/// on disjoint files; a candidate touching a busy file is passed over.
pub type Busy<'a> = &'a dyn Fn(u64) -> bool;

impl Compaction {
    pub fn is_free(&self, busy: Busy<'_>) -> bool {
        self.all_inputs().all(|(_, f)| !busy(f.number))
    }
}

/// The compaction most needed now that touches no busy file, if any.
pub fn pick(
    version: &Version,
    cursors: &[Vec<u8>],
    cfg: &Config,
    busy: Busy<'_>,
) -> Option<Compaction> {
    pick_for_tombstones(version, busy).or_else(|| pick_by_size(version, cursors, cfg, busy))
}

/// A file that may hold data a range tombstone deletes.
pub fn pick_for_tombstones(version: &Version, busy: Busy<'_>) -> Option<Compaction> {
    for t in &version.tombstones {
        for (level, files) in version.levels.iter().enumerate() {
            for f in files.iter().filter(|f| may_hold_covered(f, t)) {
                let candidate = if level == 0 {
                    level0(version)
                } else {
                    Some(Compaction {
                        level,
                        inputs: vec![f.clone()],
                        overlapping: Vec::new(),
                        output_level: level,
                    })
                };
                if let Some(c) = candidate.filter(|c| c.is_free(busy)) {
                    return Some(c);
                }
            }
        }
    }
    None
}

fn level0(version: &Version) -> Option<Compaction> {
    let inputs = version.levels[0].clone();
    let (lo, hi) = key_range(&inputs)?;
    let overlapping = version.levels[1]
        .iter()
        .filter(|f| overlaps(f, &lo, &hi))
        .cloned()
        .collect();
    Some(Compaction {
        level: 0,
        inputs,
        overlapping,
        output_level: 1,
    })
}

/// One file of `level` into the next, round-robin from `cursor`.
fn one_file(
    version: &Version,
    level: usize,
    cursor: Option<&Vec<u8>>,
    busy: Busy<'_>,
) -> Option<Compaction> {
    let files = &version.levels[level];
    let start = cursor.filter(|c| !c.is_empty()).map_or(0, |c| {
        files.partition_point(|f| key::compare(&f.props.smallest, c) != Ordering::Greater)
    });
    (0..files.len())
        .map(|i| &files[(start + i) % files.len()])
        .find_map(|f| {
            let inputs = vec![f.clone()];
            let (lo, hi) = key_range(&inputs)?;
            let overlapping = version.levels[level + 1]
                .iter()
                .filter(|g| overlaps(g, &lo, &hi))
                .cloned()
                .collect();
            Some(Compaction {
                level,
                inputs,
                overlapping,
                output_level: level + 1,
            })
            .filter(|c| c.is_free(busy))
        })
}

fn pick_by_size(
    version: &Version,
    cursors: &[Vec<u8>],
    cfg: &Config,
    busy: Busy<'_>,
) -> Option<Compaction> {
    let mut scored: Vec<(f64, usize)> = Vec::new();
    let l0 = version.levels[0].len() as f64 / cfg.l0_trigger.max(1) as f64;
    if l0 >= 1.0 {
        scored.push((l0, 0));
    }
    for level in 1..NUM_LEVELS - 1 {
        let score = level_bytes(&version.levels[level]) as f64 / target_bytes(cfg, level) as f64;
        if score >= 1.0 {
            scored.push((score, level));
        }
    }
    scored.sort_by(|a, b| b.0.total_cmp(&a.0));
    scored.into_iter().find_map(|(_, level)| {
        if level == 0 {
            level0(version).filter(|c| c.is_free(busy))
        } else {
            one_file(version, level, cursors.get(level), busy)
        }
    })
}

/// Level 0 into level 1, if level 0 holds anything in `[lo, hi]` (`cf ‖ key`).
pub fn pick_level0_overlapping(
    version: &Version,
    lo: &[u8],
    hi: &[u8],
    busy: Busy<'_>,
) -> Option<Compaction> {
    version.levels[0]
        .iter()
        .any(|f| overlaps(f, lo, hi))
        .then(|| level0(version))
        .flatten()
        .filter(|c| c.is_free(busy))
}

/// Whether a level below `output_level` may hold `cf ‖ user key`.
fn may_exist_below(version: &Version, output_level: usize, id: &[u8]) -> bool {
    version.levels[output_level + 1..].iter().any(|files| {
        let at = files.partition_point(|f| key::cf_and_user_key(&f.props.largest) < id);
        files
            .get(at)
            .is_some_and(|f| key::cf_and_user_key(&f.props.smallest) <= id)
    })
}

/// Merge the inputs into new files for the output level. Each file is
/// fsynced, then the directory. The outputs carry the highest tombstone
/// sequence of `version` as their watermark.
///
/// The key space is split at input file boundaries into up to
/// `cfg.subcompactions` ranges merged in parallel: a compaction from level 0
/// rewrites most of level 1 and would otherwise run on one core while
/// writers wait on it.
pub fn run(
    compaction: &Compaction,
    version: &Version,
    live_cf: &(dyn Fn(u32) -> bool + Sync),
    fs: &dyn Fs,
    dir: &Path,
    cfg: &Config,
    allocate: &(dyn Fn() -> u64 + Sync),
) -> Result<Vec<(u64, TableProps)>> {
    let ranges = split(compaction, cfg.subcompactions.max(1));
    if ranges.len() == 1 {
        return merge_range(
            compaction, version, live_cf, fs, dir, cfg, allocate, None, None,
        );
    }
    let results: Vec<Result<Vec<(u64, TableProps)>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = ranges
            .iter()
            .map(|(lo, hi)| {
                scope.spawn(move || {
                    merge_range(
                        compaction,
                        version,
                        live_cf,
                        fs,
                        dir,
                        cfg,
                        allocate,
                        lo.as_deref(),
                        hi.as_deref(),
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("a subcompaction panicked"))
            .collect()
    });
    let mut written: Vec<(u64, TableProps)> = Vec::new();
    for result in results {
        written.extend(result?);
    }
    // The ranges must partition the key space: outputs in order, disjoint.
    // A bug here would silently break the level's sort order, so check.
    for pair in written.windows(2) {
        if key::cf_and_user_key(&pair[0].1.largest) >= key::cf_and_user_key(&pair[1].1.smallest) {
            return Err(Error::Corruption("subcompaction outputs overlap".into()));
        }
    }
    Ok(written)
}

type Range = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Up to `n` contiguous `cf ‖ user key` ranges covering every input, split
/// at file boundaries so each range holds a similar number of files.
fn split(compaction: &Compaction, n: usize) -> Vec<Range> {
    let mut bounds: Vec<Vec<u8>> = compaction
        .inputs
        .iter()
        .chain(&compaction.overlapping)
        .map(|f| key::cf_and_user_key(&f.props.smallest).to_vec())
        .collect();
    bounds.sort();
    bounds.dedup();
    if n <= 1 || bounds.len() < 2 {
        return vec![(None, None)];
    }
    // Skip the lowest bound: nothing sorts below it.
    let candidates = &bounds[1..];
    let parts = n.min(candidates.len() + 1);
    let cuts: Vec<Vec<u8>> = (1..parts)
        .map(|i| candidates[i * candidates.len() / parts].clone())
        .collect();
    let mut ranges = Vec::with_capacity(parts);
    let mut lo = None;
    for cut in cuts {
        if lo.as_ref().is_some_and(|l: &Vec<u8>| *l >= cut) {
            continue;
        }
        ranges.push((lo, Some(cut.clone())));
        lo = Some(cut);
    }
    ranges.push((lo, None));
    ranges
}

/// Merge the part of the inputs in `[lo, hi)` (`cf ‖ user key`).
#[allow(clippy::too_many_arguments)]
fn merge_range(
    compaction: &Compaction,
    version: &Version,
    live_cf: &(dyn Fn(u32) -> bool + Sync),
    fs: &dyn Fs,
    dir: &Path,
    cfg: &Config,
    allocate: &(dyn Fn() -> u64 + Sync),
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Result<Vec<(u64, TableProps)>> {
    let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
    if compaction.level == 0 {
        for f in &compaction.inputs {
            children.push(Box::new(
                TableIter::new(f.reader()?).without_filling_cache(),
            ));
        }
    } else {
        children.push(Box::new(
            LevelIter::new(compaction.inputs.clone()).without_filling_cache(),
        ));
    }
    if !compaction.overlapping.is_empty() {
        children.push(Box::new(
            LevelIter::new(compaction.overlapping.clone()).without_filling_cache(),
        ));
    }
    let mut merged = MergingIterator::new(children);
    match lo {
        Some(lo) => merged.seek_ge(&key::first_of_id(lo)),
        None => merged.seek_ge(&key::first_of(0, b"")),
    }

    let watermark = version.tombstones.iter().map(|t| t.seq).max().unwrap_or(0);
    let mut out = OutputFiles::new(fs, dir, cfg.target_file_size, watermark);
    let mut allocate = || allocate();
    let mut live = LiveCf::new(live_cf);
    let mut last_id: Vec<u8> = Vec::new();
    let mut have_last = false;
    while merged.valid() {
        let k = merged.key();
        let id = key::cf_and_user_key(k);
        if hi.is_some_and(|hi| id >= hi) {
            break;
        }
        if have_last && id == last_id.as_slice() {
            merged.next();
            continue; // an older version of a key already handled
        }
        last_id.clear();
        last_id.extend_from_slice(id);
        have_last = true;
        let parsed = key::parse(k).ok_or_else(|| Error::Corruption("bad key in table".into()))?;
        let covered =
            covering_seq(&version.tombstones, parsed.cf, parsed.user_key, u64::MAX) > parsed.seq;
        let dead_marker =
            parsed.kind == Kind::Delete && !may_exist_below(version, compaction.output_level, id);
        if !(covered || dead_marker || !live.check(parsed.cf)) {
            out.add(parsed.cf, k, &merged.value(), &mut allocate)?;
        }
        merged.next();
    }
    merged.status()?;
    out.finish()
}

/// Whether a column family is live, asked once per run of keys in it: the
/// answer comes from a hash set, and entries arrive grouped by family.
pub struct LiveCf<'a> {
    live: &'a (dyn Fn(u32) -> bool + Sync),
    last: Option<(u32, bool)>,
}

impl<'a> LiveCf<'a> {
    pub fn new(live: &'a (dyn Fn(u32) -> bool + Sync)) -> Self {
        LiveCf { live, last: None }
    }

    pub fn check(&mut self, cf: u32) -> bool {
        match self.last {
            Some((last, alive)) if last == cf => alive,
            _ => {
                let alive = (self.live)(cf);
                self.last = Some((cf, alive));
                alive
            }
        }
    }
}

/// Table files being written in key order, cut at a size and at column
/// family boundaries.
pub struct OutputFiles<'a> {
    fs: &'a dyn Fs,
    dir: &'a Path,
    target: u64,
    watermark: u64,
    current: Option<(u64, TableBuilder)>,
    last_cf: Option<u32>,
    written: Vec<(u64, TableProps)>,
}

impl<'a> OutputFiles<'a> {
    pub fn new(fs: &'a dyn Fs, dir: &'a Path, target: u64, watermark: u64) -> Self {
        OutputFiles {
            fs,
            dir,
            target,
            watermark,
            current: None,
            last_cf: None,
            written: Vec::new(),
        }
    }

    pub fn add(
        &mut self,
        cf: u32,
        key: &[u8],
        value: &[u8],
        allocate: &mut impl FnMut() -> u64,
    ) -> Result<()> {
        let cut = self
            .current
            .as_ref()
            .is_some_and(|(_, b)| b.file_size() >= self.target || Some(cf) != self.last_cf);
        if cut {
            self.close()?;
        }
        if self.current.is_none() {
            let number = allocate();
            self.current = Some((
                number,
                TableBuilder::new(self.fs.create(&self.dir.join(table_name(number)))?),
            ));
        }
        self.last_cf = Some(cf);
        self.current.as_mut().expect("just set").1.add(key, value)
    }

    fn close(&mut self) -> Result<()> {
        if let Some((number, builder)) = self.current.take() {
            let mut props = builder.finish()?;
            props.tombstone_watermark = self.watermark;
            self.written.push((number, props));
        }
        Ok(())
    }

    /// Close the last file and sync the directory.
    pub fn finish(mut self) -> Result<Vec<(u64, TableProps)>> {
        self.close()?;
        if !self.written.is_empty() {
            self.fs.sync_dir(self.dir)?;
        }
        Ok(self.written)
    }
}
