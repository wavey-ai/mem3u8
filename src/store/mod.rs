use crate::Options;
use bytes::{BufMut, Bytes, BytesMut};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::BTreeMap;
use std::io::prelude::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    RwLock,
};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

pub struct Store {
    buffer: Vec<RwLock<Bytes>>,
    seg_parts: Vec<AtomicUsize>,
    last_seg: Vec<AtomicUsize>,
    last_part: Vec<AtomicUsize>,
    inits: Vec<RwLock<Bytes>>,
    offsets: RwLock<BTreeMap<u64, usize>>,
    offset: AtomicUsize,
    options: Options,
}

impl Store {
    pub fn new(options: Options) -> Self {
        let buffer_size_bytes = options.buffer_size_kb * 1024;
        let init_size_bytes = options.init_size_kb * 1024;

        let buffer_repeat_value = Bytes::from(vec![0u8; buffer_size_bytes]);
        let init_repeat_value = Bytes::from(vec![0u8; init_size_bytes]);

        let buffer_size =
            options.num_playlists * options.max_parts_per_segment * options.max_parted_segments;
        let mut buffer = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            buffer.push(RwLock::new(buffer_repeat_value.clone()));
        }

        let seg_parts_size = options.max_segments * options.num_playlists;
        let mut seg_parts = Vec::with_capacity(seg_parts_size);
        for _ in 0..seg_parts_size {
            seg_parts.push(AtomicUsize::new(0));
        }

        let num_playlists = options.num_playlists;
        let mut last_seg = Vec::with_capacity(num_playlists);
        let mut last_part = Vec::with_capacity(num_playlists);
        let mut initial_seg_id = Vec::with_capacity(num_playlists);
        let mut inits = Vec::with_capacity(num_playlists);
        for _ in 0..num_playlists {
            last_seg.push(AtomicUsize::new(0));
            last_part.push(AtomicUsize::new(0));
            initial_seg_id.push(AtomicUsize::new(0));
            inits.push(RwLock::new(init_repeat_value.clone()));
        }

        Store {
            buffer,
            seg_parts,
            last_seg,
            last_part,
            inits,
            offsets: RwLock::new(BTreeMap::new()),
            offset: AtomicUsize::new(0),
            options,
        }
    }

    fn offset(&self, stream_id: u64) -> Option<usize> {
        let lock = self.offsets.read().unwrap();
        lock.get(&stream_id).copied()
    }

    fn last_seg(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_seg[n].load(Ordering::SeqCst))
    }

    fn last_part(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_part[n].load(Ordering::SeqCst))
    }

    fn add_stream_id(&self, stream_id: u64) {
        let idx = self.offset.load(Ordering::SeqCst);
        let next_offset = idx + 1;
        {
            let mut lock = self.offsets.write().unwrap();
            let stream_id = stream_id.to_owned();
            lock.insert(stream_id, idx);
        }
        self.offset.store(next_offset, Ordering::SeqCst);
    }

    fn set_last_seg(&self, stream_id: u64, id: usize) {
        if let Some(n) = self.offset(stream_id) {
            self.last_seg[n].store(id, Ordering::SeqCst);
        }
    }

    fn set_last_part(&self, stream_id: u64, id: usize) {
        if let Some(n) = self.offset(stream_id) {
            self.last_part[n].store(id, Ordering::SeqCst);
        }
    }

    pub fn set_init(&self, stream_id: u64, data_bytes: Bytes) {
        if let Some(n) = self.offset(stream_id) {
            let mut inits_lock = self.inits[n].write().unwrap();
            *inits_lock = data_bytes;
        }
    }

    pub fn get_init(&self, stream_id: u64) -> Option<Bytes> {
        if let Some(n) = self.offset(stream_id) {
            let lock = &self.inits[n];
            let data = lock.read().unwrap();
            Some(data.clone())
        } else {
            None
        }
    }

    pub fn add(&self, stream_id: u64, segment_id: usize, part_id: usize, data: Bytes) -> u64 {
        if self.offset(stream_id).is_none() {
            self.add_stream_id(stream_id);
        }

        let h = const_xxh3(&data);
        let gz = self.compress_data(&data).unwrap();
        let b = Bytes::from(gz);

        let mut packet = BytesMut::new();
        packet.put_u32(b.len() as u32);
        packet.put_u64(h);
        packet.put(b);

        if let Some(idx) = self.calculate_index(stream_id, segment_id, part_id) {
            let mut lock = self.buffer[idx].write().unwrap();
            *lock = packet.freeze();
        }

        self.set_last_seg(stream_id, segment_id);
        self.set_last_part(stream_id, part_id);

        h
    }

    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).map_err(|e| e.to_string())?;
        encoder.finish().map_err(|e| e.to_string())
    }

    fn is_included(&self, stream_id: u64, segment_id: usize, part_id: usize) -> bool {
        if let (Some(last_seg), Some(last_part)) =
            (self.last_seg(stream_id), self.last_part(stream_id))
        {
            if segment_id < last_seg
                || (segment_id == last_seg && part_id <= last_part)
                    && (last_seg - segment_id) < self.options.max_segments
            {
                return true;
            }
        }

        false
    }

    fn calculate_seg_index(&self, stream_id: u64, segment_id: usize) -> Option<usize> {
        if let Some(offset) = self.offset(stream_id) {
            let sub_buffer_size = self.options.max_segments;
            let idx = segment_id % sub_buffer_size;
            Some((offset * sub_buffer_size) + idx)
        } else {
            None
        }
    }

    fn calculate_index(&self, stream_id: u64, segment_id: usize, part_id: usize) -> Option<usize> {
        if segment_id == 0 {
            return None;
        }

        if let Some(offset) = self.offset(stream_id) {
            let sub_buffer_size =
                self.options.max_parts_per_segment * self.options.max_parted_segments;
            let idx =
                ((segment_id * self.options.max_parts_per_segment) + part_id) % sub_buffer_size;
            Some((offset * sub_buffer_size) + idx)
        } else {
            None
        }
    }

    pub fn end_segment(&self, stream_id: u64, part_id: usize) {
        if let Some(seg_id) = self.last_seg(stream_id) {
            if let Some(idx) = self.calculate_seg_index(stream_id, seg_id) {
                self.seg_parts[idx].store(part_id, Ordering::SeqCst);
            }
        }
    }

    pub fn get_idxs(&self, stream_id: u64, segment_id: usize) -> Option<(usize, usize)> {
        if let Some(idx) = self.calculate_seg_index(stream_id, segment_id) {
            let b = self.seg_parts[idx].load(Ordering::SeqCst);
            if let Some(idx) = self.calculate_seg_index(stream_id, segment_id) {
                let a = self.seg_parts[idx].load(Ordering::SeqCst);
                if a < b {
                    return Some((a + 1, b + 1));
                }
            }
        }

        None
    }

    fn get_bytes(&self, data: &Bytes) -> (Bytes, u64) {
        let data_size = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let h = u64::from_be_bytes(data[4..12].try_into().unwrap());
        let payload = data.slice(12..12 + data_size as usize);
        (payload, h)
    }

    pub fn get(&self, stream_id: u64, segment_id: usize, part_id: usize) -> Option<(Bytes, u64)> {
        if self.is_included(stream_id, segment_id, part_id) {
            if let Some(idx) = self.calculate_index(stream_id, segment_id, part_id) {
                let lock = self.buffer[idx].read().unwrap();
                let (payload, h) = self.get_bytes(&lock);
                drop(lock);
                Some((payload, h))
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn last(&self, stream_id: u64) -> Option<(Bytes, u64)> {
        if let (Some(last_seg), Some(last_part)) =
            (self.last_seg(stream_id), self.last_part(stream_id))
        {
            if let Some(idx) = self.calculate_index(stream_id, last_seg, last_part) {
                let lock = self.buffer[idx].read().unwrap();
                let (payload, h) = self.get_bytes(&lock);
                drop(lock);
                Some((payload, h))
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrapping_ring_buffer() {
        let rb = Store::new(Options::default());

        let mut all_added_data = Vec::new();

        let mut ranges = BTreeMap::new();

        for stream_id in 0..2 {
            let mut part_id = 1;
            rb.add_stream_id(stream_id as u64);
            let mut added_data = Vec::new();
            let mut range = Vec::new();
            for a in 1..14 {
                let ra = part_id;
                for _ in 0..4 {
                    let data = {
                        let mut d = BytesMut::new();
                        d.put_u8(a);
                        let bytes = (part_id as u64).to_be_bytes();
                        d.put(&bytes[..]);
                        d.freeze()
                    };

                    let h = rb.add(stream_id as u64, a as usize, part_id, data);
                    added_data.push((a as usize, part_id, h));

                    part_id += 1;
                }
                range.push((ra, part_id));

                rb.end_segment(stream_id as u64, part_id - 1);
            }
            all_added_data.push(added_data);

            ranges.insert(stream_id, range);
        }

        for (stream_id, added_data) in all_added_data.iter().enumerate() {
            for &(a, b, c) in added_data.iter().rev().take(3).collect::<Vec<_>>().iter() {
                match rb.get(stream_id as u64, *a, *b) {
                    Some((_, h)) => {
                        assert_eq!(*c, h);
                    }
                    None => panic!(
                        "Expected data not found for stream_id: {}, segment ID: {}, part ID: {}",
                        stream_id, a, b
                    ),
                }
            }
        }

        for stream_id in 0..2 {
            let range = ranges.get(&stream_id).unwrap();
            let l = range.len();
            for i in (l - 3)..l {
                if let Some(idxs) = rb.get_idxs(stream_id as u64, i) {
                    let r = range[i - 1];
                    assert_eq!(idxs.0, r.0);
                    assert_eq!(idxs.1, r.1);
                }
            }
        }
    }
}
