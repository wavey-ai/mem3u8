use crate::Options;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

const NUM_PLAYLISTS: usize = 10;
const MAX_SEGMENTS: usize = 32;
const MAX_PARTS_PER_SEGMENT: usize = 128;
const BUFFER_SIZE_KB: usize = 512;
const BUFFER_SIZE_BYTES: usize = BUFFER_SIZE_KB * 1024;

pub struct RingBuffer {
    buffer: Vec<RwLock<Bytes>>,
    idxs: RwLock<BTreeMap<u64, usize>>,
    offsets: RwLock<BTreeMap<u64, usize>>,
    idx: AtomicUsize,
    options: Options,
}

impl RingBuffer {
    pub fn new(options: Options) -> Self {
        let buffer_size_bytes = options.buffer_size_kb * 1024;
        let buffer_repeat_value = Bytes::from(vec![0u8; buffer_size_bytes]);

        let buffer_size =
            options.num_playlists * options.max_parts_per_segment * options.max_parted_segments;
        let mut buffer = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            buffer.push(RwLock::new(buffer_repeat_value.clone()));
        }

        RingBuffer {
            buffer,
            idxs: RwLock::new(BTreeMap::new()),
            offsets: RwLock::new(BTreeMap::new()),
            idx: AtomicUsize::new(0),
            options,
        }
    }

    pub fn set(&self, stream_id: u64, id: usize, data: Bytes) {
        let h = const_xxh3(&data);
        let b = Bytes::from(data);

        let mut packet = BytesMut::new();
        packet.put_u32(b.len() as u32);
        packet.put_u64(h);
        packet.put(b);

        if let Some(idx) = self.offset(stream_id, id) {
            let mut lock = self.buffer[idx].write().unwrap();
            *lock = packet.freeze();
        }
    }

    fn get_bytes(&self, data: &Bytes) -> (Bytes, u64) {
        let data_size = u32::from_be_bytes(data[0..4].try_into().unwrap());

        let h = u64::from_be_bytes(data[4..12].try_into().unwrap());

        let payload = data.slice(12..12 + data_size as usize);

        (payload, h)
    }

    fn add_stream_id(&self, stream_id: u64) {
        let idx = self.idx.load(Ordering::SeqCst);
        let next_offset = (idx + 1) % self.options.num_playlists;
        {
            let mut lock = self.offsets.write().unwrap();
            let stream_id = stream_id.to_owned();
            lock.insert(stream_id, idx);
        }
        self.idx.store(next_offset, Ordering::SeqCst);
    }

    pub(crate) fn zero_stream_id(&self, stream_id: u64) {
        let mut lock = self.offsets.write().unwrap();
        let stream_id = stream_id.to_owned();
        lock.remove(&stream_id);
    }

    pub fn add(&self, stream_id: u64, id: usize, data_bytes: Bytes) {
        if let Some(idx) = self.offset(stream_id, id) {
            self.set(stream_id, idx, data_bytes);
        } else {
            self.add_stream_id(stream_id);
            if let Some(idx) = self.offset(stream_id, id) {
                self.set(stream_id, idx, data_bytes);
            }
        }

        let mut lock = self.idxs.write().unwrap();
        let stream_id = stream_id.to_owned();
        lock.insert(stream_id, id);
    }

    pub fn get(&self, stream_id: u64, id: usize) -> Option<(Bytes, u64)> {
        {
            let lock = self.idxs.read().unwrap();
            if let Some(last) = lock.get(&stream_id) {
                if &id > last {
                    return None;
                }
            } else {
                return None;
            }
        }

        if let Some(idx) = self.offset(stream_id, id) {
            let lock = self.buffer[idx].read().unwrap();
            let (payload, h) = self.get_bytes(&lock);
            drop(lock);
            Some((payload, h))
        } else {
            None
        }
    }

    fn offset(&self, stream_id: u64, id: usize) -> Option<usize> {
        let lock = self.offsets.read().unwrap();
        if let Some(offset) = lock.get(&stream_id) {
            let sub_buffer_size = MAX_PARTS_PER_SEGMENT * MAX_SEGMENTS;
            Some((offset * sub_buffer_size) + id % sub_buffer_size)
        } else {
            None
        }
    }
}
