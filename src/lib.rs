pub mod cache;
pub mod mq;
pub mod playlist;
pub mod store;

pub struct Options {
    pub max_segments: usize,
    pub num_playlists: usize,
    pub max_parts_per_segment: usize,
    pub max_parted_segments: usize,
    pub segment_min_ms: u32,
    pub buffer_size_kb: usize,
    pub init_size_kb: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            max_segments: 32,
            num_playlists: 10,
            max_parts_per_segment: 128,
            max_parted_segments: 32,
            segment_min_ms: 1500,
            buffer_size_kb: 5,
            init_size_kb: 5,
        }
    }
}
