use crate::cache::RingBuffer as CacheRingBuffer;
use crate::playlist::Playlist;
use crate::store::Store;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fmp4_segmenter::Fmp4;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::{broadcast, oneshot, watch};
use tracing::{debug, error, info};

pub struct MessageBroker {
    fmp4_cache: Arc<CacheRingBuffer>,
    m3u8_cache: Arc<Store>,
}

impl MessageBroker {
    pub fn new(fmp4_cache: Arc<CacheRingBuffer>, m3u8_cache: Arc<Store>) -> Self {
        Self {
            fmp4_cache,
            m3u8_cache,
        }
    }

    pub async fn start(
        &self,
        ip: &str,
    ) -> Result<
        (
            oneshot::Receiver<()>,
            oneshot::Receiver<()>,
            broadcast::Sender<()>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(16);
        let (up_tx, up_rx) = oneshot::channel();
        let (fin_tx, fin_rx) = oneshot::channel();

        let addr = format!("{}:4243", ip);
        let mut stream = TcpStream::connect(addr).await?;
        up_tx.send(());

        stream.write_all(b"LIST").await?;

        let mut buffer = BytesMut::with_capacity(1024);
        let mut remainder = BytesMut::new();

        let fmp4_cache = self.fmp4_cache.clone();
        let m3u8_cache = self.m3u8_cache.clone();

        let srv = async move {
            let (shutdown_tx, _) = broadcast::channel(16);
            loop {
                select! {
                    _= shutdown_rx.recv() => {
                        break;
                    }
                    result = stream.read_buf(&mut buffer) => {
                        if let Ok(n) = result {
                            if n == 0 {
                                break;
                            }

                            buffer.extend_from_slice(&remainder);
                            remainder.clear();

                            while !buffer.is_empty() {
                                if buffer.len() < 4 {
                                    remainder.extend_from_slice(&buffer);
                                    break;
                                }

                                let packet_len = buffer.get_u32();

                                if buffer.len() < packet_len as usize {
                                    remainder.extend_from_slice(&buffer);
                                    break;
                                }

                                let fmp4_cache = fmp4_cache.clone();
                                let m3u8_cache = m3u8_cache.clone();

                                let mut start = 0;
                                while start < packet_len {
                                    let end = start + 8; // 8 bytes for u64
                                    if end <= packet_len {
                                        let path_bytes = &buffer[start as usize..end as usize];
                                        let path = u64::from_be_bytes(path_bytes.try_into().unwrap());
                                        info!("received change: stream {}", path);
                                        let shutdown_signal = shutdown_tx.subscribe();
                                        start_stream(path, fmp4_cache.clone(), m3u8_cache.clone(), shutdown_signal).await;
                                    } else {
                                        break;
                                    }
                                    start = end;
                                }

                                buffer.advance(packet_len as usize);
                            }
                        }
                    }
                }
            }

            shutdown_tx.send(());

            let _ = fin_tx.send(());
        };

        tokio::spawn(srv);

        Ok((up_rx, fin_rx, shutdown_tx))
    }
}

fn process_frame(
    id: u64,
    frame: Bytes,
    fmp4_cache: Arc<CacheRingBuffer>,
    m3u8_cache: Arc<Store>,
) -> bool {
    let buffer_type = &frame[..4];
    debug!(
        "got: {}",
        std::str::from_utf8(buffer_type).unwrap_or("invalid")
    );

    match buffer_type {
        b"fini" => {
            m3u8_cache.zero_stream_id(id);
            fmp4_cache.zero_stream_id(id);

            return false;
        }
        b"init" => {
            let data = frame.slice(4..);
            m3u8_cache.set_init(id, data);
        }
        b"fmp4" => {
            let seq_bytes = frame.slice(4..8);
            let seq = u32::from_be_bytes(*seq_bytes.as_ref().try_into().unwrap_or(&[0; 4]));
            let data = frame.slice(8..);
            fmp4_cache.add(id, seq as usize, data);
        }
        b"m3u8" => {
            let add_idx = frame[4];
            let seg_bytes = frame.slice(5..9);
            let seg =
                u32::from_be_bytes(*seg_bytes.as_ref().try_into().unwrap_or(&[0; 4])) as usize;
            let seq_bytes = frame.slice(9..13);
            let seq =
                u32::from_be_bytes(*seq_bytes.as_ref().try_into().unwrap_or(&[0; 4])) as usize;
            let idx_bytes = frame.slice(13..17);
            let idx =
                u32::from_be_bytes(*idx_bytes.as_ref().try_into().unwrap_or(&[0; 4])) as usize;
            let m3u8 = frame.slice(17..);
            if add_idx == 1 {
                m3u8_cache.end_segment(id, seg, seq);
            }
            m3u8_cache.add(id, seg, seq, idx, m3u8);
        }
        _ => {
            error!(
                "Unknown buffer type: {}",
                std::str::from_utf8(buffer_type).unwrap_or("invalid")
            );
        }
    }

    return true;
}

async fn start_stream(
    id: u64,
    fmp4_cache: Arc<CacheRingBuffer>,
    m3u8_cache: Arc<Store>,
    mut shutdown_signal: broadcast::Receiver<()>,
) {
    tokio::spawn(async move {
        let mut stream = TcpStream::connect("127.0.0.1:4243").await.unwrap();
        stream.write_all(&id.to_be_bytes()).await.unwrap();

        let mut buffer = BytesMut::new();
        let mut current_frame_length: Option<usize> = None;

        'outer: loop {
            tokio::select! {
                _ = shutdown_signal.recv() => {
                    break;
                },
                read_result = stream.read_buf(&mut buffer) => {
                    match read_result {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            while {
                                if current_frame_length.is_none() && buffer.len() >= 4 {
                                    let length_bytes = buffer.split_to(4);
                                    current_frame_length = Some(u32::from_be_bytes(length_bytes[..].try_into().unwrap()) as usize);
                                }

                                if let Some(len) = current_frame_length {
                                    if buffer.len() >= len {
                                        let frame = buffer.split_to(len);
                                        if !process_frame(id, frame.freeze(), Arc::clone(&fmp4_cache), Arc::clone(&m3u8_cache)) {
                                            break 'outer;
                                        }
                                        current_frame_length = None;
                                        true // Continue loop if there's more data
                                    } else {
                                        false // Break loop, need more data for current frame
                                    }
                                } else {
                                    false // Break loop, need more data for length
                                }
                            } {}
                        },
                        Err(_) => break, // Read error
                    }
                }
            }
        }

        info!("read closed on stream {}", id);
    });
}

pub struct Mq {
    streams: Arc<RwLock<BTreeMap<u64, broadcast::Sender<Bytes>>>>,
    changes: Arc<broadcast::Sender<Bytes>>,
    shutdown_tx: Option<watch::Sender<()>>,
    playlists: Mutex<BTreeMap<u64, Playlist>>,
    last_segs: Mutex<BTreeMap<u64, u32>>,
}

impl Mq {
    pub fn new() -> Self {
        let (sender, mut rx) = broadcast::channel(16);
        // TODO: why is this needed to keep the channel open. why.
        tokio::task::spawn(async move { while let Ok(d) = rx.recv().await {} });

        Self {
            streams: Arc::new(RwLock::new(BTreeMap::new())),
            changes: Arc::new(sender),
            shutdown_tx: None,
            playlists: Mutex::new(BTreeMap::new()),
            last_segs: Mutex::new(BTreeMap::new()),
        }
    }

    fn send(&self, id: u64, packet: Bytes) {
        let mut inserted = false;
        {
            let mut lock = self.streams.write().unwrap();
            let sender = lock.entry(id).or_insert_with(|| {
                let (sender, mut rx) = broadcast::channel(16);
                // TODO: why is this needed to keep the channel open. why.
                tokio::task::spawn(async move { while let Ok(d) = rx.recv().await {} });
                inserted = true;
                sender
            });

            let l = packet.len();
            match sender.send(packet) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("error broadcasting: {}", e);
                }
            }
        }
        if inserted {
            let keys: Vec<u64> = {
                let lock = self.streams.read().unwrap();
                lock.keys().cloned().collect()
            };

            let mut data = BytesMut::new();
            data.put_u32((keys.len() * 8) as u32);
            for key in keys {
                data.put_u64(key);
            }

            match self.changes.send(data.freeze()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("error broadcasting: {}", e);
                }
            }
        }
    }

    pub fn fin(&self, id: u64) {
        let packet_size = 4 as u32;
        let mut packet = BytesMut::with_capacity(packet_size as usize);
        packet.put_u32(packet_size);
        packet.put_slice(b"fini");
        self.send(id, packet.freeze());
        self.streams.write().unwrap().remove(&id);
        self.playlists.lock().unwrap().remove(&id);
        self.last_segs.lock().unwrap().remove(&id);
    }

    pub fn add(&self, id: u64, fmp4: Fmp4) {
        let (m3u8, seg, seq, idx) = {
            let mut lock = self.playlists.lock().unwrap();
            let mut playlist = lock
                .entry(id)
                .or_insert_with(|| Playlist::new(crate::Options::default()));

            playlist.add_part(fmp4.duration, fmp4.key)
        };
        let mut add_idx = 0;

        {
            let mut lock = self.last_segs.lock().unwrap();

            let last_seg = lock.entry(id).or_insert(0);

            if *last_seg + 1 == seg as u32 {
                add_idx = 1;
                *last_seg = seg as u32;
            }
        }

        if let Some(data) = fmp4.init {
            let packet_size = 4 + data.len() as u32;
            let mut packet = BytesMut::with_capacity(packet_size as usize);
            packet.put_u32(packet_size);
            packet.put_slice(b"init");
            packet.put(data);
            self.send(id, packet.freeze());
        }

        let packet_size = 8 + fmp4.data.len() as u32;
        let mut packet = BytesMut::with_capacity(packet_size as usize);
        packet.put_u32(packet_size);
        packet.put_slice(b"fmp4");
        packet.put_u32(seq as u32);
        packet.put(fmp4.data);
        self.send(id, packet.freeze());

        let packet_size = 17 + m3u8.len() as u32;
        let mut packet = BytesMut::with_capacity(packet_size as usize);
        packet.put_u32(packet_size);
        packet.put_slice(b"m3u8");
        packet.put_u8(add_idx);
        packet.put_u32(seg as u32);
        packet.put_u32(seq as u32);
        packet.put_u32(idx as u32);
        packet.put(m3u8);
        self.send(id, packet.freeze());
    }

    pub async fn start(
        &self,
    ) -> Result<
        (
            oneshot::Receiver<()>,
            oneshot::Receiver<()>,
            watch::Sender<()>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let (up_tx, up_rx) = oneshot::channel();
        let (fin_tx, fin_rx) = oneshot::channel();

        let addr: SocketAddr = ([127, 0, 0, 1], 4243).into();
        let listener = TcpListener::bind(addr).await.unwrap();
        up_tx.send(());

        let streams_clone = Arc::clone(&self.streams);
        let changes_clone = Arc::clone(&self.changes);

        let srv = async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _)) => {
                                let streams_clone = Arc::clone(&streams_clone);
                                let changes_clone = Arc::clone(&changes_clone);
                                tokio::task::spawn(async move {
                                    stream_handler(stream, Arc::clone(&streams_clone), Arc::clone(&changes_clone))
                                        .await
                                });
                            }
                            Err(err) => {
                                eprintln!("Error accepting connection: {:?}", err);
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("Received shutdown signal, exiting...");
                        break;
                    }
                }
            }

            fin_tx.send(());
        };

        info!("cache-balancer server listening on {:?}", addr);

        tokio::spawn(srv);

        Ok((up_rx, fin_rx, shutdown_tx))
    }
}

async fn stream_handler(
    mut stream: TcpStream,
    streams: Arc<RwLock<BTreeMap<u64, broadcast::Sender<Bytes>>>>,
    changes: Arc<broadcast::Sender<Bytes>>,
) {
    let mut buffer = [0; 1024];

    match stream.read(&mut buffer).await {
        Ok(n) => {
            if let Ok(command) = String::from_utf8(buffer[..n].to_vec()) {
                let command = command.trim().to_string();

                if command == "LIST" {
                    let ids = {
                        let lock = streams.read().unwrap();
                        lock.keys().cloned().collect::<Vec<_>>()
                    };

                    let mut data = BytesMut::new();
                    data.put_u32((ids.len() * 8) as u32);
                    for id in &ids {
                        data.put_u64(*id);
                    }

                    info!("sending initial changes {:?}", ids);
                    stream.write_all(&data).await.unwrap();
                    let mut rx = changes.subscribe();

                    loop {
                        match rx.recv().await {
                            Ok(data) => {
                                info!("sending change {:?}", data);
                                if stream.write_all(&data).await.is_err() {
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            }
                            Err(e) => {
                                error!("Error receiving data from broadcast channel: {:?}", e);
                                break;
                            }
                        }
                    }
                } else {
                    let id = u64::from_be_bytes(buffer[..8].try_into().unwrap());
                    let tx = {
                        let lock = streams.read().unwrap();
                        lock.get(&id).cloned()
                    };

                    if let Some(tx) = tx {
                        let mut rx = tx.subscribe();

                        loop {
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(10),
                                rx.recv(),
                            )
                            .await
                            {
                                Ok(result) => match result {
                                    Ok(data) => {
                                        if stream.write_all(&data).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(broadcast::error::RecvError::Closed) => {
                                        break;
                                    }
                                    Err(e) => {
                                        error!(
                                            "Error receiving data from broadcast channel: {:?}",
                                            e
                                        );
                                        break;
                                    }
                                },
                                Err(_) => {
                                    eprintln!(
                                        "timeout waiting for broadcast data for stream {}",
                                        id
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            error!("Error reading from stream: {:?}", e);
        }
    }
}

fn extract_fields(data: &[u8]) -> Option<(u64, u32, Bytes)> {
    if data.len() < 13 {
        return None;
    }

    let stream_id = u64::from_be_bytes(data[..8].try_into().unwrap());
    let id = u32::from_be_bytes(data[8..12].try_into().unwrap());
    let payload = Bytes::copy_from_slice(&data[12..]);

    Some((stream_id, id, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server() {
        let mut mq = Mq::new();

        let (up, fin, shutdown) = mq.start().await.unwrap();

        up.await;

        shutdown.send(());

        fin.await;
    }
}
