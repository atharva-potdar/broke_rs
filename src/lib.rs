use std::io::ErrorKind;
use std::sync::Arc;

use dashmap::DashMap;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};

use anyhow::Result;

#[derive(Debug, Serialize, Deserialize)]
enum RequestType {
    NewBroadcaster,
    NewSubscriber,
}

#[derive(Debug, Serialize, Deserialize)]
struct IncomingRequest {
    request_type: RequestType,
    message_topic: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BroadcastMessage {
    message_topic: String,
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum Payload {
    Request(IncomingRequest),
    Broadcast(BroadcastMessage),
}

const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

async fn write_framed(writer: &mut OwnedWriteHalf, data: &[u8]) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let len = u32::try_from(data.len());
    writer.write_all(&len?.to_be_bytes()).await?;
    writer.write_all(data).await?;
    Ok(())
}

async fn read_framed(reader: &mut tokio::net::tcp::OwnedReadHalf) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_SIZE {
        anyhow::bail!("Message size {len} exceeds maximum allowed size");
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

pub async fn run_broker() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    // For now, one broadcaster per message_topic
    let map_broadcast = Arc::new(DashMap::new());
    let map_subscribe = Arc::new(DashMap::<
        String,
        Arc<RwLock<Vec<(SocketAddr, Arc<Mutex<OwnedWriteHalf>>)>>>,
    >::new());

    // Buffer size 1024
    let (tx, mut rx) = mpsc::channel::<BroadcastMessage>(1024);

    let map_subscriber2 = Arc::clone(&map_subscribe);

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let message = Arc::new(match serde_json::to_vec(&msg) {
                Ok(m) => m,
                Err(e) => {
                    println!("Failed to serialize message: {e}");
                    continue;
                }
            });
            let subs = map_subscriber2
                .get(&msg.message_topic)
                .map(|v| Arc::clone(&v));
            if let Some(subs) = subs {
                let handles: Vec<(SocketAddr, Arc<Mutex<OwnedWriteHalf>>)> = {
                    let subs_lock = subs.read().await;
                    subs_lock
                        .iter()
                        .map(|(addr, m)| (*addr, Arc::clone(m)))
                        .collect()
                };

                let futs = handles.iter().map(|(addr, write_half_mutex)| {
                    let message = Arc::clone(&message);
                    let write_half_mutex = Arc::clone(write_half_mutex);
                    let addr = *addr;
                    async move {
                        let result =
                            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                                let mut write_half = write_half_mutex.lock().await;
                                write_framed(&mut write_half, &message).await
                            })
                            .await;
                        match result {
                            Ok(Ok(())) => (addr, Ok(())),
                            Ok(Err(e)) => (addr, Err(e)),
                            Err(_) => (addr, Err(anyhow::anyhow!("Write timed out for {addr}"))),
                        }
                    }
                });

                let results = futures::future::join_all(futs).await;
                let dead: std::collections::HashSet<SocketAddr> = results
                    .into_iter()
                    .filter_map(|(addr, result)| {
                        if let Err(e) = result {
                            println!("Failed to write to subscriber {addr}, removing: {e}");
                            Some(addr)
                        } else {
                            None
                        }
                    })
                    .collect();

                if !dead.is_empty() {
                    let mut subs_write = subs.write().await;
                    subs_write.retain(|(addr, _)| !dead.contains(addr));
                }
            }
        }
    });

    loop {
        let (socket, addr) = match listener.accept().await {
            Ok((s, a)) => (s, a),
            Err(e) => {
                println!("Error: {e}");
                continue;
            }
        };
        let map_broadcaster = Arc::clone(&map_broadcast);
        let map_subscriber = Arc::clone(&map_subscribe);
        let tx_clone = tx.clone();
        let (mut read_half, write_half) = socket.into_split();
        let mut write_half = Some(write_half);
        tokio::spawn(async move {
            let mut initialized = false;
            let mut is_broadcaster = false;
            let mut message_topic: String = String::new();
            loop {
                match tokio::time::timeout(READ_TIMEOUT, read_framed(&mut read_half)).await {
                    Err(_) => {
                        println!("Read timeout for {addr}, closing connection");
                        break;
                    }
                    Ok(Err(e)) => {
                        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                            if io_err.kind() == ErrorKind::UnexpectedEof {
                                println!("Closing connection");
                            } else {
                                println!("Error reading: {e}");
                            }
                        } else {
                            println!("Error reading: {e}");
                        }
                        break;
                    }
                    Ok(Ok(buf)) => match serde_json::from_slice::<Payload>(&buf) {
                        Ok(Payload::Request(msg)) => {
                            println!("Request from {addr}: {msg:#?}");
                            match msg.request_type {
                                RequestType::NewBroadcaster => {
                                    if !initialized {
                                        match map_broadcaster.entry(msg.message_topic.clone()) {
                                            dashmap::mapref::entry::Entry::Occupied(_) => {
                                                println!(
                                                    "Topic {} already has a broadcaster, closing connection",
                                                    msg.message_topic
                                                );
                                                break;
                                            }
                                            dashmap::mapref::entry::Entry::Vacant(v) => {
                                                message_topic.clone_from(&msg.message_topic);
                                                v.insert(addr);
                                                map_subscriber
                                                    .entry(msg.message_topic)
                                                    .or_insert_with(|| {
                                                        Arc::new(RwLock::new(Vec::new()))
                                                    });
                                                initialized = true;
                                                is_broadcaster = true;
                                            }
                                        }
                                    }
                                }
                                RequestType::NewSubscriber => {
                                    if !initialized {
                                        if let Some(wh) = write_half.take() {
                                            loop {
                                                let subs_arc = {
                                                    let entry = map_subscriber
                                                        .entry(msg.message_topic.clone())
                                                        .or_insert_with(|| {
                                                            Arc::new(RwLock::new(Vec::new()))
                                                        });
                                                    Arc::clone(&entry)
                                                };

                                                let mut write_guard = subs_arc.write().await;

                                                let is_valid = map_subscriber
                                                    .get(&msg.message_topic)
                                                    .is_some_and(|current_arc| {
                                                        Arc::ptr_eq(&current_arc, &subs_arc)
                                                    });

                                                if is_valid {
                                                    write_guard
                                                        .push((addr, Arc::new(Mutex::new(wh))));
                                                    drop(write_guard); // Tighten the drop
                                                    message_topic = msg.message_topic.clone();
                                                    initialized = true;
                                                    break;
                                                }
                                            }
                                        } else {
                                            println!(
                                                "Write half already taken, closing connection"
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Payload::Broadcast(msg)) => {
                            if !initialized {
                                println!(
                                    "Client sent broadcast before registering, closing connection"
                                );
                                break;
                            }
                            if !is_broadcaster {
                                println!("Subscriber cannot broadcast messages");
                            }
                            if msg.message_topic != message_topic {
                                println!(
                                    "Client sent broadcast to wrong topic, closing connection"
                                );
                                break;
                            }
                            if let Some(broadcaster_addr) = map_broadcaster.get(&msg.message_topic)
                            {
                                if addr == *broadcaster_addr {
                                    match tx_clone.send(msg).await {
                                        Ok(()) => println!("Broadcasted message"),
                                        Err(e) => println!("Error: {e}"),
                                    }
                                }
                            } else {
                                println!("No topic {}", msg.message_topic);
                                break;
                            }
                        }
                        Err(e) => {
                            println!("Error: {e}");
                            break;
                        }
                    },
                }
            }

            // Cleanup regardless of why the loop exited
            let subs = map_subscriber.get(&message_topic).map(|v| Arc::clone(&v));
            if is_broadcaster {
                map_broadcaster.remove(&message_topic);
                map_subscriber.remove_if(&message_topic, |_, arc_rwlock| {
                    arc_rwlock
                        .try_read()
                        .is_ok_and(|inner_guard| inner_guard.is_empty())
                });
            } else if let Some(subs) = subs {
                let mut subs_lock = subs.write().await;
                if let Some(pos) = subs_lock.iter().position(|(a, _)| *a == addr) {
                    subs_lock.swap_remove(pos);
                }
                if subs_lock.is_empty() {
                    drop(subs_lock);
                    map_subscriber.remove_if(&message_topic, |_, arc_rwlock| {
                        arc_rwlock
                            .try_read()
                            .is_ok_and(|inner_guard| inner_guard.is_empty())
                    });
                }
            }
            println!("Cleaned up connection: {addr}");
        });
    }
}
