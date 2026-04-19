use std::io::ErrorKind;
use std::sync::Arc;

use dashmap::DashMap;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
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
        Arc<Mutex<Vec<(SocketAddr, OwnedWriteHalf)>>>,
    >::new());

    // Buffer size 1024
    let (tx, mut rx) = mpsc::channel::<BroadcastMessage>(1024);

    let map_subscriber2 = Arc::clone(&map_subscribe);

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let message = match serde_json::to_vec(&msg) {
                Ok(m) => m,
                Err(e) => {
                    println!("Failed to serialize message: {e}");
                    continue;
                }
            };
            let subs = map_subscriber2
                .get(&msg.message_topic)
                .map(|v| Arc::clone(&v));
            if let Some(subs) = subs {
                let mut subs_lock = subs.lock().await;
                let futs = subs_lock
                    .iter_mut()
                    .map(|(_, write_half)| write_framed(write_half, &message));
                let results = futures::future::join_all(futs).await;
                let mut dead = vec![];
                for (i, result) in results.into_iter().enumerate() {
                    if let Err(e) = result {
                        println!("Failed to write to subscriber, removing: {e}");
                        dead.push(i);
                    }
                }
                for i in dead.into_iter().rev() {
                    subs_lock.swap_remove(i);
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
                match read_framed(&mut read_half).await {
                    Err(e) => {
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
                    Ok(buf) => match serde_json::from_slice::<Payload>(&buf) {
                        Ok(Payload::Request(msg)) => {
                            println!("Request from {addr}: {msg:#?}");
                            match msg.request_type {
                                RequestType::NewBroadcaster => {
                                    if map_broadcaster.contains_key(&msg.message_topic) {
                                        println!(
                                            "Topic {} already has a broadcaster, closing connection",
                                            msg.message_topic
                                        );
                                        break;
                                    }
                                    if !initialized {
                                        message_topic.clone_from(&msg.message_topic);
                                        map_broadcaster.insert(msg.message_topic.clone(), addr);
                                        map_subscriber
                                            .entry(msg.message_topic)
                                            .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
                                        initialized = true;
                                        is_broadcaster = true;
                                    }
                                }
                                RequestType::NewSubscriber => {
                                    if !initialized {
                                        message_topic.clone_from(&msg.message_topic);
                                        let entry = map_subscriber
                                            .entry(msg.message_topic)
                                            .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
                                        let subs_arc = Arc::clone(&entry);
                                        drop(entry);
                                        initialized = true;
                                        if let Some(wh) = write_half.take() {
                                            subs_arc.lock().await.push((addr, wh));
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
                            if let Some(broadcaster_addr) = map_broadcaster.get(&msg.message_topic)
                            {
                                if addr == *broadcaster_addr {
                                    match tx_clone.send(msg).await {
                                        Ok(()) => println!("Broadcasted message"),
                                        Err(e) => println!("Error: {e}"),
                                    }
                                } else {
                                    println!("Subscriber cannot broadcast messages");
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
            } else if let Some(subs) = subs {
                let mut subs_lock = subs.lock().await;
                if let Some(pos) = subs_lock.iter().position(|(a, _)| *a == addr) {
                    subs_lock.swap_remove(pos);
                }
                if subs_lock.is_empty() {
                    drop(subs_lock);
                    map_subscriber.remove(&message_topic);
                }
            }
            println!("Cleaned up connection: {addr}");
        });
    }
}
