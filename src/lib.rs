use std::sync::Arc;

use dashmap::DashMap;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::{io::AsyncReadExt, sync::mpsc};

use serde::{Deserialize, Serialize};

use anyhow::Result;

use rayon::prelude::*;

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

pub async fn run_broker() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    // For now, one broadcaster per message_topic
    let map_broadcast = Arc::new(DashMap::new());
    let map_subscribe =
        Arc::new(DashMap::<String, Mutex<Vec<(SocketAddr, OwnedWriteHalf)>>>::new());

    // Buffer size 1024
    let (tx, mut rx) = mpsc::channel::<BroadcastMessage>(1024);

    let map_subscriber2 = Arc::clone(&map_subscribe);

    tokio::spawn(async move {
        let handle = tokio::runtime::Handle::current();
        while let Some(msg) = rx.recv().await {
            let message = serde_json::to_vec(&msg).unwrap();
            if let Some(subs) = map_subscriber2.get(&msg.message_topic) {
                loop {
                    if let Ok(mut subs_lock) = subs.try_lock() {
                        subs_lock.par_iter_mut().for_each(|(_, write_half)| {
                            let data = message.clone();
                            handle.block_on(async move {
                                use tokio::io::AsyncWriteExt;
                                write_half.write_all(&data).await.unwrap();
                            });
                        });
                        break;
                    }
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
            let mut buf = [0u8; 1024];
            let mut initialized = false;
            let mut is_broadcaster = false;
            let mut message_topic: String = String::new();
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) => {
                        println!("Closing connection");
                        if !is_broadcaster {
                            if let Some(subs) = map_subscriber.get(&message_topic) {
                                loop {
                                    if let Ok(mut subs_lock) = subs.try_lock() {
                                        if let Some(pos) =
                                            subs_lock.iter().position(|(a, _)| *a == addr)
                                        {
                                            subs_lock.swap_remove(pos);
                                        }
                                        break;
                                    }
                                }
                            }
                        } else {
                            map_broadcaster.remove(&message_topic);
                        }
                        break;
                    }
                    Ok(n) => match serde_json::from_slice::<Payload>(&buf[..n]) {
                        Ok(Payload::Request(msg)) => {
                            println!("Request from {addr}: {msg:#?}");
                            match msg.request_type {
                                RequestType::NewBroadcaster => {
                                    if !initialized {
                                        message_topic = msg.message_topic.clone();
                                        map_broadcaster.insert(msg.message_topic.clone(), addr);
                                        map_subscriber.entry(msg.message_topic).or_default();
                                        initialized = true;
                                        is_broadcaster = true;
                                    }
                                }
                                RequestType::NewSubscriber => {
                                    if !initialized {
                                        message_topic = msg.message_topic.clone();
                                        let entry =
                                            map_subscriber.entry(msg.message_topic).or_default();
                                        initialized = true;
                                        loop {
                                            if let Ok(mut subs) = entry.try_lock() {
                                                subs.push((addr, write_half.take().unwrap()));
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Payload::Broadcast(msg)) => {
                            match map_broadcaster.get(&msg.message_topic) {
                                Some(broadcaster_addr) => {
                                    if addr == *broadcaster_addr {
                                        match tx_clone.send(msg).await {
                                            Ok(()) => println!("Broadcasted message"),
                                            Err(e) => println!("Error: {e}"),
                                        }
                                    } else {
                                        println!("Subscriber cannot broadcast messages");
                                    }
                                }
                                None => {
                                    println!("No topic {}", msg.message_topic);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error: {e}");
                            break;
                        }
                    },
                    Err(e) => {
                        println!("Error: {e}");
                        break;
                    }
                }
            }
        });
    }
}
