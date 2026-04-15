use std::{net::SocketAddr, sync::Arc};

use dashmap::{DashMap, DashSet};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

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

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    // For now, one broadcaster per message_topic
    let map_broadcast = Arc::new(DashMap::new());
    let map_subscribe = Arc::new(DashMap::<String, DashSet<SocketAddr>>::new());

    loop {
        let (mut socket, addr) = match listener.accept().await {
            Ok((s, a)) => (s, a),
            Err(e) => {
                println!("Error: {e}");
                continue;
            }
        };
        let map_broadcaster = Arc::clone(&map_broadcast);
        let map_subscriber = Arc::clone(&map_subscribe);
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];

            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => {
                        println!("Empty request");
                        break;
                    }
                    Ok(n) => match serde_json::from_slice::<Payload>(&buf[..n]) {
                        Ok(Payload::Request(msg)) => {
                            println!("Request from {addr}: {msg:#?}");
                            match msg.request_type {
                                RequestType::NewBroadcaster => {
                                    map_broadcaster.insert(msg.message_topic.clone(), addr);
                                    map_subscriber.entry(msg.message_topic).or_default();
                                }
                                RequestType::NewSubscriber => {
                                    map_subscriber
                                        .entry(msg.message_topic)
                                        .or_default()
                                        .insert(addr);
                                }
                            }
                        }
                        Ok(Payload::Broadcast(msg)) => {
                            match map_broadcaster.get(&msg.message_topic) {
                                Some(broadcaster_addr) => {
                                    if addr == *broadcaster_addr {
                                        println!("Broadcast from {addr}: {msg:#?}");
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
