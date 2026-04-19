use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

async fn make_conn() -> TcpStream {
    TcpStream::connect("127.0.0.1:8080").await.unwrap()
}

async fn send_framed(stream: &mut TcpStream, payload: serde_json::Value) {
    let data = serde_json::to_vec(&payload).unwrap();
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await.unwrap();
    stream.write_all(&data).await.unwrap();
}

async fn recv_framed(stream: &mut TcpStream) -> serde_json::Value {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.unwrap();
    serde_json::from_slice(&buf).unwrap()
}

#[tokio::test]
async fn test_broadcast() {
    tokio::spawn(broke_rs::run_broker());
    sleep(Duration::from_millis(100)).await;

    let mut broadcaster = make_conn().await;
    send_framed(
        &mut broadcaster,
        json!({"request_type": "NewBroadcaster", "message_topic": "match.1234"}),
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    let mut sub1 = make_conn().await;
    let mut sub2 = make_conn().await;
    let mut sub3 = make_conn().await;

    send_framed(
        &mut sub1,
        json!({"request_type": "NewSubscriber", "message_topic": "match.1234"}),
    )
    .await;
    send_framed(
        &mut sub2,
        json!({"request_type": "NewSubscriber", "message_topic": "match.1234"}),
    )
    .await;
    send_framed(
        &mut sub3,
        json!({"request_type": "NewSubscriber", "message_topic": "match.1234"}),
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    send_framed(
        &mut broadcaster,
        json!({"message_topic": "match.1234", "message": "GOAL! 1-0"}),
    )
    .await;

    let msg1 = recv_framed(&mut sub1).await;
    let msg2 = recv_framed(&mut sub2).await;
    let msg3 = recv_framed(&mut sub3).await;

    assert_eq!(msg1["message"], "GOAL! 1-0");
    assert_eq!(msg2["message"], "GOAL! 1-0");
    assert_eq!(msg3["message"], "GOAL! 1-0");
}
