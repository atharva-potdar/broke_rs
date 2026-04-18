use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

async fn make_conn() -> TcpStream {
    TcpStream::connect("127.0.0.1:8080").await.unwrap()
}

async fn send(stream: &mut TcpStream, payload: serde_json::Value) {
    stream
        .write_all(serde_json::to_vec(&payload).unwrap().as_slice())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_broadcast() {
    tokio::spawn(broke_rs::run_broker());
    sleep(Duration::from_millis(100)).await;

    let mut broadcaster = make_conn().await;
    send(
        &mut broadcaster,
        json!({"request_type": "NewBroadcaster", "message_topic": "match.1234"}),
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    let mut sub1 = make_conn().await;
    let mut sub2 = make_conn().await;
    let mut sub3 = make_conn().await;

    send(
        &mut sub1,
        json!({"request_type": "NewSubscriber", "message_topic": "match.1234"}),
    )
    .await;
    send(
        &mut sub2,
        json!({"request_type": "NewSubscriber", "message_topic": "match.1234"}),
    )
    .await;
    send(
        &mut sub3,
        json!({"request_type": "NewSubscriber", "message_topic": "match.1234"}),
    )
    .await;
    sleep(Duration::from_millis(100)).await;

    send(
        &mut broadcaster,
        json!({"message_topic": "match.1234", "message": "GOAL! 1-0"}),
    )
    .await;

    let mut buf = [0u8; 1024];

    let n = sub1.read(&mut buf).await.unwrap();
    let msg1: serde_json::Value = serde_json::from_slice(&buf[..n]).unwrap();

    let n = sub2.read(&mut buf).await.unwrap();
    let msg2: serde_json::Value = serde_json::from_slice(&buf[..n]).unwrap();

    let n = sub3.read(&mut buf).await.unwrap();
    let msg3: serde_json::Value = serde_json::from_slice(&buf[..n]).unwrap();

    assert_eq!(msg1["message"], "GOAL! 1-0");
    assert_eq!(msg2["message"], "GOAL! 1-0");
    assert_eq!(msg3["message"], "GOAL! 1-0");
}
