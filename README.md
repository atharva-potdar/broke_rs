# Broke_rs

Broke_rs is a single-node pub/sub broker written in Rust.

Producers publish to named topics, consumers subscribe to topics and receive every message in real time.

Extensive work has been done to ensure there are no concurrency issues or performance issues.

## Known Limitations Right Now
- A single global mpsc channel routes all topics. High traffic on one topic may affect the others.
- An actor model would make the horrid type for map_subscriber a lot nicer.
