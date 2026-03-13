## How tos

1. Start the server: `cargo build --release && RUST_LOG=debug ./target/release/q --port 8085 --data-dir <dir> --num-partition 8`
1. Push: `grpcurl -plaintext -proto ../proto/proto/q/v1/api.proto -d '{"queue_id": "ok", "message": "VGhpcyBpcyBhIG1lc3NhZ2UK"}' 0.0.0.0:8085 q.v1.QueueService/Push`
  - Get grpcurl: `brew install grpcurl`
1. Pop: `grpcurl -plaintext -proto ../proto/proto/q/v1/api.proto -d '{"queue_id": "ok"}' 0.0.0.0:8085 q.v1.QueueService/Pop`
1. Run scaling benchmark: `cargo test --features bench-stress benchmark_scaling_on_mac -- --nocapture`
