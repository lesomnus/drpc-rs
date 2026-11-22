# drpc-rs

Serve Rust handlers over a browser MessagePort using the [dRPC wire protocol](https://github.com/lesomnus/grpc-dgram/blob/a7366077bf6fa40598071f06497202917468e5b6/docs/PROTOCOL.md).
The web client uses `dialWorker` from `@lesomnus/grpc-dgram/transport/port` and passes `createDrpcTransport(conn)` to Connect's ordinary generated client.

Pass the transferred port, a random nonzero server incarnation, and a Connect Rust dispatcher (such as a registered `Router`) to `PortServer::bind`. Keep the returned handle alive until the port should close. Each handle serves one client incarnation. The native `Session` API accepts an ordered byte sender and a local executor.

The initial server supports unary and server-streaming methods, protobuf and JSON payloads, gRPC status, metadata, per-call timeouts, cancellation and stream/peer credit on a reliable channel. Client-streaming and bidirectional calls receive Unimplemented. Unreliable transports, compression and a Rust client are outside this implementation. The published TypeScript peer used for interoperability is `@lesomnus/grpc-dgram@0.1.0`.

## Development

Run `cargo test --locked` and `cargo check --locked --target wasm32-unknown-unknown`. The checked-in wire types come from the upstream Edition 2023 files in `proto/drpc`. Regenerate with `buf generate proto` using the Buffa 0.8.1 plugins. The [directory example](https://github.com/protobuf-orm/protoc-gen-orm-rust/tree/main/example) exercises a browser Worker, generated services and Diesel SQLite.
