# xmip-core-transport-aws-sns

Amazon SNS transport: Signature Version 4 over the Query API — publish a Stream as a message to a topic, and receive as the HTTP endpoint a subscription delivers to, confirming itself and taking each notification as a Stream — a topic ARN is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

Signature Version 4 and the Query API come from [xmip-core-transport-aws](https://github.com/IlleNilsson/xmip-core-transport-aws), where every AWS technology shares what AWS speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

A Stream is bytes and SNS carries a message as text (ADR-0038, amendment 2026-09-26): the transport takes and hands up bytes, and turns them into UTF-8 text only on the wire. A payload that is not UTF-8 of the characters XML permits is refused at publish with the reason, never replaced.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
