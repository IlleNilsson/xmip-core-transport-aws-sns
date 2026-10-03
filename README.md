# xmip-core-transport-aws-sns

Amazon SNS transport: Signature Version 4 over the Query API — publish a Stream as a message to a topic, and receive as the HTTP endpoint a subscription delivers to, confirming itself and taking each notification as a Stream — a topic ARN is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

Signature Version 4 and the Query API come from [xmip-core-transport-aws](https://github.com/IlleNilsson/xmip-core-transport-aws), where every AWS technology shares what AWS speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

A Stream is bytes and SNS carries a message as text (ADR-0038, amendment 2026-09-26): the transport takes and hands up bytes, and turns them into UTF-8 text only on the wire. A payload that is not UTF-8 of the characters XML permits is refused at publish with the reason, never replaced.

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

A Receive Location keeps its listener, bound on the first receive, and the connections senders keep open on it (`http::inbound::Inbound`): each receive takes the next request from whichever sends first, where until 2026-09-27 each receive bound a listener of its own, answered one request with `Connection: close`, and refused a request that came between two receives.

## Acknowledged after the receive cycle

SNS waits on its connection for the answer to a notification until the runtime's whole receive cycle has ended (runtime-model section 5): `202` on `Accepted`; `401`, `403` or `422` on `Refused` (`http::server::status`), a `4xx` SNS's delivery policy does not retry, so the notification is not delivered again; `503` on `Failed`, which SNS retries by the subscription's delivery policy, so the notification is delivered again. The `SubscriptionConfirmation` is the handshake, not a Stream: it is answered at once and confirmed by fetching its `SubscribeURL`. No round trip is added: the answer is the one SNS always waited for, only later.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
