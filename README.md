# xmip-core-transport-aws-sns

Amazon SNS transport: Signature Version 4 over the Query API — publish a Stream as a message to a topic, and receive as the HTTP endpoint a subscription delivers to, confirming itself and taking each notification as a Stream — a topic ARN is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location publishes to the topic its `topic_arn` setting names: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which it reads, through `transport::destination::named`, as that topic, its address naming only the endpoint. A target naming another topic ARN still wins (ADR-0064, amendment 2026-11-10).

Signature Version 4 and the Query API come from [xmip-core-transport-aws](https://github.com/IlleNilsson/xmip-core-transport-aws), where every AWS technology shares what AWS speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

A Send Location publishes as the access key ID its `access_key_id` setting names, with the secret access key its `secret` holds: a `password` secret kept in the Administration Database and never in configuration, read at each publish, so one set or replaced reaches the next without a restart (ADR-0019, amendment 2026-10-29; ADR-0015, amendment 2026-11-25). A Location naming a secret and no `access_key_id` is refused as the node starts: Signature Version 4 names the access key ID with every signature (`aws::access_key`). A Receive Location is delivered to and presents nothing; its confirmation is the one unsigned call.

A Stream is bytes and SNS carries a message as text (ADR-0038, amendment 2026-09-26): the transport takes and hands up bytes, and turns them into UTF-8 text only on the wire. A payload that is not UTF-8 of the characters XML permits is refused at publish with the reason, never replaced.

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

A Receive Location keeps its listener, bound as its transport is opened (`Configured::listening`; on its first receive until 2026-11-16), and the connections senders keep open on it (`http::inbound::Inbound`): each receive takes the next request from whichever sends first, where until 2026-09-27 each receive bound a listener of its own, answered one request with `Connection: close`, and refused a request that came between two receives. A request that is not a delivery is answered at once, its caller turned away, and the receive goes on for the others (ADR-0018, amendment 2026-11-13).

## Acknowledged after the receive cycle

SNS waits on its connection for the answer to a notification until the runtime's whole receive cycle has ended (runtime-model section 5): `202` on `Accepted`; `401`, `403` or `422` on `Refused` (`http::server::status`), a `4xx` SNS's delivery policy does not retry, so the notification is not delivered again; `503` on `Failed`, which SNS retries by the subscription's delivery policy, so the notification is delivered again. The `SubscriptionConfirmation` is the handshake, not a Stream: it is answered at once and confirmed by fetching its `SubscribeURL`. No round trip is added: the answer is the one SNS always waited for, only later.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, `region`, `access_key_id`, `topic_arn`, `listen` and `timeout`, and `SCHEMES`, `aws-sns`; `access_key_id` is the AWS crate's, which this crate's declaration takes by that crate's own `declaration` feature. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on — the http technology, and with it TLS — are behind the default feature, and `tls`, which is code, turns the default on with it. So the library the surfaces load can carry it without the code, for the language server and the desktop to suggest and check an SNS Location's settings, its `access_key_id` among them, though no node in their process loaded the technology, once the gathering crate of open problem 48 links it. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
