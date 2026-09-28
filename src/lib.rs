#![forbid(unsafe_code)]

//! Streams that travel as SNS messages. One message is one Stream, its id
//! kept beside it.
//!
//! SNS is the fan-out of every organisation that lives in AWS: a topic, and
//! subscriptions that deliver what is published to it. A Send Location
//! publishes a Stream as one message to a topic; a Receive Location is the
//! HTTP endpoint a subscription delivers to — it confirms the subscription
//! SNS offers it by fetching the URL SNS sends, then takes each
//! notification as a Stream. Publishing is Signature Version 4 over the
//! Query API on plain HTTP/1.1 — `https://` with the `tls` feature, which
//! is the http technology's TLS (ADR-0033); delivery is SNS calling in.
//!
//! ```text
//! client.rs        Xmip's side: publish, confirm
//! subscription.rs  the endpoint SNS delivers to, and what it delivers
//! session.rs       the far end a test or the playground runs on loopback
//! ```
//!
//! The endpoint and HTTP itself come from the http technology, the
//! percent-encoding from `net`; the Query API and Signature Version 4 from
//! the AWS crate, the flat XML scan from the capability (ADR-0044). Until
//! 2026-09-14 the Query API and the signer came from the aws-sqs technology,
//! a sideways import the record forbids; what AWS speaks is shared through
//! the AWS crate (the owner's ruling of 2026-09-22).
//!
//! A payload is bytes (ADR-0038, amendment 2026-09-26), and SNS carries a
//! message as text — one to 256 KiB of the characters XML permits, UTF-8.
//! Only the wire is text: the transport takes bytes and hands bytes up, and
//! turns them into text where a publish or a delivery is written. What is
//! not that text is refused there with the reason, never replaced and
//! called delivered. [`ceiling`] and [`aws::query::refusal`] say both rules.
//!
//! A topic is not an artefact anyone claims, so [`Transport::claims`]
//! answers `None`. The origin URI is the topic ARN with the message id as
//! its fragment. A send target is a topic ARN, or empty for this
//! transport's own topic.
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! session up at the endpoint's authority as SNS, takes the one publish,
//! and delivers it to the subscription this transport listens as.

pub mod client;
pub mod session;
pub mod subscription;

use std::net::TcpListener;
use std::time::Duration;

use aws::query::refusal;
pub use client::{Client, VERSION};
use http::endpoint::Connections;
use http::inbound::Inbound;
use net::Endpoint;
use net::ceiling;
pub use session::{Event, Session};
pub use subscription::Delivery;
use transport::arrived::next_arrival;
use transport::error::{Result, protocol_error};
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback, both_ends, poke};
use transport::socket;
use transport::{Arrived, Configured, Directions, Transport};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

/// The largest message SNS carries: 256 KiB.
#[must_use]
pub const fn ceiling() -> usize {
    256 * 1024
}

#[derive(Clone)]
pub struct SnsTransport {
    endpoint: String,
    region: String,
    topic_arn: String,
    access_key: String,
    secret_key: String,
    bind: String,
    timeout: Option<Duration>,
    /// The connections kept to the service, shared by every client this
    /// makes.
    connections: Connections,
    /// The subscription's listener a Receive Location keeps, and the
    /// connections SNS keeps on it.
    inbound: Inbound,
}

impl SnsTransport {
    /// Speak to the SNS endpoint at `endpoint` — `https://sns.<region>.
    /// amazonaws.com` in the cloud, `http://host:port` for a stand-in — in
    /// `region`, about the topic at `topic_arn`.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, region: &str, topic_arn: &str) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.to_string(),
            topic_arn: topic_arn.to_string(),
            access_key: String::new(),
            secret_key: String::new(),
            bind: "127.0.0.1:0".to_string(),
            timeout: None,
            connections: Connections::new(),
            inbound: Inbound::new(),
        }
    }

    /// Sign as this access key.
    #[must_use]
    pub fn with_credentials(mut self, access_key: &str, secret_key: &str) -> Self {
        self.access_key = access_key.to_string();
        self.secret_key = secret_key.to_string();
        self
    }

    /// Listen for deliveries at `bind` — the address the subscription's
    /// endpoint URL resolves to.
    #[must_use]
    pub fn listening_at(mut self, bind: &str) -> Self {
        self.bind = bind.to_string();
        self
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The client this transport speaks through.
    ///
    /// # Errors
    /// Where the endpoint is not an HTTP URL.
    pub fn client(&self) -> Result<Client> {
        let client = Client::new(
            &self.endpoint,
            &self.region,
            &self.access_key,
            &self.secret_key,
        )?;
        let client = client.sharing(self.connections.clone());
        Ok(match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        })
    }

    /// A far end that holds this transport's credentials, for a test or the
    /// playground to run on loopback.
    #[must_use]
    pub fn session(&self) -> Session {
        let session = Session::new(&self.region, &self.access_key, &self.secret_key);
        match self.timeout {
            Some(timeout) => session.timing_out_after(timeout),
            None => session,
        }
    }

    /// Bind the endpoint and report the address actually assigned.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.bind)
    }

    /// Take one delivery off an already-bound listener: a notification as
    /// the Stream it carries; a confirmation, confirmed by fetching its
    /// URL, as nothing yet.
    ///
    /// # Errors
    /// Where the connection failed, the delivery was not one, or a
    /// confirmation could not be fetched.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Vec<Arrived>> {
        self.arrivals(subscription::accept_one(listener, self.timeout)?)
    }

    /// What a delivery comes to: a notification as the Stream it carries;
    /// a confirmation, confirmed by fetching its URL, as nothing yet.
    fn arrivals(&self, delivery: Delivery) -> Result<Vec<Arrived>> {
        match delivery {
            Delivery::Notification {
                topic_arn,
                message_id,
                message,
            } => Ok(vec![Arrived::new(
                session::origin(&topic_arn, &message_id),
                message,
            )]),
            Delivery::Confirmation { subscribe_url, .. } => {
                self.client()?.confirm(&subscribe_url)?;
                Ok(Vec::new())
            }
            Delivery::Other(_) => Ok(Vec::new()),
        }
    }

    /// The topic a target names, or this transport's own where it names
    /// none.
    fn resolve<'a>(&'a self, target: &'a str) -> &'a str {
        if target.is_empty() {
            &self.topic_arn
        } else {
            target
        }
    }
}

impl Transport for SnsTransport {
    fn name(&self) -> &'static str {
        "aws-sns"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// One delivery: the notification it carried, or nothing where it was
    /// a confirmation, now confirmed. Taken from whichever connection SNS
    /// posts on first, on the listener the first receive bound and kept.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let delivery = self.inbound.next(
            || self.bind(),
            self.timeout,
            |request, _| subscription::answer(request),
        )??;
        self.arrivals(delivery)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        ceiling::within(bytes.len(), ceiling(), "one SNS message carries")?;
        self.client()?
            .publish(self.resolve(target), bytes)
            .map(|_| ())
    }
}

impl Configured for SnsTransport {
    /// The address is the SNS endpoint, `https://sns.<region>.amazonaws.com`:
    /// published to on send, confirmed through on receive. The access key
    /// and its secret are the Location's credentials, not settings.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "region",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The AWS region requests are signed for, eu-north-1.",
                applies: Applies::Both,
            },
            Setting {
                name: "topic_arn",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The ARN of the topic published to when a send target names none.",
                applies: Applies::Send,
            },
            Setting {
                name: "listen",
                kind: Kind::Address,
                presence: Presence::Required,
                meaning: "Where a Receive Location listens for deliveries: the host and port \
                          the subscription's endpoint URL resolves to.",
                applies: Applies::Receive,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long an endpoint that stops answering is waited on; unbounded \
                          when left out.",
                applies: Applies::Both,
            },
        ],
    };

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        // The access key and secret come through the Location's credentials.
        let topic_arn = settings.optional_text("topic_arn").unwrap_or_default();
        let mut transport = Self::new(address, settings.text("region"), topic_arn);
        if let Some(listen) = settings.optional_text("listen") {
            transport = transport.listening_at(listen);
        }
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

/// The topic the loopback publishes to and is subscribed to.
pub const LOOPBACK_TOPIC: &str = "arn:aws:sns:eu-north-1:123456789012:loopback";

impl SnsTransport {
    /// Both ends on this machine: the session stands in for SNS on an
    /// ephemeral local port, the subscription's endpoint listens on
    /// another, one topic and one credential, the loopback timeout on
    /// every side.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("http://127.0.0.1:0", "eu-north-1", LOOPBACK_TOPIC)
            .with_credentials("AKID", "secret")
            .timing_out_after(LOOPBACK_TIMEOUT)
    }

    /// A fresh near end aimed at the session at `address`, with this
    /// transport's credentials and topic.
    fn aimed_at(&self, address: &str) -> Self {
        let near = Self::new(format!("http://{address}"), &self.region, &self.topic_arn)
            .with_credentials(&self.access_key, &self.secret_key);
        match self.timeout {
            Some(timeout) => near.timing_out_after(timeout),
            None => near,
        }
    }
}

/// The notification SNS delivers for what was published: the Stream back
/// as the bytes it is, under the topic and the id the session gave it.
fn notification(published: &Arrived) -> Result<Delivery> {
    let (topic_arn, message_id) = published
        .origin_uri
        .rsplit_once('#')
        .ok_or_else(|| protocol_error("a publish with no message id"))?;
    Ok(Delivery::Notification {
        topic_arn: topic_arn.to_string(),
        message_id: message_id.to_string(),
        message: published.bytes.clone(),
    })
}

impl Loopback for SnsTransport {
    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    /// What SNS does not carry: a message is text, at least one character
    /// of it, every one permitted by XML 1.0.
    fn refuses(&self, payload: &[u8]) -> Option<String> {
        refusal(payload)
    }

    /// A session listening for its one publish, bound at the endpoint's
    /// authority — `127.0.0.1:0` for the loopback — and the endpoint it then
    /// delivers to, bound where this transport listens: the far end is SNS
    /// and the subscription both, so what comes back went through the topic
    /// and arrived the way a Receive Location takes it.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let transport = self.clone();
        let mut session = self.session();
        let (subscription, subscription_address) = self.bind()?;
        Ok(Box::new(Listening::new(
            move |listener: &TcpListener| {
                let published = match session.serve_one(listener)? {
                    Event::Published(arrived) => arrived,
                    other => {
                        return Err(protocol_error(format!("{other:?} where a publish was due")));
                    }
                };
                let notification = notification(&published)?;
                let url = format!("http://{subscription_address}/sns");
                let (delivered, taken) = both_ends(
                    move || transport.accept_one(&subscription),
                    || session.deliver(&url, &notification),
                    || poke(&subscription_address),
                );
                delivered?;
                next_arrival(taken?, "delivered, but the endpoint took nothing")
            },
            socket::bind_tcp(&Endpoint::parse(&self.endpoint)?.address())?,
        )))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        self.aimed_at(address).send("", payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC: &str = "arn:aws:sns:eu-north-1:123456789012:orders";

    fn node(endpoint: &str, secret: &str) -> SnsTransport {
        SnsTransport::new(endpoint, "eu-north-1", TOPIC)
            .with_credentials("AKID", secret)
            .timing_out_after(Duration::from_secs(2))
    }

    #[test]
    fn sns_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(SnsTransport::SETTINGS.problems(), Vec::<String>::new());
        let text = |name: &str, value: &str| (name.to_string(), Given::Text(value.to_string()));
        let endpoint = "https://sns.eu-north-1.amazonaws.com";
        let sent = SnsTransport::open(
            endpoint,
            Applies::Send,
            &[
                text("region", "eu-north-1"),
                text("topic_arn", TOPIC),
                text("timeout", "5s"),
            ],
        )
        .expect("built");
        assert_eq!(
            (sent.endpoint.as_str(), sent.topic_arn.as_str()),
            (endpoint, TOPIC)
        );
        assert_eq!(sent.timeout, Some(Duration::from_secs(5)));
        let received = SnsTransport::open(
            endpoint,
            Applies::Receive,
            &[text("region", "eu-north-1"), text("listen", "0.0.0.0:8443")],
        )
        .expect("built");
        assert_eq!(received.bind, "0.0.0.0:8443");
        let Err(refused) =
            SnsTransport::open(endpoint, Applies::Receive, &[text("region", "eu-north-1")])
        else {
            panic!("a Receive Location says where it listens");
        };
        assert!(refused.message.contains("\"listen\""), "{refused}");
    }

    #[test]
    fn every_receive_takes_from_the_listener_the_first_bound() {
        let endpoint = node("http://127.0.0.1:1", "secret");
        let address = endpoint.inbound.bound(|| endpoint.bind()).expect("bound");
        let url = format!("http://{address}/sns");
        let sns = std::thread::spawn(move || {
            for round in 0..5u8 {
                let delivery = Delivery::Notification {
                    topic_arn: TOPIC.to_string(),
                    message_id: format!("{round}-xmip"),
                    message: format!("round {round}").into_bytes(),
                };
                subscription::push(&url, &delivery, Some(Duration::from_secs(2))).expect("pushed");
            }
        });
        for round in 0..5u8 {
            let arrived = endpoint.receive().expect("received");
            assert_eq!(arrived[0].bytes, format!("round {round}").as_bytes());
        }
        sns.join().expect("sns");
    }

    #[test]
    fn what_is_published_is_delivered_to_the_confirmed_endpoint_and_received() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let sns = format!("http://{address}");
        let near = node(&sns, "secret");
        let (endpoint, endpoint_address) = near.bind().expect("bind the endpoint");
        let endpoint_url = format!("http://{endpoint_address}/sns");
        let mut session = near.session();
        let far_end = std::thread::spawn(move || {
            // One publish; then SNS offers the subscription, the endpoint
            // confirms it, and SNS delivers what was published.
            let published = session.serve_one(&listener).expect("served");
            let offer = Delivery::Confirmation {
                topic_arn: TOPIC.to_string(),
                subscribe_url: Session::subscribe_url(&sns, TOPIC),
            };
            session.deliver(&endpoint_url, &offer).expect("offered");
            let confirmed = session.serve_one(&listener).expect("served");
            let (origin, message) = session.messages().into_iter().next().expect("one message");
            let notification = Delivery::Notification {
                topic_arn: TOPIC.to_string(),
                message_id: origin.rsplit('#').next().unwrap_or_default().to_string(),
                message,
            };
            session
                .deliver(&endpoint_url, &notification)
                .expect("delivered");
            (published, confirmed)
        });
        near.send("", "r\u{e4}k <&> \"b\"\r\n".as_bytes())
            .expect("its own topic");
        assert!(near.accept_one(&endpoint).expect("offered").is_empty());
        let arrived = near.accept_one(&endpoint).expect("delivered");
        assert_eq!(arrived.len(), 1);
        assert_eq!(arrived[0].bytes, "r\u{e4}k <&> \"b\"\r\n".as_bytes());
        assert!(arrived[0].origin_uri.starts_with(&format!("{TOPIC}#")));
        let (published, confirmed) = far_end.join().expect("thread");
        assert_eq!(
            published,
            Event::Published(Arrived::new(
                arrived[0].origin_uri.clone(),
                arrived[0].bytes.clone()
            ))
        );
        assert!(matches!(confirmed, Event::Confirmed { topic_arn, .. } if topic_arn == TOPIC));
    }

    #[test]
    fn a_wrong_secret_is_refused_with_snss_own_status_and_code() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let mut session = node("http://x", "secret").session();
        let far_end = std::thread::spawn(move || session.serve_one(&listener).expect("served"));
        let failure = node(&format!("http://{address}"), "wrong")
            .send(TOPIC, b"x")
            .expect_err("refused");
        assert!(
            failure.message.contains("403 SignatureDoesNotMatch"),
            "{failure}"
        );
        assert!(!failure.retryable);
        assert_eq!(
            far_end.join().expect("thread"),
            Event::Refused("SignatureDoesNotMatch".to_string())
        );
    }

    #[test]
    fn a_topic_is_not_claimed_and_an_unreachable_endpoint_is_retryable() {
        let near = node("http://127.0.0.1:1", "secret");
        assert!(near.claims().is_none());
        assert_eq!(near.name(), "aws-sns");
        assert!(near.directions().receives() && near.directions().sends());
        assert!(
            near.send("", b"x")
                .expect_err("nothing listening")
                .retryable
        );
        assert!(
            !node("sns.local", "s")
                .send("", b"x")
                .expect_err("no scheme")
                .retryable
        );
        assert!(
            node("http://x", "s")
                .listening_at("not an address")
                .receive()
                .is_err()
        );
    }

    #[test]
    fn what_sns_does_not_carry_is_refused_before_the_wire_with_the_reason() {
        let near = node("http://127.0.0.1:1", "secret");
        let over = vec![b'x'; ceiling() + 1];
        let failure = near.send("", &over).expect_err("over the ceiling");
        assert!(!failure.retryable);
        assert!(failure.message.contains("262144"), "{failure}");
        let failure = near.send("", b"").expect_err("empty");
        assert!(!failure.retryable);
        assert!(failure.message.contains("at least one"), "{failure}");
        let failure = near.send("", &[b'r', 0xe4, b'k']).expect_err("Latin-1");
        assert!(!failure.retryable, "{failure}");
        assert!(
            failure.message.contains("SNS carries a message as text")
                && failure.message.contains("UTF-8"),
            "{failure}"
        );
        assert!(refusal(&[0xff]).is_some());
        assert!(refusal(b"text").is_none());
    }

    /// The edge payloads, and one at the brim: SNS carries the text among
    /// them and refuses the rest, which the test checks either way.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("the brim", vec![b'x'; ceiling()]),
        ]
    }

    #[test]
    fn a_loopback_round_publishes_and_takes_the_delivery_at_the_subscription() {
        let sns = SnsTransport::loopback();
        let arrived = sns
            .round("r\u{e4}k <&> \"b\"\r\n".as_bytes())
            .expect("round");
        assert_eq!(arrived.bytes, "r\u{e4}k <&> \"b\"\r\n".as_bytes());
        assert!(
            arrived
                .origin_uri
                .starts_with(&format!("{LOOPBACK_TOPIC}#")),
            "{}",
            arrived.origin_uri
        );
        assert_eq!(sns.name(), "aws-sns");
    }

    #[test]
    fn the_loopback_returns_the_text_edge_payloads_whole_and_refuses_the_rest() {
        let sns = SnsTransport::loopback();
        assert_eq!(sns.ceiling(), Some(256 * 1024));
        let mut carried = 0;
        for (name, payload) in edge_payloads() {
            match sns.refuses(&payload) {
                None => {
                    let arrived = sns.round(&payload).expect(name);
                    assert_eq!(arrived.bytes, payload, "{name}");
                    carried += 1;
                }
                Some(why) => {
                    let failure = sns.round(&payload).expect_err(name);
                    assert!(failure.message.starts_with("send failed:"), "{failure}");
                    assert!(failure.message.contains(&why), "{name}: {failure}");
                }
            }
        }
        assert_eq!(carried, 3, "one byte, the CRLF storm and the brim");
        let over = vec![b'x'; ceiling() + 1];
        let failure = sns.round(&over).expect_err("over the brim");
        assert!(failure.message.contains("262144"), "{failure}");
    }
}
