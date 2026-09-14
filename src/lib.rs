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
//! The endpoint, the percent-encoding, HTTP itself, the Query API and
//! Signature Version 4 come from the http technology, the flat XML scan
//! from the capability (ADR-0044). Until 2026-09-14 the Query API and the
//! signer came from the aws-sqs technology, a sideways import the record
//! forbids; what rides on HTTP is shared through the http technology.
//!
//! A message is text — one to 256 KiB of the characters XML permits — and
//! the transport carries bytes as they are or says why it cannot: what is
//! not that text is refused before a request is formed, never encoded and
//! called delivered. [`ceiling`] and [`refusal`] say both rules.
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

use std::net::{TcpListener, TcpStream};
use std::time::Duration;

pub use client::{Client, VERSION};
use http::endpoint;
pub use http::query::refusal;
pub use session::{Event, Session};
pub use subscription::Delivery;
use transport::error::{Result, TransportError, protocol_error};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

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
        match subscription::accept_one(listener, self.timeout)? {
            Delivery::Notification {
                topic_arn,
                message_id,
                message,
            } => Ok(vec![Arrived::new(
                session::origin(&topic_arn, &message_id),
                message.into_bytes(),
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
    /// a confirmation, now confirmed.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let (listener, _) = self.bind()?;
        self.accept_one(&listener)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() > ceiling() {
            return Err(TransportError::permanent(format!(
                "{} bytes is over the {} one SNS message carries",
                bytes.len(),
                ceiling()
            )));
        }
        self.client()?
            .publish(self.resolve(target), bytes)
            .map(|_| ())
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

/// A session listening for its one publish, and the endpoint it then
/// delivers to: the far end is SNS and the subscription both, so what
/// comes back went through the topic and arrived the way a Receive
/// Location takes it.
struct Serving {
    transport: SnsTransport,
    session: Session,
    listener: TcpListener,
    address: String,
    subscription: TcpListener,
    subscription_address: String,
}

impl FarEnd for Serving {
    fn address(&self) -> &str {
        &self.address
    }

    fn take_one(mut self: Box<Self>) -> Result<Arrived> {
        let published = match self.session.serve_one(&self.listener)? {
            Event::Published(arrived) => arrived,
            other => {
                return Err(protocol_error(format!("{other:?} where a publish was due")));
            }
        };
        let notification = notification(&published)?;
        let Serving {
            transport,
            session,
            subscription,
            subscription_address,
            ..
        } = *self;
        let taking = std::thread::spawn(move || transport.accept_one(&subscription));
        let delivered =
            session.deliver(&format!("http://{subscription_address}/sns"), &notification);
        if delivered.is_err() {
            drop(TcpStream::connect(&subscription_address));
        }
        let taken = taking
            .join()
            .map_err(|_| protocol_error("the endpoint's thread panicked"))?;
        delivered?;
        taken?
            .into_iter()
            .next()
            .ok_or_else(|| protocol_error("delivered, but the endpoint took nothing"))
    }
}

/// The notification SNS delivers for what was published: the Stream back
/// as the text it is, under the topic and the id the session gave it.
fn notification(published: &Arrived) -> Result<Delivery> {
    let (topic_arn, message_id) = published
        .origin_uri
        .rsplit_once('#')
        .ok_or_else(|| protocol_error("a publish with no message id"))?;
    let message = String::from_utf8(published.bytes.clone())
        .map_err(|_| protocol_error("a published message that is not text"))?;
    Ok(Delivery::Notification {
        topic_arn: topic_arn.to_string(),
        message_id: message_id.to_string(),
        message,
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

    /// The session bound at the endpoint's authority — `127.0.0.1:0` for
    /// the loopback — and the subscription's endpoint bound where this
    /// transport listens.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = socket::bind_tcp(&endpoint::authority(&self.endpoint)?)?;
        let (subscription, subscription_address) = self.bind()?;
        Ok(Box::new(Serving {
            transport: self.clone(),
            session: self.session(),
            listener,
            address,
            subscription,
            subscription_address,
        }))
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
