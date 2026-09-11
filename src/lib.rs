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
//! The endpoint, the percent-encoding and HTTP itself come from the http
//! technology; the Query API and the signer for a service that is not S3
//! from the aws-sqs technology, which built them for this crate to take;
//! the flat XML scan from the capability (ADR-0044).
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

pub mod client;
pub mod session;
pub mod subscription;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, VERSION};
pub use session::{Event, Session};
pub use subscription::Delivery;
use transport::error::{Result, TransportError};
use transport::socket;
use transport::{Arrived, Directions, Transport};
pub use transport_aws_sqs::query::refusal;

/// The largest message SNS carries: 256 KiB.
#[must_use]
pub const fn ceiling() -> usize {
    256 * 1024
}

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
}
