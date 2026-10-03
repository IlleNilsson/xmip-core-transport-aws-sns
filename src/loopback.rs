//! The transport as its own far end (ADR-0051): the session stands in for
//! SNS, takes the one publish, and delivers it as a notification to the
//! subscription this transport listens as, so what comes back went through
//! the topic and arrived the way a Receive Location takes it.

use std::net::TcpListener;

use aws::query::refusal;
use net::Endpoint;
use transport::arrived::next_arrival;
use transport::error::{Result, protocol_error};
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback, both_ends, poke};
use transport::{Taken, Transport, socket};

use crate::{Delivery, Event, SnsTransport, ceiling};

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
fn notification(published: &Taken) -> Result<Delivery> {
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
