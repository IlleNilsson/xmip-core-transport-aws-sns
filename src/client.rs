//! Xmip's side: the two calls a Location makes — publish to a topic, and
//! confirm the subscription SNS offers — each one request over one
//! connection.
//!
//! The endpoint is the service, not the topic: `https://sns.eu-north-1.
//! amazonaws.com` in the cloud, `http://127.0.0.1:9911` for a stand-in. A
//! topic is named in the `TopicArn` parameter, and a request is a signed
//! `POST` to `/`. A confirmation is the one unsigned call: SNS hands the
//! endpoint a `SubscribeURL` carrying a token, and fetching it is the proof.

use std::time::Duration;

use transport::error::Result;
use transport::xml::first;

use http::endpoint;
use http::message::{self, Request, Response};
use http::query::{self, text};
use http::sigv4::{self, Signer};
use http::target::HttpTarget;

/// The Query API version every request names.
pub const VERSION: &str = "2010-03-31";

pub struct Client {
    endpoint: String,
    host: String,
    signer: Signer,
    timeout: Option<Duration>,
}

impl Client {
    /// Speak to the SNS endpoint at `endpoint` — `http://host:port` or
    /// `https://host:port` — in `region`, signing as `access_key`.
    ///
    /// # Errors
    /// Where `endpoint` is not an HTTP URL.
    pub fn new(endpoint: &str, region: &str, access_key: &str, secret_key: &str) -> Result<Self> {
        Ok(Self {
            endpoint: endpoint.to_string(),
            host: endpoint::authority(endpoint)?,
            signer: Signer::new("sns", region, access_key, secret_key),
            timeout: None,
        })
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Publish `bytes` as one message to the topic at `topic_arn`, and
    /// learn its id.
    ///
    /// # Errors
    /// Where the bytes are not a message body, or the endpoint refused or
    /// could not be reached.
    pub fn publish(&self, topic_arn: &str, bytes: &[u8]) -> Result<String> {
        let body = text(bytes)?;
        let parameters = [
            ("Action", "Publish"),
            ("Version", VERSION),
            ("TopicArn", topic_arn),
            ("Message", body),
        ];
        let request = query::request("/", &parameters).header("Host", &self.host);
        let signed = self.signer.sign(request, &sigv4::now());
        let answer = self.call(&self.endpoint, &signed)?;
        Ok(first(&answer.text(), "MessageId").unwrap_or_default())
    }

    /// Confirm a subscription by fetching the `SubscribeURL` SNS delivered,
    /// as SNS asks: one unsigned `GET`, the token in its query. The
    /// subscription ARN comes back.
    ///
    /// # Errors
    /// Where the URL is not an HTTP URL, or the endpoint refused or could
    /// not be reached.
    pub fn confirm(&self, subscribe_url: &str) -> Result<String> {
        let target = HttpTarget::parse(subscribe_url)?;
        let scheme = if target.secure { "https" } else { "http" };
        let endpoint = format!("{scheme}://{}", target.authority);
        let request = Request::new("GET", target.path).header("Host", target.authority);
        let answer = self.call(&endpoint, &request)?;
        Ok(first(&answer.text(), "SubscriptionArn").unwrap_or_default())
    }

    fn call(&self, endpoint: &str, request: &Request) -> Result<Response> {
        let stream = endpoint::connect(endpoint, self.timeout)?;
        query::judge("SNS", message::exchange(stream, request)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Event, Session};
    use transport::socket;

    const TOPIC: &str = "arn:aws:sns:eu-north-1:123456789012:orders";

    #[test]
    fn the_two_calls_reach_a_session_and_come_back_shaped_as_sns_shapes_them() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session = Session::new("eu-north-1", "AKID", "secret")
                .timing_out_after(Duration::from_secs(2));
            let events: Vec<Event> = (0..4)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        let endpoint = format!("http://{address}");
        let client = Client::new(&endpoint, "eu-north-1", "AKID", "secret")
            .expect("endpoint")
            .timing_out_after(Duration::from_secs(2));
        let id = client.publish(TOPIC, b"UNA:+.? '").expect("published");
        assert!(!id.is_empty(), "an id came back");
        client
            .publish(TOPIC, "r\u{e4}k <&> \"b\"".as_bytes())
            .expect("published");
        let arn = client
            .confirm(&Session::subscribe_url(&endpoint, TOPIC))
            .expect("confirmed");
        assert_eq!(arn, format!("{TOPIC}:xmip-subscription"));
        let wrong = format!("{endpoint}/?Action=ConfirmSubscription&Token=other");
        let refused = client.confirm(&wrong).expect_err("a token not handed out");
        assert!(
            refused.message.contains("400 InvalidParameter"),
            "{refused}"
        );
        assert!(!refused.retryable);
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.messages().len(), 2);
        assert_eq!(
            session
                .messages()
                .get(&format!("{TOPIC}#{id}"))
                .map(String::as_str),
            Some("UNA:+.? '")
        );
        assert!(matches!(&events[2], Event::Confirmed { topic_arn, .. } if topic_arn == TOPIC));
        assert_eq!(events[3], Event::Refused("InvalidParameter".to_string()));
    }

    #[test]
    fn what_is_not_a_message_body_or_an_endpoint_is_refused_before_a_wire_is_touched() {
        let client = Client::new("http://127.0.0.1:1", "r", "a", "s").expect("endpoint");
        let refused = client.publish(TOPIC, b"\x00").expect_err("not text");
        assert!(!refused.retryable);
        assert!(refused.message.contains("U+0000"));
        assert!(Client::new("sns.local", "r", "a", "s").is_err());
        assert!(
            !client
                .confirm("sns.local/?x")
                .expect_err("no scheme")
                .retryable
        );
        assert!(client.publish(TOPIC, b"x").expect_err("nobody").retryable);
    }
}
