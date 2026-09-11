//! The HTTP endpoint a subscription delivers to: what SNS sends it, read
//! and written.
//!
//! SNS delivers by `POST`, the kind named in `x-amz-sns-message-type` and
//! the body a JSON document: a `SubscriptionConfirmation` carrying the
//! `SubscribeURL` to fetch, a `Notification` carrying the `Message` and its
//! `MessageId`, an `UnsubscribeConfirmation` carrying nothing a Location
//! wants. Both halves are here — the endpoint reads a delivery, the far end
//! in [`crate::Session`] writes one — so the two cannot drift.
//!
//! SNS also signs each delivery with a certificate it names in
//! `SigningCertURL`; checking that is a fetch of the certificate and an RSA
//! verification, which the estate does not carry yet. The endpoint trusts
//! the network it listens on, which is what the `tls` feature and a private
//! address are for.

use std::net::TcpListener;
use std::time::Duration;

use serde_json::{Value, json};
use transport::error::{Result, TransportError, protocol_error};
use transport::socket;

use http::endpoint;
use http::message::{self, Request, Response};
use http::target::HttpTarget;

/// What SNS delivered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// A subscription to confirm by fetching `subscribe_url`.
    Confirmation {
        topic_arn: String,
        subscribe_url: String,
    },
    /// One message from `topic_arn`.
    Notification {
        topic_arn: String,
        message_id: String,
        message: String,
    },
    /// A kind a Location has nothing to do with — an unsubscribe
    /// confirmation.
    Other(String),
}

/// The delivery `request` carries.
///
/// # Errors
/// Where the request is not a delivery: no kind, a body that is not JSON,
/// or a document missing what its kind carries.
pub fn parse(request: &Request) -> Result<Delivery> {
    let kind = request
        .header_value("x-amz-sns-message-type")
        .ok_or_else(|| protocol_error("a request that is not an SNS delivery"))?
        .to_string();
    let body: Value = serde_json::from_slice(&request.body)
        .map_err(|e| protocol_error(format!("a delivery that is not JSON: {e}")))?;
    let field = |name: &str| {
        body[name]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| protocol_error(format!("a {kind} with no {name}")))
    };
    match kind.as_str() {
        "SubscriptionConfirmation" => Ok(Delivery::Confirmation {
            topic_arn: field("TopicArn")?,
            subscribe_url: field("SubscribeURL")?,
        }),
        "Notification" => Ok(Delivery::Notification {
            topic_arn: field("TopicArn")?,
            message_id: field("MessageId")?,
            message: field("Message")?,
        }),
        _ => Ok(Delivery::Other(kind.clone())),
    }
}

/// The far end's side: the request SNS makes to deliver `delivery` to an
/// endpoint at `path`.
#[must_use]
pub fn deliver(path: &str, delivery: &Delivery) -> Request {
    let (kind, body) = match delivery {
        Delivery::Confirmation {
            topic_arn,
            subscribe_url,
        } => (
            "SubscriptionConfirmation",
            json!({
                "Type": "SubscriptionConfirmation",
                "TopicArn": topic_arn,
                "SubscribeURL": subscribe_url,
                "Token": "xmip",
            }),
        ),
        Delivery::Notification {
            topic_arn,
            message_id,
            message,
        } => (
            "Notification",
            json!({
                "Type": "Notification",
                "TopicArn": topic_arn,
                "MessageId": message_id,
                "Message": message,
            }),
        ),
        Delivery::Other(kind) => (kind.as_str(), json!({ "Type": kind })),
    };
    Request::new("POST", path)
        .header("x-amz-sns-message-type", kind)
        .header("Content-Type", "text/plain; charset=UTF-8")
        .body(body.to_string().as_bytes())
}

/// Push `delivery` to the endpoint at `endpoint_url`, as SNS does, and
/// expect it taken.
///
/// # Errors
/// Where the URL is not HTTP, the endpoint could not be reached, or it did
/// not answer 2xx — SNS retries that, so it is retryable.
pub fn push(endpoint_url: &str, delivery: &Delivery, timeout: Option<Duration>) -> Result<()> {
    let target = HttpTarget::parse(endpoint_url)?;
    let scheme = if target.secure { "https" } else { "http" };
    let request = deliver(target.path, delivery).header("Host", target.authority);
    let stream = endpoint::connect(&format!("{scheme}://{}", target.authority), timeout)?;
    let response = message::exchange(stream, &request)?;
    if (200..300).contains(&response.status) {
        Ok(())
    } else {
        Err(TransportError::retryable(format!(
            "the endpoint answered {}",
            response.status
        )))
    }
}

/// Accept one delivery on `listener`, answer it, and say what it was.
///
/// # Errors
/// Where the connection could not be accepted, broke, or did not carry a
/// delivery — which is answered 400 before the error is returned.
pub fn accept_one(listener: &TcpListener, timeout: Option<Duration>) -> Result<Delivery> {
    let (stream, _) = socket::accept_tcp(listener, timeout)?;
    let (mut reader, mut writer) = socket::split(stream)?;
    let request = message::read_request(&mut reader)?
        .ok_or_else(|| protocol_error("a connection that sent no request"))?;
    let delivery = parse(&request);
    let status = if delivery.is_ok() { 200 } else { 400 };
    message::write_response(&mut writer, &Response::new(status))?;
    delivery
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC: &str = "arn:aws:sns:eu-north-1:123456789012:orders";

    fn notification(message: &str) -> Delivery {
        Delivery::Notification {
            topic_arn: TOPIC.to_string(),
            message_id: "1-xmip".to_string(),
            message: message.to_string(),
        }
    }

    #[test]
    fn a_delivery_is_written_as_sns_writes_it_and_reads_back() {
        let sent = deliver("/sns", &notification("r\u{e4}k <&> \"b\"\r\n"));
        assert_eq!(
            sent.header_value("x-amz-sns-message-type"),
            Some("Notification")
        );
        assert!(sent.body.starts_with(b"{"));
        assert_eq!(
            parse(&sent).expect("read"),
            notification("r\u{e4}k <&> \"b\"\r\n")
        );
        let confirmation = Delivery::Confirmation {
            topic_arn: TOPIC.to_string(),
            subscribe_url: "http://sns.local/?Action=ConfirmSubscription".to_string(),
        };
        assert_eq!(
            parse(&deliver("/", &confirmation)).expect("read"),
            confirmation
        );
        let other = Delivery::Other("UnsubscribeConfirmation".to_string());
        assert_eq!(parse(&deliver("/", &other)).expect("read"), other);
    }

    #[test]
    fn what_is_not_a_delivery_is_refused_with_the_reason() {
        let plain = Request::new("POST", "/").body(b"{}");
        let refused = parse(&plain).expect_err("no kind");
        assert!(refused.message.contains("not an SNS delivery"));
        assert!(!refused.retryable);
        let broken = Request::new("POST", "/")
            .header("x-amz-sns-message-type", "Notification")
            .body(b"not json");
        assert!(
            parse(&broken)
                .expect_err("not JSON")
                .message
                .contains("JSON")
        );
        let bare = Request::new("POST", "/")
            .header("x-amz-sns-message-type", "Notification")
            .body(br#"{"TopicArn":"t"}"#);
        assert!(
            parse(&bare)
                .expect_err("no id")
                .message
                .contains("MessageId")
        );
    }

    #[test]
    fn a_pushed_delivery_is_accepted_on_loopback_and_a_bad_one_answered_400() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let timeout = Some(Duration::from_secs(2));
        let endpoint = std::thread::spawn(move || {
            let first = accept_one(&listener, timeout);
            let second = accept_one(&listener, timeout);
            (first, second)
        });
        push(
            &format!("http://{address}/sns"),
            &notification("a"),
            timeout,
        )
        .expect("pushed");
        let stream = endpoint::connect(&format!("http://{address}"), timeout).expect("connect");
        let answer =
            message::exchange(stream, &Request::new("POST", "/").body(b"x")).expect("answered");
        assert_eq!(answer.status, 400);
        let (first, second) = endpoint.join().expect("thread");
        assert_eq!(first.expect("a delivery"), notification("a"));
        assert!(second.is_err(), "not a delivery");
        assert!(push("sns.local/x", &notification("a"), timeout).is_err());
    }
}
