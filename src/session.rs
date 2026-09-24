//! The far end: enough of SNS to answer one Location, and what a test or
//! the playground puts on loopback.
//!
//! Not SNS. One session holds the messages published to every topic it is
//! asked about in memory, verifies every `Publish` against one credential,
//! and answers in the shapes SNS answers — the message id, the subscription
//! ARN, the error with its code. A `ConfirmSubscription` arrives unsigned
//! with its token in the query, as the `SubscribeURL` SNS hands out carries
//! it. Delivering to an endpoint is [`Session::deliver`], which a test
//! calls where SNS would push on its own.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::time::Duration;

use transport::Arrived;
use transport::error::Result;

use crate::client::VERSION;
use crate::subscription::{self, Delivery};
use aws::query::{self, parameter};
use aws::sigv4::Signer;
use http::message::{Request, Response};
use http::percent::encode;
use http::server;

/// What the client did, as [`Session::serve_one`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client published a message; here is the Stream, its origin the
    /// topic ARN and the id it was given.
    Published(Arrived),
    /// The client confirmed a subscription to `topic_arn`.
    Confirmed {
        topic_arn: String,
        subscription_arn: String,
    },
    /// The client was answered with this error code.
    Refused(String),
}

pub struct Session {
    signer: Signer,
    topics: BTreeMap<String, Vec<(String, String)>>,
    next: usize,
    timeout: Option<Duration>,
}

impl Session {
    /// Answer requests signed in `region` as `access_key` with `secret_key`.
    #[must_use]
    pub fn new(region: &str, access_key: &str, secret_key: &str) -> Self {
        Self {
            signer: Signer::new("sns", region, access_key, secret_key),
            topics: BTreeMap::new(),
            next: 1,
            timeout: None,
        }
    }

    /// Give up on a client that stops mid-request after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Every message published so far, keyed `topic_arn#id`.
    #[must_use]
    pub fn messages(&self) -> BTreeMap<String, String> {
        self.topics
            .iter()
            .flat_map(|(topic, held)| {
                held.iter()
                    .map(move |(id, message)| (origin(topic, id), message.clone()))
            })
            .collect()
    }

    /// The `SubscribeURL` this session hands an endpoint for `topic_arn`,
    /// at a session listening at `endpoint` — `http://host:port`.
    #[must_use]
    pub fn subscribe_url(endpoint: &str, topic_arn: &str) -> String {
        format!(
            "{endpoint}/?Action=ConfirmSubscription&TopicArn={}&Token=xmip",
            encode(topic_arn, false)
        )
    }

    /// Deliver to the endpoint at `endpoint_url`, as SNS pushes to a
    /// subscription.
    ///
    /// # Errors
    /// As [`subscription::push`].
    pub fn deliver(&self, endpoint_url: &str, delivery: &Delivery) -> Result<()> {
        subscription::push(endpoint_url, delivery, self.timeout)
    }

    /// Accept one connection on `listener`, answer its one request, and say
    /// what it was.
    ///
    /// # Errors
    /// Where the connection could not be accepted, broke, or sent nothing.
    pub fn serve_one(&mut self, listener: &TcpListener) -> Result<Event> {
        server::serve_one(listener, self.timeout, |request| self.answer(request))
    }

    fn answer(&mut self, request: &Request) -> (Event, Response) {
        if request.query_value("Action") == Some("ConfirmSubscription") {
            return Self::confirm(request);
        }
        if let Err(failure) = self.signer.verify(request) {
            return refused(403, "SignatureDoesNotMatch", &failure.message);
        }
        let parameters = query::parameters(request);
        if parameter(&parameters, "Version") != Some(VERSION) {
            return refused(
                400,
                "InvalidParameterValue",
                "a version this session does not speak",
            );
        }
        match parameter(&parameters, "Action") {
            Some("Publish") => self.publish(&parameters),
            _ => refused(400, "InvalidAction", "not a call this session answers"),
        }
    }

    fn confirm(request: &Request) -> (Event, Response) {
        let topic_arn = request.query_value("TopicArn").unwrap_or_default();
        if request.query_value("Token") != Some("xmip") || topic_arn.is_empty() {
            return refused(
                400,
                "InvalidParameter",
                "a token this session did not hand out",
            );
        }
        let subscription_arn = format!("{topic_arn}:xmip-subscription");
        let xml = format!(
            "<ConfirmSubscriptionResponse><ConfirmSubscriptionResult>\
             <SubscriptionArn>{subscription_arn}</SubscriptionArn>\
             </ConfirmSubscriptionResult></ConfirmSubscriptionResponse>"
        );
        (
            Event::Confirmed {
                topic_arn: topic_arn.to_string(),
                subscription_arn,
            },
            answer(&xml),
        )
    }

    fn publish(&mut self, parameters: &[(String, String)]) -> (Event, Response) {
        let Some(topic) = parameter(parameters, "TopicArn") else {
            return refused(400, "MissingParameter", "a request naming no TopicArn");
        };
        let Some(body) = parameter(parameters, "Message") else {
            return refused(400, "MissingParameter", "a request with no Message");
        };
        if let Some(why) = query::refusal(body.as_bytes()) {
            return refused(400, "InvalidParameter", &why);
        }
        let id = format!("{:08x}-xmip", self.next);
        self.next += 1;
        self.topics
            .entry(topic.to_string())
            .or_default()
            .push((id.clone(), body.to_string()));
        let xml = format!(
            "<PublishResponse><PublishResult><MessageId>{id}</MessageId>\
             </PublishResult></PublishResponse>"
        );
        (
            Event::Published(Arrived::new(origin(topic, &id), body.as_bytes())),
            answer(&xml),
        )
    }
}

/// The message `id` on `topic_arn`, as an origin says it.
#[must_use]
pub fn origin(topic_arn: &str, id: &str) -> String {
    format!("{topic_arn}#{id}")
}

fn answer(xml: &str) -> Response {
    Response::new(200)
        .header("Content-Type", "text/xml")
        .body(format!("<?xml version=\"1.0\"?>{xml}").as_bytes())
}

fn refused(status: u16, code: &str, message: &str) -> (Event, Response) {
    (
        Event::Refused(code.to_string()),
        query::error(status, code, message),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const AT: &str = "20260910T000000Z";
    const TOPIC: &str = "arn:aws:sns:eu-north-1:123456789012:orders";

    fn signed(parameters: &[(&str, &str)]) -> Request {
        let mut all = vec![("Version", VERSION), ("TopicArn", TOPIC)];
        all.extend_from_slice(parameters);
        Signer::new("sns", "r", "AKID", "secret")
            .sign(query::request("/", &all).header("Host", "sns.local"), AT)
    }

    #[test]
    fn a_session_answers_in_snss_shapes_and_refuses_a_bad_signature() {
        let mut session = Session::new("r", "AKID", "secret");
        let (event, response) =
            session.answer(&signed(&[("Action", "Publish"), ("Message", "a<b")]));
        assert_eq!(response.status, 200);
        assert!(
            response
                .text()
                .contains("<MessageId>00000001-xmip</MessageId>")
        );
        let origin = format!("{TOPIC}#00000001-xmip");
        assert_eq!(
            event,
            Event::Published(Arrived::new(origin.clone(), b"a<b".to_vec()))
        );
        assert_eq!(
            session.messages().get(&origin).map(String::as_str),
            Some("a<b")
        );
        let empty = signed(&[("Action", "Publish"), ("Message", "")]);
        let (event, _) = session.answer(&empty);
        assert_eq!(event, Event::Refused("InvalidParameter".to_string()));
        let (_, response) = session.answer(&signed(&[("Action", "Subscribe")]));
        assert_eq!(response.status, 400);
        let other = Signer::new("sns", "r", "AKID", "wrong")
            .sign(query::request("/", &[]).header("Host", "sns.local"), AT);
        let (event, response) = session.answer(&other);
        assert_eq!(event, Event::Refused("SignatureDoesNotMatch".to_string()));
        assert_eq!(response.status, 403);
    }

    #[test]
    fn a_confirmation_arrives_unsigned_with_its_token_and_is_answered_with_an_arn() {
        let mut session = Session::new("r", "AKID", "secret");
        let url = Session::subscribe_url("http://sns.local", TOPIC);
        let confirm = Request::new("GET", "/")
            .query("Action", "ConfirmSubscription")
            .query("TopicArn", TOPIC)
            .query("Token", "xmip");
        let (event, response) = session.answer(&confirm);
        assert!(url.contains("Token=xmip"));
        assert_eq!(response.status, 200);
        assert!(response.text().contains("<SubscriptionArn>"));
        assert_eq!(
            event,
            Event::Confirmed {
                topic_arn: TOPIC.to_string(),
                subscription_arn: format!("{TOPIC}:xmip-subscription"),
            }
        );
        let stale = Request::new("GET", "/")
            .query("Action", "ConfirmSubscription")
            .query("TopicArn", TOPIC)
            .query("Token", "other");
        let (event, response) = session.answer(&stale);
        assert_eq!(event, Event::Refused("InvalidParameter".to_string()));
        assert_eq!(response.status, 400);
    }
}
