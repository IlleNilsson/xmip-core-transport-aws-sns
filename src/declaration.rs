//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48). `access_key_id` is the AWS crate's, taken by that crate's
//! own `declaration` feature.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the SNS endpoint,
/// `https://sns.<region>.amazonaws.com`: published to on send, confirmed
/// through on receive. The access key ID is `access_key_id`; its secret
/// access key is the Location's `secret`, never a setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "region",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The AWS region requests are signed for, eu-north-1.",
            applies: Applies::Both,
        },
        aws::access_key::ACCESS_KEY_ID,
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
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long an endpoint that stops answering is waited on; unbounded \
             when left out.",
        ),
    ],
};

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["aws-sns"];
