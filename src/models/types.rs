use chrono::{DateTime, Utc};

use crate::core::Error;
use crate::db::{value_option_impl, Value};

pub type Instant = DateTime<Utc>;

pub fn now() -> Instant {
    Utc::now()
}

impl TryFrom<Value> for Instant {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Error> {
        DateTime::from_timestamp(value.try_into()?, 0).ok_or("cannot parse timestamp".into())
    }
}

impl From<Instant> for Value {
    fn from(value: Instant) -> Self {
        value.timestamp().into()
    }
}

pub type JSON = serde_json::Value;

impl TryFrom<Value> for JSON {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Error> {
        Ok(match value {
            Value::Text(v) => serde_json::from_str(&v)?,
            Value::Blob(v) => serde_json::from_slice(&v)?,
            _ => return Err("cannot parse json".into()),
        })
    }
}

impl From<JSON> for Value {
    fn from(value: JSON) -> Self {
        value.to_string().into()
    }
}

value_option_impl!(Instant);
value_option_impl!(JSON);
