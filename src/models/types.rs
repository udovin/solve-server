use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::core::Error;
use solve_db::{FromValue, IntoValue, Value};

#[derive(Copy, Clone, Default, Debug, PartialEq, PartialOrd)]
pub struct Instant(DateTime<Utc>);

impl Instant {
    pub fn now() -> Self {
        Utc::now().into()
    }
}

impl FromValue for Instant {
    fn from_value(value: &Value) -> Result<Self, Error> {
        let dt = DateTime::from_timestamp(value.parse()?, 0);
        Ok(Self(dt.ok_or("Cannot parse Instant")?))
    }
}

impl IntoValue for Instant {
    fn into_value(self) -> Value {
        self.0.timestamp().into_value()
    }
}

impl From<DateTime<Utc>> for Instant {
    fn from(value: DateTime<Utc>) -> Self {
        Self(value)
    }
}

impl From<Instant> for DateTime<Utc> {
    fn from(value: Instant) -> Self {
        value.0
    }
}

impl std::ops::Add<Duration> for Instant {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self {
        Self(self.0 + rhs)
    }
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct JSON(serde_json::Value);

impl FromValue for JSON {
    fn from_value(value: &Value) -> Result<Self, Error> {
        Ok(Self(match value {
            Value::Text(v) => serde_json::from_str(v)?,
            Value::Blob(v) => serde_json::from_slice(v)?,
            _ => return Err("Cannot parse JSON".into()),
        }))
    }
}

impl IntoValue for JSON {
    fn into_value(self) -> Value {
        self.0.to_string().into_value()
    }
}

impl From<serde_json::Value> for JSON {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

impl From<JSON> for serde_json::Value {
    fn from(value: JSON) -> Self {
        value.0
    }
}
