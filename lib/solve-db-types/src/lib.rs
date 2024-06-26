use chrono::{DateTime, Utc};

use solve_db::{Error, FromValue, IntoValue, Value};

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
        Ok(Self(dt.ok_or("cannot parse timestamp")?))
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

impl<T> std::ops::Add<T> for Instant
where
    DateTime<Utc>: std::ops::Add<T, Output = DateTime<Utc>>,
{
    type Output = Self;

    fn add(self, rhs: T) -> Self {
        Self(self.0 + rhs)
    }
}

impl<T> std::ops::Sub<T> for Instant
where
    DateTime<Utc>: std::ops::Sub<T, Output = DateTime<Utc>>,
{
    type Output = Self;

    fn sub(self, rhs: T) -> Self {
        Self(self.0 - rhs)
    }
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct JSON(serde_json::Value);

impl FromValue for JSON {
    fn from_value(value: &Value) -> Result<Self, Error> {
        Ok(Self(match value {
            Value::Text(v) => serde_json::from_str(v)?,
            Value::Blob(v) => serde_json::from_slice(v)?,
            _ => return Err("cannot parse json".into()),
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
