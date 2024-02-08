use crate::core::Error;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    BigInt(i64),
    Double(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ValueKind {
    Null,
    Bool,
    BigInt,
    Double,
    Text,
    Blob,
}

impl Value {
    pub fn kind(&self) -> ValueKind {
        match *self {
            Value::Null => ValueKind::Null,
            Value::Bool(_) => ValueKind::Bool,
            Value::BigInt(_) => ValueKind::BigInt,
            Value::Double(_) => ValueKind::Double,
            Value::Text(_) => ValueKind::Text,
            Value::Blob(_) => ValueKind::Blob,
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl TryFrom<Value> for bool {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Error> {
        match value {
            Value::Bool(v) => Ok(v),
            _ => Err("cannot parse bool".into()),
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::BigInt(value)
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Error> {
        match value {
            Value::BigInt(v) => Ok(v),
            _ => Err("cannot parse i64".into()),
        }
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

impl TryFrom<Value> for f64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Error> {
        match value {
            Value::Double(v) => Ok(v),
            _ => Err("cannot parse f64".into()),
        }
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl TryFrom<Value> for String {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Error> {
        match value {
            Value::Text(v) => Ok(v),
            _ => Err("cannot parse String".into()),
        }
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Error> {
        match value {
            Value::Blob(v) => Ok(v),
            _ => Err("cannot parse Vec<u8>".into()),
        }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

value_option_impl!(bool);
value_option_impl!(i64);
value_option_impl!(f64);
value_option_impl!(String);
value_option_impl!(Vec<u8>);
value_option_impl!(&str);
value_option_impl!(&[u8]);

macro_rules! value_option_impl {
    (&$($tt:tt)+) => {
        impl From<Option<&$($tt)+>> for Value {
            fn from(value: Option<&$($tt)+>) -> Self {
                match value {
                    Some(v) => v.to_owned().into(),
                    None => Value::Null,
                }
            }
        }
    };

    ($($tt:tt)+) => {
        impl TryFrom<Value> for Option<$($tt)+> {
            type Error = Error;

            fn try_from(value: Value) -> Result<Self, Error> {
                Ok(match value {
                    Value::Null => None,
                    _ => Some(value.try_into()?),
                })
            }
        }

        impl From<Option<$($tt)+>> for Value {
            fn from(value: Option<$($tt)+>) -> Self {
                match value {
                    Some(v) => v.into(),
                    None => Value::Null,
                }
            }
        }
    };
}

pub(crate) use value_option_impl;
