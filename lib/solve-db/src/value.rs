use crate::Error;

#[derive(Clone, Default, Debug, PartialEq)]
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
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

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

    pub fn from<T: IntoValue>(value: T) -> Self {
        IntoValue::into_value(value)
    }

    pub fn parse<T: FromValue>(&self) -> Result<T, Error> {
        FromValue::from_value(self)
    }
}

pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Result<Self, Error>;
}

pub trait IntoValue: Sized {
    fn into_value(self) -> Value;
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Result<Self, Error> {
        Ok(value.clone())
    }
}

impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self, Error> {
        match value {
            Value::Null => Ok(None),
            v => Ok(Some(FromValue::from_value(v)?)),
        }
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    fn into_value(self) -> Value {
        match self {
            None => Value::Null,
            Some(v) => v.into_value(),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self, Error> {
        match value {
            Value::Bool(v) => Ok(*v),
            _ => Err("cannot parse bool".into()),
        }
    }
}

impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::Bool(self)
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self, Error> {
        match value {
            Value::BigInt(v) => Ok(*v),
            _ => Err("cannot parse bool".into()),
        }
    }
}

impl IntoValue for i64 {
    fn into_value(self) -> Value {
        Value::BigInt(self)
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self, Error> {
        match value {
            Value::Double(v) => Ok(*v),
            _ => Err("cannot parse bool".into()),
        }
    }
}

impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value::Double(self)
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self, Error> {
        match value {
            Value::Text(v) => Ok(v.clone()),
            _ => Err("cannot parse bool".into()),
        }
    }
}

impl IntoValue for String {
    fn into_value(self) -> Value {
        Value::Text(self)
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self, Error> {
        match value {
            Value::Blob(v) => Ok(v.clone()),
            _ => Err("cannot parse bool".into()),
        }
    }
}

impl IntoValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::Blob(self)
    }
}

impl IntoValue for &str {
    fn into_value(self) -> Value {
        Value::Text(self.to_owned())
    }
}

impl IntoValue for &[u8] {
    fn into_value(self) -> Value {
        Value::Blob(self.to_owned())
    }
}
