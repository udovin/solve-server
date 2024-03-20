use std::fmt::Display;

use crate::core::Error;
use crate::db::builder::{Expression, IntoRow};
use crate::db::{FromRow, Row, Value};

use super::types::{now, Instant};

pub trait Object: FromRow + IntoRow + Default + Clone + Send + Sync + 'static {
    type Id: Clone + Into<Expression> + Default + Display + Send + Sync + PartialEq + 'static;

    const ID: &'static str = "id";

    fn id(&self) -> Self::Id;

    fn set_id(&mut self, id: Self::Id);

    fn is_valid(&self) -> bool {
        true
    }

    fn columns() -> Vec<String> {
        IntoRow::into_row(Self::default())
            .into_iter()
            .map(|v| v.0.to_owned())
            .collect()
    }
}

pub trait Event: FromRow + IntoRow + Default + Clone + Send + Sync + 'static {
    type Object: Object;

    const ID: &'static str = "event_id";

    fn id(&self) -> i64;

    fn set_id(&mut self, id: i64);

    fn kind(&self) -> EventKind;

    fn set_kind(&mut self, kind: EventKind);

    fn time(&self) -> Instant;

    fn set_time(&mut self, time: Instant);

    fn account_id(&self) -> Option<i64>;

    fn set_account_id(&mut self, id: Option<i64>);

    fn object(&self) -> &Self::Object;

    fn mut_object(&mut self) -> &mut Self::Object;

    fn into_object(self) -> Self::Object;

    fn set_object(&mut self, object: Self::Object);

    fn columns() -> Vec<String> {
        IntoRow::into_row(Self::default())
            .into_iter()
            .map(|v| v.0.to_owned())
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Value)]
#[repr(i8)]
pub enum EventKind {
    Create = 1,
    Delete = 2,
    Update = 3,
    Unknown(i8),
}

impl ToString for EventKind {  
    fn to_string(&self) -> String {
        match self {
            EventKind::Create => "create",
            EventKind::Delete => "delete",
            EventKind::Update => "update",
            EventKind::Unknown(_) => "unknown",
        }
        .into()
    }
}

#[derive(Clone)]
pub struct BaseEvent<O> {
    id: i64,
    time: Instant,
    account_id: Option<i64>,
    kind: EventKind,
    object: O,
}

impl<O: Object> BaseEvent<O> {
    pub fn create(object: O) -> Self {
        Self {
            kind: EventKind::Create,
            object,
            ..Default::default()
        }
    }

    pub fn update(object: O) -> Self {
        Self {
            kind: EventKind::Update,
            object,
            ..Default::default()
        }
    }

    pub fn delete(id: O::Id) -> Self {
        let mut value = Self {
            kind: EventKind::Delete,
            ..Default::default()
        };
        value.mut_object().set_id(id);
        value
    }
}

impl<O: Object> Default for BaseEvent<O> {
    fn default() -> Self {
        Self {
            id: Default::default(),
            time: now(),
            account_id: Default::default(),
            kind: EventKind::Create,
            object: Default::default(),
        }
    }
}

impl<O: Object> FromRow for BaseEvent<O> {
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok(Self {
            id: row.get(Self::ID).ok_or("unknown field")?.clone().try_into()?,
            time: row.get("event_time").ok_or("unknown field")?.clone().try_into()?,
            account_id: row.get("event_account_id").ok_or("unknown field")?.clone().try_into()?,
            kind: row.get("event_kind").ok_or("unknown field")?.clone().try_into()?,
            object: FromRow::from_row(row)?,
        })
    }
}

impl<O: Object> IntoRow for BaseEvent<O> {
    fn into_row(self) -> Vec<(String, Value)> {
        let mut row = self.object.into_row();
        row.push((Self::ID.into(), self.id.into()));
        row.push(("event_time".into(), self.time.into()));
        row.push(("event_account_id".into(), self.account_id.into()));
        row.push(("event_kind".into(), self.kind.into()));
        row
    }
}

impl<O: Object<Id = I>, I> Event for BaseEvent<O> {
    type Object = O;

    fn id(&self) -> i64 {
        self.id
    }

    fn set_id(&mut self, id: i64) {
        self.id = id
    }

    fn kind(&self) -> EventKind {
        self.kind
    }

    fn set_kind(&mut self, kind: EventKind) {
        self.kind = kind
    }

    fn time(&self) -> Instant {
        self.time
    }

    fn set_time(&mut self, time: Instant) {
        self.time = time
    }

    fn account_id(&self) -> Option<i64> {
        self.account_id
    }

    fn set_account_id(&mut self, id: Option<i64>) {
        self.account_id = id
    }

    fn object(&self) -> &O {
        &self.object
    }

    fn mut_object(&mut self) -> &mut O {
        &mut self.object
    }

    fn into_object(self) -> O {
        self.object
    }

    fn set_object(&mut self, object: O) {
        self.object = object
    }
}
