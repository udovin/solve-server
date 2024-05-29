use solve_db::Transaction;

use crate::core::Error;
use crate::db::builder::{Predicate, Select};

use super::{Event, Object};

pub struct Context<'a, 'b> {
    pub tx: Option<&'a mut Transaction<'b>>,
    pub account_id: Option<i64>,
}

impl<'a, 'b> Context<'a, 'b> {
    pub fn new() -> Self {
        Self {
            tx: Default::default(),
            account_id: Default::default(),
        }
    }

    pub fn with_tx<'c, 'd>(self, tx: &'c mut Transaction<'d>) -> Context<'c, 'd> {
        Context::<'c, 'd> {
            tx: Some(tx),
            ..self
        }
    }
}

impl<'a, 'b> Default for Context<'a, 'b> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
pub trait AsyncIter<'a>: Send {
    type Item;

    async fn next(&mut self) -> Option<Result<Self::Item, Error>>;
}

#[async_trait::async_trait]
pub trait ObjectStore: Send {
    type Id;
    type Object: Object<Id = Self::Id>;
    type Event: Event<Object = Self::Object>;

    type FindIter<'a>: AsyncIter<'a, Item = Self::Object>
    where
        Self: 'a;

    async fn find<'a>(
        &'a self,
        ctx: Context<'a, '_>,
        select: Select,
    ) -> Result<Self::FindIter<'a>, Error>;

    async fn get<'a>(
        &'a self,
        ctx: Context<'a, '_>,
        id: Self::Id,
    ) -> Result<Option<Self::Object>, Error>;

    async fn create(
        &self,
        ctx: Context<'_, '_>,
        object: Self::Object,
    ) -> Result<Self::Event, Error>;

    async fn update(
        &self,
        ctx: Context<'_, '_>,
        object: Self::Object,
    ) -> Result<Self::Event, Error>;

    async fn update_where(
        &self,
        ctx: Context<'_, '_>,
        object: Self::Object,
        predicate: Predicate,
    ) -> Result<Self::Event, Error>;

    async fn delete(&self, ctx: Context<'_, '_>, id: Self::Id) -> Result<Self::Event, Error>;
}
