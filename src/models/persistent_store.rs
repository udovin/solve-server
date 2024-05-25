use std::{marker::PhantomData, sync::Arc};

use crate::core::Error;
use crate::db::builder::{column, Delete, Insert, Predicate, Select, Update};

use super::{AsyncIter, BaseEvent, Context, Event, EventKind, Object, ObjectStore};

pub struct PersistentStore<O: Object> {
    db: Arc<Database>,
    table: String,
    event_table: String,
    columns: Vec<String>,
    event_columns: Vec<String>,
    _phantom: PhantomData<O>,
}

impl<O: Object> PersistentStore<O> {
    pub fn new<T: Into<String>, ET: Into<String>>(
        db: Arc<Database>,
        table: T,
        event_table: ET,
    ) -> Self {
        let columns = O::columns();
        let event_columns = BaseEvent::<O>::columns();
        Self {
            db,
            columns,
            event_columns,
            table: table.into(),
            event_table: event_table.into(),
            _phantom: PhantomData,
        }
    }

    pub fn db(&self) -> &Database {
        self.db.as_ref()
    }

    async fn create_object(&self, tx: &mut impl Executor<'_>, object: O) -> Result<O, Error> {
        assert!(object.is_valid());
        let row: Vec<_> = object
            .into_row()
            .into_iter()
            .filter(|v| v.0 != O::ID)
            .collect();
        let query = Insert::new()
            .with_table(&self.table)
            .with_row(row)
            .with_returning(self.columns.clone());
        let mut rows = tx.query(query).await?;
        let row = match rows.next().await {
            Some(Ok(v)) => v,
            Some(Err(v)) => return Err(v),
            None => return Err("empty query result".into()),
        };
        FromRow::from_row(&row)
    }

    async fn update_object(
        &self,
        tx: &mut impl Executor<'_>,
        object: O,
        predicate: Option<Predicate>,
    ) -> Result<O, Error> {
        assert!(object.is_valid());
        let id = object.id();
        let row: Vec<_> = object
            .into_row()
            .into_iter()
            .filter(|v| v.0 != O::ID)
            .collect();
        let predicate = match predicate {
            Some(v) => column(O::ID).equal(id).and(v),
            None => column(O::ID).equal(id),
        };
        let query = Update::new()
            .with_table(&self.table)
            .with_row(row)
            .with_where(predicate)
            .with_returning(self.columns.clone());
        let mut rows = tx.query(query).await?;
        let row = match rows.next().await {
            Some(Ok(v)) => v,
            Some(Err(v)) => return Err(v),
            None => return Err("empty query result".into()),
        };
        FromRow::from_row(&row)
    }

    async fn delete_object(&self, tx: &mut impl Executor<'_>, id: O::Id) -> Result<(), Error> {
        let query = Delete::new()
            .with_table(&self.table)
            .with_where(column(O::ID).equal(id.clone()));
        let status = tx.execute(query).await?;
        match status.rows_affected() {
            Some(1) => Ok(()),
            _ => Err(format!("cannot delete object with id {}", id).into()),
        }
    }

    async fn create_event(
        &self,
        tx: &mut impl Executor<'_>,
        event: BaseEvent<O>,
    ) -> Result<BaseEvent<O>, Error> {
        assert!(!matches!(event.kind(), EventKind::Unknown(_)));
        let row: Vec<_> = event
            .into_row()
            .into_iter()
            .filter(|v| v.0 != BaseEvent::<O>::ID)
            .collect();
        let query = Insert::new()
            .with_table(&self.event_table)
            .with_row(row)
            .with_returning(self.event_columns.clone());
        let mut rows = tx.query(query).await?;
        let row = match rows.next().await {
            Some(Ok(v)) => v,
            Some(Err(v)) => return Err(v),
            None => return Err("empty query result".into()),
        };
        FromRow::from_row(&row)
    }
}

pub fn write_tx_options() -> TransactionOptions {
    TransactionOptions {
        isolation_level: IsolationLevel::RepeatableRead,
        read_only: false,
    }
}

pub struct RowsIter<'a, T> {
    rows: Rows<'a>,
    _phantom: PhantomData<T>,
}

#[async_trait::async_trait]
impl<'a, T: Send + FromRow> AsyncIter<'a> for RowsIter<'a, T> {
    type Item = T;

    async fn next(&mut self) -> Option<Result<Self::Item, Error>> {
        match self.rows.next().await {
            Some(Ok(v)) => Some(FromRow::from_row(&v)),
            Some(Err(v)) => Some(Err(v)),
            None => None,
        }
    }
}

#[async_trait::async_trait]
impl<O: Object> ObjectStore for PersistentStore<O> {
    type Id = O::Id;
    type Object = O;
    type Event = BaseEvent<O>;
    type FindIter<'a> = RowsIter<'a, O>;

    async fn find<'a>(
        &'a self,
        mut ctx: Context<'a, '_>,
        select: Select,
    ) -> Result<Self::FindIter<'a>, Error> {
        let query = select
            .with_table(&self.table)
            .with_columns(self.columns.clone())
            .with_order_by(vec![O::ID.to_owned()]);
        let rows = if let Some(tx) = ctx.tx.take() {
            tx.query(query).await?
        } else {
            self.db.query(query).await?
        };
        Ok(RowsIter {
            rows,
            _phantom: PhantomData,
        })
    }

    async fn create(&self, mut ctx: Context<'_, '_>, object: O) -> Result<Self::Event, Error> {
        if let Some(tx) = ctx.tx.take() {
            let object = self.create_object(tx, object).await?;
            let event = self.create_event(tx, BaseEvent::create(object)).await?;
            return Ok(event);
        }
        let mut tx = self.db.transaction(write_tx_options()).await?;
        let event = self.create(ctx.with_tx(&mut tx), object).await?;
        tx.commit().await?;
        Ok(event)
    }

    async fn update(&self, mut ctx: Context<'_, '_>, object: O) -> Result<Self::Event, Error> {
        if let Some(tx) = ctx.tx.take() {
            let object = self.update_object(tx, object, None).await?;
            let event = self.create_event(tx, BaseEvent::update(object)).await?;
            return Ok(event);
        }
        let mut tx = self.db.transaction(write_tx_options()).await?;
        let event = self.update(ctx.with_tx(&mut tx), object).await?;
        tx.commit().await?;
        Ok(event)
    }

    async fn update_where(
        &self,
        mut ctx: Context<'_, '_>,
        object: Self::Object,
        predicate: Predicate,
    ) -> Result<Self::Event, Error> {
        if let Some(tx) = ctx.tx.take() {
            let object = self.update_object(tx, object, Some(predicate)).await?;
            let event = self.create_event(tx, BaseEvent::update(object)).await?;
            return Ok(event);
        }
        let mut tx = self.db.transaction(write_tx_options()).await?;
        let event = self
            .update_where(ctx.with_tx(&mut tx), object, predicate)
            .await?;
        tx.commit().await?;
        Ok(event)
    }

    async fn delete(&self, mut ctx: Context<'_, '_>, id: O::Id) -> Result<Self::Event, Error> {
        if let Some(tx) = ctx.tx.take() {
            self.delete_object(tx, id.clone()).await?;
            let event = self.create_event(tx, BaseEvent::delete(id)).await?;
            return Ok(event);
        }
        let mut tx = self.db.transaction(write_tx_options()).await?;
        let event = self.delete(ctx.with_tx(&mut tx), id).await?;
        tx.commit().await?;
        Ok(event)
    }
}

macro_rules! object_store_impl {
    ($store:ident, $object:ident, $event:ident) => {
        #[async_trait::async_trait]
        impl $crate::models::ObjectStore for $store {
            type Id = i64;
            type Object = $object;
            type Event = $event;
            type FindIter<'a> = $crate::models::RowsIter<'a, $object>;

            async fn find<'a>(
                &'a self,
                ctx: $crate::models::Context<'a, '_>,
                select: $crate::db::builder::Select,
            ) -> std::result::Result<Self::FindIter<'a>, $crate::core::Error> {
                self.0.find(ctx, select).await
            }

            async fn create(
                &self,
                ctx: $crate::models::Context<'_, '_>,
                object: Self::Object,
            ) -> std::result::Result<Self::Event, $crate::core::Error> {
                self.0.create(ctx, object).await
            }

            async fn update(
                &self,
                ctx: $crate::models::Context<'_, '_>,
                object: Self::Object,
            ) -> std::result::Result<Self::Event, $crate::core::Error> {
                self.0.update(ctx, object).await
            }

            async fn update_where(
                &self,
                ctx: $crate::models::Context<'_, '_>,
                object: Self::Object,
                predicate: $crate::db::builder::Predicate,
            ) -> std::result::Result<Self::Event, $crate::core::Error> {
                self.0.update_where(ctx, object, predicate).await
            }

            async fn delete(
                &self,
                ctx: $crate::models::Context<'_, '_>,
                id: Self::Id,
            ) -> std::result::Result<Self::Event, $crate::core::Error> {
                self.0.delete(ctx, id).await
            }
        }
    };
}

pub(super) use object_store_impl;
use solve_db::{
    Database, Executor, FromRow, IntoRow, IsolationLevel, Row, Rows, TransactionOptions,
};
