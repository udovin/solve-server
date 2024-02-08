use std::collections::HashMap;
use std::sync::Arc;

use crate::config::DatabaseConfig;
use crate::core::Error;

use super::{postgres, sqlite, IntoQuery, Query, QueryBuilder, Value};

pub trait ColumnIndex {
    fn index(&self, row: &Row) -> Result<usize, Error>;
}

impl ColumnIndex for usize {
    fn index(&self, row: &Row) -> Result<usize, Error> {
        if *self < row.values.len() {
            return Ok(*self);
        }
        Err(format!("column out of range: {} >= {}", self, row.values.len()).into())
    }
}

impl ColumnIndex for String {
    fn index(&self, row: &Row) -> Result<usize, Error> {
        row.columns
            .get(self)
            .copied()
            .ok_or(format!("unknown column name: {}", self).into())
    }
}

impl ColumnIndex for &str {
    fn index(&self, row: &Row) -> Result<usize, Error> {
        row.columns
            .get(*self)
            .copied()
            .ok_or(format!("unknown column name: {}", *self).into())
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    columns: Arc<HashMap<String, usize>>,
    values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>, columns: Arc<HashMap<String, usize>>) -> Self {
        assert_eq!(values.len(), columns.len());
        Self { values, columns }
    }

    pub fn get<I: ColumnIndex>(&self, index: I) -> Result<Value, Error> {
        let index = index.index(self)?;
        Ok(self.values.get(index).unwrap().clone())
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self, Error>;
}

#[async_trait::async_trait]
pub trait RowsBackend<'a>: Send + Sync {
    fn columns(&self) -> &[String];

    async fn next(&mut self) -> Option<Result<Row, Error>>;
}

impl<'a, T: RowsBackend<'a> + 'a> From<T> for Rows<'a> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

#[derive(Default, Clone)]
pub struct Status {
    pub(super) rows_affected: Option<u64>,
    pub(super) last_insert_id: Option<i64>,
}

impl Status {
    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }

    pub fn last_insert_id(&self) -> Option<i64> {
        self.last_insert_id
    }
}

pub struct Rows<'a> {
    inner: Box<dyn RowsBackend<'a> + 'a>,
}

impl<'a> Rows<'a> {
    pub fn new<T: RowsBackend<'a> + 'a>(rows: T) -> Self {
        let inner = Box::new(rows);
        Self { inner }
    }

    pub fn columns(&self) -> &[String] {
        self.inner.columns()
    }

    pub async fn next(&mut self) -> Option<Result<Row, Error>> {
        self.inner.next().await
    }
}

#[async_trait::async_trait]
pub trait TransactionBackend<'a>: Send + Sync {
    fn builder(&self) -> QueryBuilder;

    async fn commit(self: Box<Self>) -> Result<(), Error>;

    async fn rollback(self: Box<Self>) -> Result<(), Error>;

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<Status, Error>;

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows, Error>;
}

impl<'a, T: TransactionBackend<'a> + 'a> From<T> for Transaction<'a> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

pub struct Transaction<'a> {
    inner: Box<dyn TransactionBackend<'a> + 'a>,
}

impl<'a> Transaction<'a> {
    pub fn new<T: TransactionBackend<'a> + 'a>(tx: T) -> Self {
        let inner = Box::new(tx);
        Self { inner }
    }

    pub fn builder(&self) -> QueryBuilder {
        self.inner.builder()
    }

    pub async fn commit(self) -> Result<(), Error> {
        self.inner.commit().await
    }

    pub async fn rollback(self) -> Result<(), Error> {
        self.inner.rollback().await
    }

    pub async fn execute<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Status, Error> {
        let query = query.into_query(self.builder());
        self.inner.execute(query.query(), query.values()).await
    }

    pub async fn query<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Rows, Error> {
        let query = query.into_query(self.builder());
        self.inner.query(query.query(), query.values()).await
    }
}

#[derive(Clone, Copy, Default)]
pub enum IsolationLevel {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Clone, Copy, Default)]
pub struct TransactionOptions {
    pub isolation_level: IsolationLevel,
    pub read_only: bool,
}

#[async_trait::async_trait]
pub trait ConnectionBackend: Send + Sync {
    fn builder(&self) -> QueryBuilder;

    async fn transaction(&mut self, options: TransactionOptions) -> Result<Transaction, Error>;

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<Status, Error>;

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows, Error>;
}

impl<T: ConnectionBackend + 'static> From<T> for Connection {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

pub struct Connection {
    inner: Box<dyn ConnectionBackend>,
}

impl Connection {
    pub fn new<T: ConnectionBackend + 'static>(conn: T) -> Self {
        let inner = Box::new(conn);
        Self { inner }
    }

    pub fn builder(&self) -> QueryBuilder {
        self.inner.builder()
    }

    pub async fn transaction(
        &mut self,
        options: TransactionOptions,
    ) -> Result<Transaction<'_>, Error> {
        self.inner.transaction(options).await
    }

    pub async fn execute<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Status, Error> {
        let query = query.into_query(self.builder());
        self.inner.execute(query.query(), query.values()).await
    }

    pub async fn query<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Rows, Error> {
        let query = query.into_query(self.builder());
        self.inner.query(query.query(), query.values()).await
    }
}

#[derive(Clone, Copy, Default)]
pub struct ConnectionOptions {
    pub read_only: bool,
}

#[async_trait::async_trait]
pub trait DatabaseBackend: Send + Sync {
    fn builder(&self) -> QueryBuilder;

    async fn connection(&self, options: ConnectionOptions) -> Result<Connection, Error>;
}

impl<T: DatabaseBackend + 'static> From<T> for Database {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

struct OwnedTransaction {
    conn: *mut (dyn ConnectionBackend),
    tx: Option<Box<dyn TransactionBackend<'static>>>,
}

impl Drop for OwnedTransaction {
    fn drop(&mut self) {
        drop(self.tx.take());
        drop(unsafe { Box::from_raw(self.conn) });
    }
}

unsafe impl Send for OwnedTransaction {}

unsafe impl Sync for OwnedTransaction {}

#[async_trait::async_trait]
impl<'a> TransactionBackend<'a> for OwnedTransaction {
    fn builder(&self) -> QueryBuilder {
        self.tx.as_ref().unwrap().builder()
    }

    async fn commit(mut self: Box<Self>) -> Result<(), Error> {
        self.tx.take().unwrap().commit().await
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), Error> {
        self.tx.take().unwrap().rollback().await
    }

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<Status, Error> {
        self.tx.as_mut().unwrap().execute(query, values).await
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows, Error> {
        self.tx.as_mut().unwrap().query(query, values).await
    }
}

struct OwnedRows {
    conn: *mut (dyn ConnectionBackend),
    rows: Option<Box<dyn RowsBackend<'static>>>,
}

impl Drop for OwnedRows {
    fn drop(&mut self) {
        drop(self.rows.take());
        drop(unsafe { Box::from_raw(self.conn) });
    }
}

unsafe impl Send for OwnedRows {}

unsafe impl Sync for OwnedRows {}

#[async_trait::async_trait]
impl<'a> RowsBackend<'a> for OwnedRows {
    fn columns(&self) -> &[String] {
        self.rows.as_ref().unwrap().columns()
    }

    async fn next(&mut self) -> Option<Result<Row, Error>> {
        self.rows.as_mut().unwrap().next().await
    }
}

pub struct Database {
    inner: Box<dyn DatabaseBackend>,
}

impl Database {
    pub fn new<T: DatabaseBackend + 'static>(db: T) -> Self {
        let inner = Box::new(db);
        Self { inner }
    }

    pub fn builder(&self) -> QueryBuilder {
        self.inner.builder()
    }

    pub async fn connection(&self, options: ConnectionOptions) -> Result<Connection, Error> {
        self.inner.connection(options).await
    }

    pub async fn transaction(&self, options: TransactionOptions) -> Result<Transaction, Error> {
        let conn_options = ConnectionOptions {
            read_only: options.read_only,
        };
        let conn = self.connection(conn_options).await?;
        let conn = Box::leak(conn.inner);
        let mut tx = OwnedTransaction { conn, tx: None };
        tx.tx = Some(conn.transaction(options).await?.inner);
        Ok(Transaction::new(tx))
    }

    pub async fn execute<Q: IntoQuery<T>, T: Query>(&self, query: Q) -> Result<Status, Error> {
        let mut conn = self.connection(Default::default()).await?;
        conn.execute(query).await
    }

    pub async fn query<Q: IntoQuery<T>, T: Query>(&self, query: Q) -> Result<Rows, Error> {
        let conn = self.connection(Default::default()).await?;
        let conn = Box::leak(conn.inner);
        let mut rows = OwnedRows { conn, rows: None };
        let query = query.into_query(self.builder());
        rows.rows = Some(conn.query(query.query(), query.values()).await?.inner);
        Ok(Rows::new(rows))
    }
}

#[async_trait::async_trait]
pub trait Executor<'a>: Send {
    fn builder(&self) -> QueryBuilder;

    async fn execute<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Status, Error>;

    async fn query<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Rows, Error>;
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for Transaction<'a> {
    fn builder(&self) -> QueryBuilder {
        Transaction::builder(self)
    }

    async fn execute<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Status, Error> {
        Transaction::execute(self, query).await
    }

    async fn query<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Rows, Error> {
        Transaction::query(self, query).await
    }
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for Connection {
    fn builder(&self) -> QueryBuilder {
        Connection::builder(self)
    }

    async fn execute<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Status, Error> {
        Connection::execute(self, query).await
    }

    async fn query<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Rows, Error> {
        Connection::query(self, query).await
    }
}

#[async_trait::async_trait]
impl<'a> Executor<'a> for Database {
    fn builder(&self) -> QueryBuilder {
        Database::builder(self)
    }

    async fn execute<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Status, Error> {
        Database::execute(self, query).await
    }

    async fn query<Q: IntoQuery<T>, T: Query>(&mut self, query: Q) -> Result<Rows, Error> {
        Database::query(self, query).await
    }
}

pub fn new_database(config: &DatabaseConfig) -> Result<Database, Error> {
    let db = match config {
        DatabaseConfig::SQLite(config) => sqlite::WrapDatabase::new(config.path.clone()).into(),
        DatabaseConfig::Postgres(config) => postgres::WrapDatabase::new(config)?.into(),
    };
    Ok(db)
}
