use crate::{driver, Error, IntoQuery, Query, QueryBuilder, Row, Value};

#[derive(Default, Clone)]
pub struct Status {
    pub rows_affected: Option<u64>,
    pub last_insert_id: Option<i64>,
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
    inner: Box<dyn driver::Rows<'a> + 'a>,
}

impl<'a> Rows<'a> {
    pub fn new<T: driver::Rows<'a> + 'a>(rows: T) -> Self {
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

impl<'a, T: driver::Rows<'a> + 'a> From<T> for Rows<'a> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

pub struct Transaction<'a> {
    inner: Box<dyn driver::Transaction<'a> + 'a>,
}

impl<'a> Transaction<'a> {
    pub fn new<T: driver::Transaction<'a> + 'a>(tx: T) -> Self {
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

impl<'a, T: driver::Transaction<'a> + 'a> From<T> for Transaction<'a> {
    fn from(value: T) -> Self {
        Self::new(value)
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

pub struct Connection {
    inner: Box<dyn driver::Connection>,
}

impl Connection {
    pub fn new<T: driver::Connection + 'static>(conn: T) -> Self {
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

impl<T: driver::Connection + 'static> From<T> for Connection {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Default)]
pub struct ConnectionOptions {
    pub read_only: bool,
}

pub struct Database {
    inner: Box<dyn driver::Database>,
}

impl Database {
    pub fn new<T: driver::Database + 'static>(db: T) -> Self {
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

impl<T: driver::Database + 'static> From<T> for Database {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

struct OwnedTransaction {
    conn: *mut (dyn driver::Connection),
    tx: Option<Box<dyn driver::Transaction<'static>>>,
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
impl<'a> driver::Transaction<'a> for OwnedTransaction {
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
    conn: *mut (dyn driver::Connection),
    rows: Option<Box<dyn driver::Rows<'static>>>,
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
impl<'a> driver::Rows<'a> for OwnedRows {
    fn columns(&self) -> &[String] {
        self.rows.as_ref().unwrap().columns()
    }

    async fn next(&mut self) -> Option<Result<Row, Error>> {
        self.rows.as_mut().unwrap().next().await
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
