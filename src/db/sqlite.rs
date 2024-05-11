use solve_db::{
    driver, ColumnIndex, Connection, ConnectionOptions, FromValue, IntoValue, QueryBuilder,
    RawQuery, Row, Rows, Status, Transaction, TransactionOptions, Value,
};

use crate::core::Error;

struct WrapValue(tokio_sqlite::Value);

impl FromValue for WrapValue {
    fn from_value(value: &Value) -> Result<Self, Error> {
        Ok(Self(match value {
            Value::Null => tokio_sqlite::Value::Null,
            Value::Bool(v) => tokio_sqlite::Value::Integer((*v).into()),
            Value::BigInt(v) => tokio_sqlite::Value::Integer(*v),
            Value::Double(v) => tokio_sqlite::Value::Real(*v),
            Value::Text(v) => tokio_sqlite::Value::Text(v.clone()),
            Value::Blob(v) => tokio_sqlite::Value::Blob(v.clone()),
        }))
    }
}

impl IntoValue for WrapValue {
    fn into_value(self) -> Value {
        match self.0 {
            tokio_sqlite::Value::Null => Value::Null,
            tokio_sqlite::Value::Integer(v) => Value::BigInt(v),
            tokio_sqlite::Value::Real(v) => Value::Double(v),
            tokio_sqlite::Value::Text(v) => Value::Text(v),
            tokio_sqlite::Value::Blob(v) => Value::Blob(v),
        }
    }
}

impl From<Value> for WrapValue {
    fn from(value: Value) -> Self {
        Self(match value {
            Value::Null => tokio_sqlite::Value::Null,
            Value::Bool(v) => tokio_sqlite::Value::Integer(v.into()),
            Value::BigInt(v) => tokio_sqlite::Value::Integer(v),
            Value::Double(v) => tokio_sqlite::Value::Real(v),
            Value::Text(v) => tokio_sqlite::Value::Text(v),
            Value::Blob(v) => tokio_sqlite::Value::Blob(v),
        })
    }
}

impl From<WrapValue> for Value {
    fn from(val: WrapValue) -> Self {
        match val.0 {
            tokio_sqlite::Value::Null => Value::Null,
            tokio_sqlite::Value::Integer(v) => Value::BigInt(v),
            tokio_sqlite::Value::Real(v) => Value::Double(v),
            tokio_sqlite::Value::Text(v) => Value::Text(v),
            tokio_sqlite::Value::Blob(v) => Value::Blob(v),
        }
    }
}

struct WrapRows<'a>(tokio_sqlite::Rows<'a>, ColumnIndex);

#[async_trait::async_trait]
impl<'a> driver::Rows<'a> for WrapRows<'a> {
    fn columns(&self) -> &[String] {
        self.0.columns()
    }

    async fn next(&mut self) -> Option<Result<Row, Error>> {
        Some(
            self.0
                .next()
                .await?
                .map(|r| {
                    Row::new(
                        r.into_values()
                            .into_iter()
                            .map(|v| WrapValue(v).into())
                            .collect(),
                        self.1.clone(),
                    )
                })
                .map_err(|v| v.into()),
        )
    }
}

#[derive(Default)]
pub(super) struct WrapQueryBuilder {
    query: String,
    values: Vec<Value>,
}

impl driver::QueryBuilder for WrapQueryBuilder {
    fn push(&mut self, ch: char) {
        self.query.push(ch);
    }

    fn push_str(&mut self, part: &str) {
        self.query.push_str(part);
    }

    fn push_name(&mut self, name: &str) {
        assert!(name.find(|c| c == '"' || c == '\\').is_none());
        self.push('"');
        self.push_str(name);
        self.push('"');
    }

    fn push_value(&mut self, value: Value) {
        self.values.push(value);
        self.push_str(format!("${}", self.values.len()).as_str())
    }

    fn build(self: Box<Self>) -> RawQuery {
        RawQuery::new(self.query, self.values)
    }
}

pub(super) struct Manager {
    path: String,
}

#[async_trait::async_trait]
impl deadpool::managed::Manager for Manager {
    type Type = tokio_sqlite::Connection;
    type Error = Error;

    async fn create(&self) -> Result<tokio_sqlite::Connection, Error> {
        Ok(tokio_sqlite::Connection::open(&self.path).await?)
    }

    async fn recycle(
        &self,
        _: &mut tokio_sqlite::Connection,
        _: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<Error> {
        Ok(())
    }
}

struct WrapTransaction<'a>(tokio_sqlite::Transaction<'a>);

#[async_trait::async_trait]
impl<'a> driver::Transaction<'a> for WrapTransaction<'a> {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        self.0.commit().await.map_err(|v| v.into())
    }

    async fn rollback(self: Box<Self>) -> Result<(), Error> {
        self.0.rollback().await.map_err(|v| v.into())
    }

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<Status, Error> {
        let values: Vec<_> = values
            .iter()
            .cloned()
            .map(|v| <Value as Into<WrapValue>>::into(v).0)
            .collect();
        let status = self.0.execute(query, &values).await?;
        Ok(Status {
            rows_affected: Some(status.rows_affected() as u64),
            last_insert_id: status.last_insert_id(),
        })
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows, Error> {
        let values: Vec<_> = values
            .iter()
            .cloned()
            .map(|v| <Value as Into<WrapValue>>::into(v).0)
            .collect();
        let rows = self.0.query(query, values).await?;
        let columns = rows.columns().to_owned();
        Ok(WrapRows(rows, ColumnIndex::new(columns)).into())
    }
}

struct WrapConnection(deadpool::managed::Object<Manager>);

#[async_trait::async_trait]
impl driver::Connection for WrapConnection {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn transaction(&mut self, _options: TransactionOptions) -> Result<Transaction, Error> {
        let tx = self.0.transaction().await?;
        Ok(WrapTransaction(tx).into())
    }

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<Status, Error> {
        let values: Vec<_> = values
            .iter()
            .cloned()
            .map(|v| <Value as Into<WrapValue>>::into(v).0)
            .collect();
        let status = self.0.execute(query, values).await?;
        Ok(Status {
            rows_affected: Some(status.rows_affected() as u64),
            last_insert_id: status.last_insert_id(),
        })
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows, Error> {
        let values: Vec<_> = values
            .iter()
            .cloned()
            .map(|v| <Value as Into<WrapValue>>::into(v).0)
            .collect();
        let rows = self.0.query(query, values).await?;
        let columns = rows.columns().to_owned();
        Ok(WrapRows(rows, ColumnIndex::new(columns)).into())
    }
}

pub(super) struct WrapDatabase(deadpool::managed::Pool<Manager>);

impl WrapDatabase {
    pub fn new(path: String) -> Self {
        Self(
            deadpool::managed::Pool::builder(Manager { path })
                .build()
                .unwrap(),
        )
    }
}

#[async_trait::async_trait]
impl driver::Database for WrapDatabase {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn connection(&self, _options: ConnectionOptions) -> Result<Connection, Error> {
        let conn = match self.0.get().await {
            Ok(v) => v,
            Err(err) => return Err(err.to_string().into()),
        };
        Ok(WrapConnection(conn).into())
    }
}
