use std::collections::HashMap;
use std::sync::Arc;

use crate::core::Error;

use super::{
    Connection, ConnectionBackend, ConnectionOptions, DatabaseBackend, QueryBuilder,
    QueryBuilderBackend, RawQuery, Row, Rows, RowsBackend, Status, Transaction, TransactionBackend,
    TransactionOptions, Value,
};

impl From<Value> for tokio_sqlite::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(v) => Self::Integer(v.into()),
            Value::BigInt(v) => Self::Integer(v),
            Value::Double(v) => Self::Real(v),
            Value::Text(v) => Self::Text(v),
            Value::Blob(v) => Self::Blob(v),
        }
    }
}

impl From<tokio_sqlite::Value> for Value {
    fn from(value: tokio_sqlite::Value) -> Self {
        match value {
            tokio_sqlite::Value::Null => Self::Null,
            tokio_sqlite::Value::Integer(v) => Self::BigInt(v),
            tokio_sqlite::Value::Real(v) => Self::Double(v),
            tokio_sqlite::Value::Text(v) => Self::Text(v),
            tokio_sqlite::Value::Blob(v) => Self::Blob(v),
        }
    }
}

struct WrapRows<'a>(tokio_sqlite::Rows<'a>, Arc<HashMap<String, usize>>);

#[async_trait::async_trait]
impl<'a> RowsBackend<'a> for WrapRows<'a> {
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
                        r.into_values().into_iter().map(|v| v.into()).collect(),
                        self.1.clone(),
                    )
                })
                .map_err(|v| v.into()),
        )
    }
}

struct WrapTransaction<'a>(tokio_sqlite::Transaction<'a>);

#[async_trait::async_trait]
impl<'a> TransactionBackend<'a> for WrapTransaction<'a> {
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
        let values: Vec<_> = values.iter().cloned().map(|v| v.into()).collect();
        let status = self.0.execute(query, &values).await?;
        Ok(Status {
            rows_affected: Some(status.rows_affected() as u64),
            last_insert_id: status.last_insert_id(),
        })
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows, Error> {
        let values: Vec<_> = values.iter().cloned().map(|v| v.into()).collect();
        let rows = self.0.query(query, values).await?;
        let mut columns = HashMap::with_capacity(rows.columns().len());
        for i in 0..rows.columns().len() {
            columns.insert(rows.columns()[i].clone(), i);
        }
        Ok(WrapRows(rows, Arc::new(columns)).into())
    }
}

struct WrapConnection(tokio_sqlite::Connection);

#[async_trait::async_trait]
impl ConnectionBackend for WrapConnection {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn transaction(&mut self, _options: TransactionOptions) -> Result<Transaction, Error> {
        let tx = self.0.transaction().await?;
        Ok(WrapTransaction(tx).into())
    }

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<Status, Error> {
        let values: Vec<_> = values.iter().cloned().map(|v| v.into()).collect();
        let status = self.0.execute(query, values).await?;
        Ok(Status {
            rows_affected: Some(status.rows_affected() as u64),
            last_insert_id: status.last_insert_id(),
        })
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows, Error> {
        let values: Vec<_> = values.iter().cloned().map(|v| v.into()).collect();
        let rows = self.0.query(query, values).await?;
        let mut columns = HashMap::with_capacity(rows.columns().len());
        for i in 0..rows.columns().len() {
            columns.insert(rows.columns()[i].clone(), i);
        }
        Ok(WrapRows(rows, Arc::new(columns)).into())
    }
}

#[derive(Default)]
pub(super) struct WrapQueryBuilder {
    query: String,
    values: Vec<Value>,
}

impl QueryBuilderBackend for WrapQueryBuilder {
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

pub(super) struct WrapDatabase {
    path: String,
}

impl WrapDatabase {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

#[async_trait::async_trait]
impl DatabaseBackend for WrapDatabase {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn connection(&self, _options: ConnectionOptions) -> Result<Connection, Error> {
        let conn = tokio_sqlite::Connection::open(self.path.clone()).await?;
        Ok(WrapConnection(conn).into())
    }
}
