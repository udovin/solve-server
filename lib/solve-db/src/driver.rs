use crate::{ConnectionOptions, Error, RawQuery, Row, TransactionOptions, Value};

pub trait QueryBuilder: Send + Sync {
    fn push(&mut self, ch: char);

    fn push_str(&mut self, string: &str);

    fn push_name(&mut self, name: &str);

    fn push_value(&mut self, value: Value);

    fn build(self: Box<Self>) -> RawQuery;
}

#[async_trait::async_trait]
pub trait Rows<'a>: Send + Sync {
    fn columns(&self) -> &[String];

    async fn next(&mut self) -> Option<Result<Row, Error>>;
}

#[async_trait::async_trait]
pub trait Transaction<'a>: Send + Sync {
    fn builder(&self) -> crate::QueryBuilder;

    async fn commit(self: Box<Self>) -> Result<(), Error>;

    async fn rollback(self: Box<Self>) -> Result<(), Error>;

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<crate::Status, Error>;

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<crate::Rows, Error>;
}

#[async_trait::async_trait]
pub trait Connection: Send + Sync {
    fn builder(&self) -> crate::QueryBuilder;

    async fn transaction(
        &mut self,
        options: TransactionOptions,
    ) -> Result<crate::Transaction, Error>;

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<crate::Status, Error>;

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<crate::Rows, Error>;
}

#[async_trait::async_trait]
pub trait Database: Send + Sync {
    fn builder(&self) -> crate::QueryBuilder;

    async fn connection(&self, options: ConnectionOptions) -> Result<crate::Connection, Error>;
}
