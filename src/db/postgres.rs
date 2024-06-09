use std::marker::PhantomData;
use std::pin::Pin;
use std::str::FromStr;

use deadpool_postgres::tokio_postgres;
use deadpool_postgres::tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use futures_util::stream::StreamExt;
use solve_db::{
    driver, ColumnIndex, Connection, ConnectionOptions, IsolationLevel, QueryBuilder, Row, Rows,
    Status, Transaction, TransactionOptions, Value,
};
use tokio_util::bytes::BufMut;

use crate::config::PostgresConfig;
use crate::core::Error;

use super::sqlite::WrapQueryBuilder;

#[derive(Debug)]
struct WrapValue(Value);

impl<'a> FromSql<'a> for WrapValue {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Error> {
        Ok(WrapValue(match *ty {
            Type::BOOL => Value::Bool(FromSql::from_sql(ty, raw)?),
            Type::INT2 => Value::BigInt(i16::from_sql(ty, raw)? as i64),
            Type::INT4 => Value::BigInt(i32::from_sql(ty, raw)? as i64),
            Type::INT8 => Value::BigInt(i64::from_sql(ty, raw)?),
            Type::FLOAT4 => Value::Double(f32::from_sql(ty, raw)? as f64),
            Type::FLOAT8 => Value::Double(FromSql::from_sql(ty, raw)?),
            Type::VARCHAR => Value::Text(FromSql::from_sql(ty, raw)?),
            Type::TEXT => Value::Text(FromSql::from_sql(ty, raw)?),
            Type::JSON => Value::Blob(raw.to_owned()),
            Type::JSONB => {
                if raw.is_empty() || raw[0] != 1 {
                    return Err("Unsupported JSONB encoding version".into());
                }
                Value::Blob(raw[1..].to_owned())
            }
            Type::BYTEA => Value::Blob(FromSql::from_sql(ty, raw)?),
            _ => unreachable!(),
        }))
    }

    fn from_sql_null(_ty: &Type) -> Result<Self, Error> {
        Ok(WrapValue(Value::Null))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::BOOL
                | Type::INT2
                | Type::INT4
                | Type::INT8
                | Type::FLOAT4
                | Type::FLOAT8
                | Type::VARCHAR
                | Type::TEXT
                | Type::JSON
                | Type::JSONB
                | Type::BYTEA
        )
    }
}

impl ToSql for WrapValue {
    fn to_sql(&self, ty: &Type, out: &mut tokio_util::bytes::BytesMut) -> Result<IsNull, Error> {
        match &self.0 {
            Value::Null => Ok(IsNull::Yes),
            Value::Bool(v) => ToSql::to_sql(&v, ty, out),
            Value::BigInt(v) => match *ty {
                Type::INT2 => ToSql::to_sql(&i16::try_from(*v)?, ty, out),
                Type::INT4 => ToSql::to_sql(&i32::try_from(*v)?, ty, out),
                _ => ToSql::to_sql(&v, ty, out),
            },
            Value::Double(v) => match *ty {
                Type::FLOAT4 => ToSql::to_sql(&(*v as f32), ty, out),
                _ => ToSql::to_sql(&v, ty, out),
            },
            Value::Text(v) => ToSql::to_sql(&v, ty, out),
            Value::Blob(v) => match *ty {
                Type::JSON => {
                    out.put(v.as_slice());
                    Ok(IsNull::No)
                }
                Type::JSONB => {
                    out.put_u8(1);
                    out.put(v.as_slice());
                    Ok(IsNull::No)
                }
                _ => ToSql::to_sql(&v, ty, out),
            },
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::BOOL
                | Type::INT2
                | Type::INT4
                | Type::INT8
                | Type::FLOAT4
                | Type::FLOAT8
                | Type::VARCHAR
                | Type::TEXT
                | Type::JSON
                | Type::JSONB
                | Type::BYTEA
        )
    }

    to_sql_checked!();
}

fn get_isolation_level(level: IsolationLevel) -> tokio_postgres::IsolationLevel {
    match level {
        IsolationLevel::ReadUncommitted => tokio_postgres::IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted => tokio_postgres::IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead => tokio_postgres::IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable => tokio_postgres::IsolationLevel::Serializable,
    }
}

struct WrapRows<'a> {
    rows: Pin<Box<tokio_postgres::RowStream>>,
    columns: Vec<String>,
    column_index: ColumnIndex,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> WrapRows<'a> {
    pub fn new(statement: tokio_postgres::Statement, rows: tokio_postgres::RowStream) -> Self {
        let columns: Vec<_> = statement
            .columns()
            .iter()
            .map(|c| c.name().to_owned())
            .collect();
        Self {
            rows: Box::pin(rows),
            columns: columns.clone(),
            column_index: ColumnIndex::new(columns),
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<'a> driver::Rows<'a> for WrapRows<'a> {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    async fn next(&mut self) -> Option<Result<Row, Error>> {
        let map_row = |r: tokio_postgres::Row| {
            let mut values = Vec::with_capacity(self.columns.len());
            for i in 0..self.columns.len() {
                let value: WrapValue = r.get(i);
                values.push(value.0);
            }
            Row::new(values, self.column_index.clone())
        };
        self.rows
            .next()
            .await
            .map(|r| r.map(map_row).map_err(|e| e.into()))
    }
}

struct WrapTransaction<'a>(deadpool_postgres::Transaction<'a>);

#[async_trait::async_trait]
impl<'a> driver::Transaction<'a> for WrapTransaction<'a> {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        Ok(self.0.commit().await?)
    }

    async fn rollback(self: Box<Self>) -> Result<(), Error> {
        Ok(self.0.rollback().await?)
    }

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<Status, Error> {
        let rows_affected = self
            .0
            .execute_raw(query, values.iter().map(|v| WrapValue(v.clone())))
            .await?;
        Ok(Status {
            rows_affected: Some(rows_affected),
            last_insert_id: None,
        })
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows, Error> {
        let statement = self.0.client().prepare(query).await?;
        let rows = self
            .0
            .query_raw(&statement, values.iter().map(|v| WrapValue(v.clone())))
            .await?;
        Ok(WrapRows::new(statement, rows).into())
    }
}

struct WrapConnection(deadpool_postgres::Client);

#[async_trait::async_trait]
impl driver::Connection for WrapConnection {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn transaction(&mut self, options: TransactionOptions) -> Result<Transaction, Error> {
        let tx_builder = self
            .0
            .build_transaction()
            .read_only(options.read_only)
            .isolation_level(get_isolation_level(options.isolation_level));
        Ok(WrapTransaction(tx_builder.start().await?).into())
    }

    async fn execute(&mut self, query: &str, values: &[Value]) -> Result<Status, Error> {
        let rows_affected = self
            .0
            .execute_raw(query, values.iter().map(|v| WrapValue(v.clone())))
            .await?;
        Ok(Status {
            rows_affected: Some(rows_affected),
            last_insert_id: None,
        })
    }

    async fn query(&mut self, query: &str, values: &[Value]) -> Result<Rows, Error> {
        let statement = self.0.prepare(query).await?;
        let rows = self
            .0
            .query_raw(&statement, values.iter().map(|v| WrapValue(v.clone())))
            .await?;
        Ok(WrapRows::new(statement, rows).into())
    }
}

pub(super) struct WrapDatabase {
    read_only: deadpool_postgres::Pool,
    writable: deadpool_postgres::Pool,
}

impl WrapDatabase {
    pub fn new(config: &PostgresConfig) -> Result<Self, Error> {
        let mut hosts = Vec::new();
        let mut ports = Vec::new();
        for host in &config.hosts {
            let parts: Vec<_> = host.rsplitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(format!("invalid host format {}", host).into());
            }
            ports.push(u16::from_str(parts[0])?);
            hosts.push(parts[1].to_owned());
        }
        let mut pg_config = deadpool_postgres::Config {
            hosts: Some(hosts),
            ports: Some(ports),
            user: Some(config.user.to_owned()),
            password: Some(config.password.to_owned()),
            dbname: Some(config.name.to_owned()),
            target_session_attrs: Some(deadpool_postgres::TargetSessionAttrs::Any),
            ..Default::default()
        };
        let tls_config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        let runtime = Some(deadpool_postgres::Runtime::Tokio1);
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
        let read_only = pg_config.create_pool(runtime, tls.clone())?;
        pg_config.target_session_attrs = Some(deadpool_postgres::TargetSessionAttrs::ReadWrite);
        let writable = pg_config.create_pool(runtime, tls.clone())?;
        Ok(Self {
            read_only,
            writable,
        })
    }
}

#[async_trait::async_trait]
impl driver::Database for WrapDatabase {
    fn builder(&self) -> QueryBuilder {
        QueryBuilder::new(WrapQueryBuilder::default())
    }

    async fn connection(&self, options: ConnectionOptions) -> Result<Connection, Error> {
        let conn = if options.read_only {
            self.read_only.get().await
        } else {
            self.writable.get().await
        }?;
        Ok(Connection::new(WrapConnection(conn)))
    }
}
