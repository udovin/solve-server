use solve::core::{blocking_await, Error};
use solve::db::builder::IntoRow;
use solve::db::{new_database, ConnectionOptions, Database, FromRow, RawQuery, Row, Value};

mod common;

struct TestTypesRow {
    pub id: i64,
    pub int64: i64,
    pub null_int64: Option<i64>,
    pub string: String,
    pub null_string: Option<String>,
    pub json: serde_json::Value,
    pub null_json: Option<serde_json::Value>,
}

impl FromRow for TestTypesRow {
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok(Self {
            id: row.get("id").ok_or("unknown field")?.clone().try_into()?,
            int64: row.get("int64").ok_or("unknown field")?.clone().try_into()?,
            null_int64: row.get("null_int64").ok_or("unknown field")?.clone().try_into()?,
            string: row.get("string").ok_or("unknown field")?.clone().try_into()?,
            null_string: row.get("null_string").ok_or("unknown field")?.clone().try_into()?,
            json: row.get("json").ok_or("unknown field")?.clone().try_into()?,
            null_json: row.get("null_json").ok_or("unknown field")?.clone().try_into()?,
        })
    }
}

impl IntoRow for TestTypesRow {
    fn into_row(self) -> solve::db::builder::Row {
        let mut row = Vec::new();
        row.push(("id".into(), self.id.into()));
        row.push(("int64".into(), self.int64.into()));
        row.push(("null_int64".into(), self.null_int64.into()));
        row.push(("string".into(), self.string.into()));
        row.push(("null_string".into(), self.null_string.into()));
        row.push(("json".into(), self.json.into()));
        row.push(("null_json".into(), self.null_json.into()));
        row
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_postgres() {
    let host = match std::env::var("POSTGRES_HOST") {
        Ok(v) => v,
        Err(_) => return,
    };
    let port = match std::env::var("POSTGRES_PORT") {
        Ok(v) => v,
        Err(_) => return,
    };
    let config = solve::config::PostgresConfig {
        user: std::env::var("POSTGRES_USER").unwrap_or("postgres".into()),
        hosts: vec![format!("{host}:{port}").into()],
        password: std::env::var("POSTGRES_PASSWORD").unwrap_or("postgres".into()),
        name: std::env::var("POSTGRES_NAME").unwrap_or("postgres".into()),
        sslmode: "".into(),
    };
    let db: Database = new_database(&solve::config::DatabaseConfig::Postgres(config)).unwrap();
    let _cleanup = {
        let mut conn = db.connection(ConnectionOptions::default()).await.unwrap();
        Defer::new(move || {
            blocking_await(conn.execute(r#"DROP TABLE IF EXISTS "test_solve_types_tbl""#)).unwrap();
        })
    };
    let mut conn = db.connection(ConnectionOptions::default()).await.unwrap();
    {
        let status = conn
            .execute(
                r#"CREATE TABLE IF NOT EXISTS "test_solve_types_tbl" (
    "id" bigserial PRIMARY KEY,
    "int64" BIGINT NOT NULL,
    "null_int64" BIGINT,
    "string" TEXT NOT NULL,
    "null_string" TEXT,
    "json" json NOT NULL,
    "null_json" json,
    "jsonb" jsonb NOT NULL,
    "null_jsonb" jsonb,
    "smallint" smallint NOT NULL
)"#,
            )
            .await
            .unwrap();
        assert_eq!(status.rows_affected().unwrap(), 0);
    }
    {
        let status = conn
            .execute(RawQuery::new(
                r#"INSERT INTO "test_solve_types_tbl" (
                    "int64", "null_int64",
                    "string", "null_string",
                    "json", "null_json",
                    "jsonb", "null_jsonb",
                    "smallint"
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
                [
                    Value::BigInt(1),
                    Value::Null,
                    Value::Text("2".into()),
                    Value::Null,
                    Value::Blob("3".into()),
                    Value::Null,
                    Value::Blob("4".into()),
                    Value::Null,
                    Value::BigInt(5),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(status.rows_affected().unwrap(), 1);
    }
    {
        let status = conn
            .execute(RawQuery::new(
                r#"INSERT INTO "test_solve_types_tbl" (
                    "int64", "null_int64",
                    "string", "null_string",
                    "json", "null_json",
                    "jsonb", "null_jsonb",
                    "smallint"
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
                [
                    Value::BigInt(2),
                    Value::BigInt(3),
                    Value::Text("4".into()),
                    Value::Text("5".into()),
                    Value::Blob("6".into()),
                    Value::Blob("7".into()),
                    Value::Blob("8".into()),
                    Value::Blob("9".into()),
                    Value::BigInt(10),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(status.rows_affected().unwrap(), 1);
    }
    {
        let mut rows = conn
            .query(r#"SELECT * FROM "test_solve_types_tbl" ORDER BY "id""#)
            .await
            .unwrap();
        {
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get(0).unwrap().clone(), Value::BigInt(1));
            assert_eq!(row.get(1).unwrap().clone(), Value::BigInt(1));
            assert_eq!(row.get(2).unwrap().clone(), Value::Null);
            assert_eq!(row.get(3).unwrap().clone(), Value::Text("2".into()));
            assert_eq!(row.get(4).unwrap().clone(), Value::Null);
            assert_eq!(row.get(5).unwrap().clone(), Value::Blob("3".into()));
            assert_eq!(row.get(6).unwrap().clone(), Value::Null);
            assert_eq!(row.get(7).unwrap().clone(), Value::Blob("4".into()));
            assert_eq!(row.get(8).unwrap().clone(), Value::Null);
            assert_eq!(row.get(9).unwrap().clone(), Value::BigInt(5));
        }
        {
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get(0).unwrap().clone(), Value::BigInt(2));
            assert_eq!(row.get(1).unwrap().clone(), Value::BigInt(2));
            assert_eq!(row.get(2).unwrap().clone(), Value::BigInt(3));
            assert_eq!(row.get(3).unwrap().clone(), Value::Text("4".into()));
            assert_eq!(row.get(4).unwrap().clone(), Value::Text("5".into()));
            assert_eq!(row.get(5).unwrap().clone(), Value::Blob("6".into()));
            assert_eq!(row.get(6).unwrap().clone(), Value::Blob("7".into()));
            assert_eq!(row.get(7).unwrap().clone(), Value::Blob("8".into()));
            assert_eq!(row.get(8).unwrap().clone(), Value::Blob("9".into()));
            assert_eq!(row.get(9).unwrap().clone(), Value::BigInt(10));
        }
        assert!(rows.next().await.is_none());
    }
}

struct Defer<T: FnOnce()> {
    func: Option<T>,
}

impl<T: FnOnce()> Defer<T> {
    pub fn new(func: T) -> Self {
        Self { func: Some(func) }
    }
}

impl<T: FnOnce()> Drop for Defer<T> {
    fn drop(&mut self) {
        if let Some(func) = self.func.take() {
            func()
        }
    }
}
