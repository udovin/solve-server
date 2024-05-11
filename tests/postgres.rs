use solve::core::{blocking_await, Error};
use solve::db::builder::IntoRow;
use solve::db::{new_database, FromRow, Value};
use solve_db::{ConnectionOptions, Database, IntoValue, RawQuery, Row};
use solve_db_types::JSON;

mod common;

struct TestTypesRow {
    pub id: i64,
    pub int64: i64,
    pub null_int64: Option<i64>,
    pub string: String,
    pub null_string: Option<String>,
    pub json: JSON,
    pub null_json: Option<JSON>,
}

impl FromRow for TestTypesRow {
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok(Self {
            id: row.get_parsed("id")?,
            int64: row.get_parsed("int64")?,
            null_int64: row.get_parsed("null_int64")?,
            string: row.get_parsed("string")?,
            null_string: row.get_parsed("null_string")?,
            json: row.get_parsed("json")?,
            null_json: row.get_parsed("null_json")?,
        })
    }
}

impl IntoRow for TestTypesRow {
    fn into_row(self) -> solve::db::builder::Row {
        let mut row = Vec::new();
        row.push(("id".into(), self.id.into_value()));
        row.push(("int64".into(), self.int64.into_value()));
        row.push(("null_int64".into(), self.null_int64.into_value()));
        row.push(("string".into(), self.string.into_value()));
        row.push(("null_string".into(), self.null_string.into_value()));
        row.push(("json".into(), self.json.into_value()));
        row.push(("null_json".into(), self.null_json.into_value()));
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
        hosts: vec![format!("{host}:{port}")],
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
                    Value::from(1),
                    Value::Null,
                    Value::from("2"),
                    Value::Null,
                    Value::Blob("3".into()),
                    Value::Null,
                    Value::Blob("4".into()),
                    Value::Null,
                    Value::from(5),
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
                    Value::from(2),
                    Value::from(3),
                    Value::Text("4".into()),
                    Value::Text("5".into()),
                    Value::Blob("6".into()),
                    Value::Blob("7".into()),
                    Value::Blob("8".into()),
                    Value::Blob("9".into()),
                    Value::from(10),
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
            assert_eq!(row.get_value(0).unwrap().clone(), Value::from(1));
            assert_eq!(row.get_value(1).unwrap().clone(), Value::from(1));
            assert_eq!(row.get_value(2).unwrap().clone(), Value::Null);
            assert_eq!(row.get_value(3).unwrap().clone(), Value::Text("2".into()));
            assert_eq!(row.get_value(4).unwrap().clone(), Value::Null);
            assert_eq!(row.get_value(5).unwrap().clone(), Value::Blob("3".into()));
            assert_eq!(row.get_value(6).unwrap().clone(), Value::Null);
            assert_eq!(row.get_value(7).unwrap().clone(), Value::Blob("4".into()));
            assert_eq!(row.get_value(8).unwrap().clone(), Value::Null);
            assert_eq!(row.get_value(9).unwrap().clone(), Value::from(5));
        }
        {
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap().clone(), Value::from(2));
            assert_eq!(row.get_value(1).unwrap().clone(), Value::from(2));
            assert_eq!(row.get_value(2).unwrap().clone(), Value::from(3));
            assert_eq!(row.get_value(3).unwrap().clone(), Value::from("4"));
            assert_eq!(row.get_value(4).unwrap().clone(), Value::from("5"));
            assert_eq!(row.get_value(5).unwrap().clone(), Value::Blob("6".into()));
            assert_eq!(row.get_value(6).unwrap().clone(), Value::Blob("7".into()));
            assert_eq!(row.get_value(7).unwrap().clone(), Value::Blob("8".into()));
            assert_eq!(row.get_value(8).unwrap().clone(), Value::Blob("9".into()));
            assert_eq!(row.get_value(9).unwrap().clone(), Value::from(10));
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
