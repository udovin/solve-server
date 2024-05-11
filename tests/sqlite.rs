use solve::db::new_database;
use solve_db::{Database, IntoValue, Value};

mod common;

#[tokio::test(flavor = "multi_thread")]
async fn test_any_sqlite() {
    let tmpdir = common::temp_dir().unwrap();
    let config = solve::config::SQLiteConfig {
        path: tmpdir
            .join("db.sqlite")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string(),
    };
    let db: Database = new_database(&solve::config::DatabaseConfig::SQLite(config)).unwrap();
    db.execute("CREATE TABLE test_tbl (a INTEGER PRIMARY KEY, b TEXT NOT NULL)")
        .await
        .unwrap();
    db.execute((
        "INSERT INTO test_tbl (b) VALUES ($1), ($2)",
        ["test1".into_value(), "test2".into_value()].as_slice(),
    ))
    .await
    .unwrap();
    let mut rows = db
        .query("SELECT a, b FROM test_tbl ORDER BY a")
        .await
        .unwrap();
    assert_eq!(rows.columns(), vec!["a", "b"]);
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap().clone(), Value::BigInt(1));
    assert_eq!(
        row.get_value(1).unwrap().clone(),
        Value::Text("test1".into())
    );
    assert_eq!(row.get_value("a").unwrap().clone(), Value::BigInt(1));
    assert_eq!(
        row.get_value("b").unwrap().clone(),
        Value::Text("test1".into())
    );
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap().clone(), Value::BigInt(2));
    assert_eq!(
        row.get_value(1).unwrap().clone(),
        Value::Text("test2".into())
    );
    assert!(rows.next().await.is_none());
    // Check commit.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute("INSERT INTO test_tbl (b) VALUES ('test3')")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut rows = db.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap().clone(), Value::BigInt(3));
    // Check rollback.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute("INSERT INTO test_tbl (b) VALUES ('test3')")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let mut rows = db.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap().clone(), Value::BigInt(3));
    // Check drop.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute("INSERT INTO test_tbl (b) VALUES ('test3')")
        .await
        .unwrap();
    drop(tx);
    let mut rows = db.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap().clone(), Value::BigInt(3));
    // Check uncommited.
    let mut tx = db.transaction(Default::default()).await.unwrap();
    tx.execute("INSERT INTO test_tbl (b) VALUES ('test3')")
        .await
        .unwrap();
    let mut rows = tx.query("SELECT COUNT(*) FROM test_tbl").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap().clone(), Value::BigInt(4));
}
