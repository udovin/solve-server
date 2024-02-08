use std::sync::Arc;

use solve::db::{new_database, Database, TransactionOptions};
use solve::models::{
    now, Context, Event, EventKind, File, FileStatus, FileStore, ObjectStore, Task, TaskStore,
};

mod common;

#[tokio::test(flavor = "multi_thread")]
async fn test_file_store() {
    let tmpdir = common::temp_dir().unwrap();
    let config = solve::config::SQLiteConfig {
        path: tmpdir
            .join("db.sqlite")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string(),
    };
    let db: Arc<Database> = Arc::new(
        new_database(&solve::config::DatabaseConfig::SQLite(config))
            .unwrap()
            .into(),
    );
    db.execute(
        r#"CREATE TABLE "solve_file" (
            "id" INTEGER PRIMARY KEY,
            "status" INTEGER NOT NULL,
            "expire_time" BIGINT,
            "path" TEXT NOT NULL,
            "meta" BLOB NOT NULL
        )"#,
    )
    .await
    .unwrap();
    db.execute(
        r#"CREATE TABLE "solve_file_event" (
            "event_id" INTEGER PRIMARY KEY,
            "event_time" BIGINT NOT NULL,
            "event_kind" INTEGER NOT NULL,
            "event_account_id" INTEGER,
            "id" INTEGER NOT NULL,
            "status" INTEGER NOT NULL,
            "expire_time" BIGINT,
            "path" TEXT NOT NULL,
            "meta" BLOB NOT NULL
        )"#,
    )
    .await
    .unwrap();
    let store = FileStore::new(db.clone());
    {
        let object = File {
            id: 123,
            status: FileStatus::Available,
            expire_time: None,
            path: "path".into(),
            meta: serde_json::Value::Null,
        };
        let event = store.create(Context::new(), object).await.unwrap();
        assert_eq!(event.id(), 1);
        assert_eq!(event.kind(), EventKind::Create);
        assert_eq!(event.object().id, 1);
        assert_eq!(event.object().status, FileStatus::Available);
        assert_eq!(event.object().expire_time, None);
        assert_eq!(event.object().path, "path");
        assert_eq!(event.object().meta, serde_json::Value::Null);

        let mut object = event.object().clone();
        object.expire_time = Some(now());
        let event = store.update(Context::new(), object).await.unwrap();
        assert_eq!(event.id(), 2);
        assert_eq!(event.kind(), EventKind::Update);
        assert_eq!(event.object().id, 1);
        assert_eq!(event.object().status, FileStatus::Available);
        assert!(event.object().expire_time.is_some());
        assert_eq!(event.object().path, "path");
        assert_eq!(event.object().meta, serde_json::Value::Null);

        let event = store
            .delete(Context::new(), event.object().id)
            .await
            .unwrap();
        assert_eq!(event.id(), 3);
        assert_eq!(event.kind(), EventKind::Delete);
        assert_eq!(event.object().id, 1);
        assert_eq!(event.object().status, Default::default());
        assert_eq!(event.object().expire_time, Default::default());
        assert_eq!(event.object().path, "");
        assert_eq!(event.object().meta, serde_json::Value::Null);
    }
    {
        let mut tx = db.transaction(TransactionOptions::default()).await.unwrap();
        let event = store
            .create(Context::new().with_tx(&mut tx), Default::default())
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        assert!(store
            .update(Context::new(), event.object().clone())
            .await
            .is_err());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_task_store() {
    let tmpdir = common::temp_dir().unwrap();
    let config = solve::config::SQLiteConfig {
        path: tmpdir
            .join("db.sqlite")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string(),
    };
    let db: Arc<Database> = Arc::new(
        new_database(&solve::config::DatabaseConfig::SQLite(config))
            .unwrap()
            .into(),
    );
    db.execute(
        r#"CREATE TABLE "solve_task" (
            "id" INTEGER PRIMARY KEY,
            "kind" INTEGER NOT NULL,
            "config" BLOB NOT NULL,
            "status" INTEGER NOT NULL,
            "state" BLOB NOT NULL,
            "expire_time" BIGINT
        )"#,
    )
    .await
    .unwrap();
    db.execute(
        r#"CREATE TABLE "solve_task_event" (
            "event_id" INTEGER PRIMARY KEY,
            "event_time" BIGINT NOT NULL,
            "event_kind" INTEGER NOT NULL,
            "event_account_id" INTEGER,
            "id" INTEGER NOT NULL,
            "kind" INTEGER NOT NULL,
            "config" BLOB NOT NULL,
            "status" INTEGER NOT NULL,
            "state" BLOB NOT NULL,
            "expire_time" BIGINT
        )"#,
    )
    .await
    .unwrap();
    let store = TaskStore::new(db);
    {
        let object = Task {
            id: 123,
            ..Default::default()
        };
        let event = store.create(Context::new(), object).await.unwrap();
        assert_eq!(event.id(), 1);
        assert_eq!(event.kind(), EventKind::Create);
        assert_eq!(event.object().id, 1);

        let mut object = event.object().clone();
        object.expire_time = Some(now());
        let event = store.update(Context::new(), object).await.unwrap();
        assert_eq!(event.id(), 2);
        assert_eq!(event.kind(), EventKind::Update);
        assert_eq!(event.object().id, 1);

        let event = store
            .delete(Context::new(), event.object().id)
            .await
            .unwrap();
        assert_eq!(event.id(), 3);
        assert_eq!(event.kind(), EventKind::Delete);
        assert_eq!(event.object().id, 1);
    }
}
