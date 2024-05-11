pub mod builder;

mod postgres;
mod sqlite;

use crate::{config::DatabaseConfig, core::Error};
use solve_db::Database;

pub fn new_database(config: &DatabaseConfig) -> Result<Database, Error> {
    let db = match config {
        DatabaseConfig::SQLite(config) => sqlite::WrapDatabase::new(config.path.clone()).into(),
        DatabaseConfig::Postgres(config) => postgres::WrapDatabase::new(config)?.into(),
    };
    Ok(db)
}
