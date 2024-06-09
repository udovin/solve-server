use std::env::var;
use std::path::{Path, PathBuf};

use gtmpl::{Context, FuncError, Value};
use serde::{Deserialize, Serialize};

use crate::core::Error;

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub db: DatabaseConfig,
    #[serde(default)]
    pub socket_file: Option<PathBuf>,
    #[serde(default)]
    pub server: Option<Server>,
    #[serde(default)]
    pub invoker: Option<Invoker>,
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    #[serde(default)]
    pub security: Option<Security>,
    #[serde(default)]
    pub smtp: Option<SMTP>,
    #[serde(default)]
    pub log_level: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Server {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u32,
    #[serde(default)]
    pub site_url: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Invoker {
    #[serde(default)]
    pub workers: u32,
    #[serde(default)]
    pub temp_dir: PathBuf,
    #[serde(default)]
    pub safeexec: Option<Safeexec>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Safeexec {
    pub path: PathBuf,
    #[serde(default)]
    pub cgroup: String,
    #[serde(default)]
    pub disable_memory_peak: bool,
    #[serde(default)]
    pub disable_cpu_limit: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "driver", content = "options")]
pub enum DatabaseConfig {
    #[serde(rename = "sqlite")]
    SQLite(SQLiteConfig),
    #[serde(rename = "postgres")]
    Postgres(PostgresConfig),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SQLiteConfig {
    #[serde(default)]
    pub path: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    #[serde(default)]
    pub hosts: Vec<String>,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub sslmode: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "driver", content = "options")]
pub enum StorageConfig {
    #[serde(rename = "local")]
    Local(LocalStorageConfig),
    #[serde(rename = "s3")]
    S3(S3StorageConfig),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LocalStorageConfig {
    #[serde(default)]
    pub files_dir: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct S3StorageConfig {
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub access_key_id: String,
    #[serde(default)]
    pub secret_access_key: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub path_prefix: String,
    #[serde(default)]
    pub use_path_style: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Security {
    #[serde(default)]
    pub password_salt: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SMTP {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u32,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub name: String,
}

pub fn parse_str(data: &str) -> Result<Config, Error> {
    let mut tmpl = gtmpl::Template::default();
    tmpl.add_func("env", tmpl_env);
    tmpl.add_func("file", tmpl_file);
    tmpl.add_func("json", tmpl_json);
    tmpl.parse(data)?;
    let result = tmpl.render(&Context::empty())?;
    Ok(serde_json::from_str(&result)?)
}

pub fn parse_file(path: impl AsRef<Path>) -> Result<Config, Error> {
    let data = std::fs::read_to_string(path)?;
    parse_str(&data)
}

fn tmpl_env(args: &[Value]) -> Result<Value, FuncError> {
    if args.len() != 1 {
        return Err(FuncError::ExactlyXArgs("env".into(), 1));
    }
    let key = match &args[0] {
        Value::String(v) => v,
        _ => return Err(FuncError::UnableToConvertFromValue),
    };
    let value = var(key).map_err(|e| FuncError::Generic(e.to_string()))?;
    Ok(Value::String(value.trim().into()))
}

fn tmpl_file(args: &[Value]) -> Result<Value, FuncError> {
    if args.len() != 1 {
        return Err(FuncError::ExactlyXArgs("file".into(), 1));
    }
    let path = PathBuf::from(match &args[0] {
        Value::String(v) => v,
        _ => return Err(FuncError::UnableToConvertFromValue),
    });
    let value = std::fs::read_to_string(path).map_err(|e| FuncError::Generic(e.to_string()))?;
    Ok(Value::String(value.trim().into()))
}

fn tmpl_json(args: &[Value]) -> Result<Value, FuncError> {
    if args.len() != 1 {
        return Err(FuncError::ExactlyXArgs("json".into(), 1));
    }
    let value = match &args[0] {
        Value::String(v) => serde_json::Value::from(v.as_str()),
        Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                serde_json::Value::from(v)
            } else if let Some(v) = v.as_u64() {
                serde_json::Value::from(v)
            } else if let Some(v) = v.as_f64() {
                serde_json::Value::from(v)
            } else {
                serde_json::Value::Null
            }
        }
        Value::Bool(v) => serde_json::Value::from(*v),
        Value::Nil => serde_json::Value::Null,
        _ => return Err(FuncError::UnableToConvertFromValue),
    };
    Ok(Value::String(value.to_string()))
}
