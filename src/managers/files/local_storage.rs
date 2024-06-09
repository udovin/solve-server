use rand::Rng;
use std::fmt::Write;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::core::Error;

use super::{FileInfo, FileStorage};

pub struct LocalStorage {
    path: PathBuf,
}

impl LocalStorage {
    pub fn new(path: &Path) -> Result<Self, Error> {
        Ok(Self {
            path: path.to_owned(),
        })
    }
}

#[async_trait::async_trait]
impl FileStorage for LocalStorage {
    async fn load(&self, key: &str) -> Result<PathBuf, Error> {
        let key = key.replace('/', std::path::MAIN_SEPARATOR_STR);
        if key.is_empty() {
            Err("Key cannot be empty")?
        }
        Ok(self.path.clone().join(key))
    }

    async fn free(&self, _key: &str, _value: PathBuf) {}

    async fn generate_key(&self) -> Result<String, Error> {
        let rand_bytes = rand::thread_rng().gen::<[u8; 8]>();
        let time_bytes = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
            .to_le_bytes();
        let mut key = String::new();
        for v in &rand_bytes[..2] {
            write!(&mut key, "{:x}", v)?;
        }
        write!(&mut key, "/")?;
        for v in &rand_bytes[2..] {
            write!(&mut key, "{:x}", v)?;
        }
        for v in time_bytes {
            write!(&mut key, "{:x}", v)?;
        }
        Ok(key)
    }

    async fn upload(&self, key: &str, mut file: FileInfo) -> Result<(), Error> {
        let key = key.replace('/', std::path::MAIN_SEPARATOR_STR);
        if key.is_empty() {
            Err("Key cannot be empty")?
        }
        if let Some(file_path) = file.path() {
            tokio::fs::copy(file_path, self.path.join(key)).await?;
        } else {
            let mut storage_file = File::create(self.path.join(key)).await?;
            tokio::io::copy(&mut file, &mut storage_file).await?;
            storage_file.flush().await?;
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        let key = key.replace('/', std::path::MAIN_SEPARATOR_STR);
        if key.is_empty() {
            Err("Key cannot be empty")?
        }
        tokio::fs::remove_dir_all(self.path.join(key)).await?;
        Ok(())
    }
}
