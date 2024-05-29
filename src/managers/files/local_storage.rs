use std::path::{Path, PathBuf};

use crate::core::Error;

use super::{FileHandle, FileStorage};

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
        Ok(self.path.clone().join(key))
    }

    async fn free(&self, _key: &str, _value: PathBuf) {}

    async fn generate_key(&self) -> Result<String, Error> {
        todo!()
    }

    async fn upload(&self, _key: &str, _file: Box<dyn FileHandle>) -> Result<(), Error> {
        todo!()
    }
}
