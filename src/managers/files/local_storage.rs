use hashing_reader::HashingReader;
use md5::Digest as _;
use rand::Rng as _;
use std::fmt::Write as _;
use std::io::Seek as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::task::block_in_place;

use crate::core::Error;

use super::{FileInfo, FileStorage, UploadResult};

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

fn to_hex(bytes: Vec<u8>) -> Result<String, Error> {
    let mut s = String::new();
    for v in bytes {
        write!(&mut s, "{:x}", v)?;
    }
    Ok(s)
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

    async fn upload(&self, key: &str, file: Pin<Box<dyn FileInfo>>) -> Result<UploadResult, Error> {
        let key = key.replace('/', std::path::MAIN_SEPARATOR_STR);
        if key.is_empty() {
            Err("Key cannot be empty")?
        }
        if let Some(file_path) = file.path() {
            let mut file = block_in_place(|| std::fs::File::open(&file_path))?;
            let md5 = {
                let mut hash = md5::Md5::new();
                block_in_place(|| std::io::copy(&mut file, &mut hash))?;
                to_hex(hash.finalize().to_vec())?
            };
            block_in_place(|| file.seek(std::io::SeekFrom::Start(0)))?;
            let sha3_224 = {
                let mut hash = sha3::Sha3_224::new();
                block_in_place(|| std::io::copy(&mut file, &mut hash))?;
                to_hex(hash.finalize().to_vec())?
            };
            block_in_place(|| file.seek(std::io::SeekFrom::Start(0)))?;
            let mut storage_file = block_in_place(|| std::fs::File::create(self.path.join(key)))?;
            let size = block_in_place(|| std::io::copy(&mut file, &mut storage_file))?;
            block_in_place(|| storage_file.sync_all())?;
            Ok(UploadResult {
                size,
                md5,
                sha3_224,
            })
        } else {
            let file = file.into_reader();
            let (file, md5_hash) = HashingReader::<_, md5::Md5>::new(file);
            let (mut file, sha3_hash) = HashingReader::<_, sha3::Sha3_224>::new(file);
            let mut storage_file = block_in_place(|| std::fs::File::create(self.path.join(key)))?;
            let size = block_in_place(|| std::io::copy(&mut file, &mut storage_file))?;
            block_in_place(|| storage_file.sync_all())?;
            let md5 = to_hex(block_in_place(|| md5_hash.recv())?.unwrap())?;
            let sha3_224 = to_hex(block_in_place(|| sha3_hash.recv())?.unwrap())?;
            Ok(UploadResult {
                size,
                md5,
                sha3_224,
            })
        }
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
