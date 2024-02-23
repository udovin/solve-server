use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use path_clean::PathClean;

use crate::core::Error;

use super::Process;

#[derive(Clone, Default)]
pub struct ProcessConfig {
    pub command: Vec<String>,
    pub environ: Vec<String>,
    pub layers: Vec<PathBuf>,
    pub work_dir: PathBuf,
    pub time_limit: Duration,
    pub real_time_limit: Duration,
    pub memory_limit: u64,
}

pub struct Manager {
    #[allow(unused)]
    cgroup_path: PathBuf,
    #[allow(unused)]
    storage_path: PathBuf,
    manager: sbox::Manager,
    counter: AtomicI64,
}

impl Manager {
    pub fn new(
        storage_path: impl Into<PathBuf>,
        cgroup_path: impl Into<PathBuf>,
    ) -> Result<Self, Error> {
        let cgroup_path = cgroup_path.into();
        let cgroup_path = if cgroup_path.is_absolute() {
            PathBuf::from("/sys/fs/cgroup").join(cgroup_path.strip_prefix("/")?)
        } else {
            Self::get_current_cgroup()?.join(cgroup_path)
        };
        let cgroup_path = cgroup_path.clean();
        assert!(cgroup_path.starts_with("/sys/fs/cgroup/"));
        let storage_path = storage_path.into().clean();
        assert!(storage_path.is_absolute());
        Self::setup_cgroup(&cgroup_path).map_err(|err| format!("cannot setup cgroup: {}", err))?;
        std::fs::create_dir_all(&storage_path)?;
        let user_mapper = sbox::NewIdMap::default();
        let manager = sbox::Manager::new(&storage_path, &cgroup_path, user_mapper)
            .map_err(|v| v.to_string())?;
        Ok(Self {
            cgroup_path,
            storage_path,
            manager,
            counter: AtomicI64::new(0),
        })
    }

    pub fn process(&self, config: ProcessConfig) -> Result<Process, Error> {
        let name = self.counter.fetch_add(1, Ordering::SeqCst).to_string();
        let container = self
            .manager
            .create_container(
                format!("safeexec-{name}"),
                sbox::ContainerConfig {
                    layers: config.layers.clone(),
                    hostname: "safeexec".into(),
                    ..Default::default()
                },
            )
            .map_err(|v| v.to_string())?;
        Ok(Process {
            config,
            container: Some(container),
            join_handle: None,
        })
    }

    fn setup_cgroup(cgroup_path: &Path) -> Result<(), Error> {
        if let Err(err) = std::fs::create_dir(cgroup_path) {
            if err.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(err.into());
            }
        };
        let content = std::fs::read(cgroup_path.join("cgroup.controllers"))?;
        let mut subtree_file = File::options()
            .write(true)
            .open(cgroup_path.join("cgroup.subtree_control"))?;
        for line in content.split(|c| *c == b'\n').filter(|v| !v.is_empty()) {
            let line = std::str::from_utf8(line)?;
            let data = line
                .split(|c| c == ' ')
                .fold("".to_owned(), |acc, v| acc + " +" + v);
            subtree_file.write_all(data.as_bytes())?;
        }
        Ok(())
    }

    fn get_current_cgroup() -> Result<PathBuf, Error> {
        let content = std::fs::read("/proc/self/cgroup")?;
        for line in content.split(|c| *c == b'\n').filter(|v| !v.is_empty()) {
            let line = std::str::from_utf8(line)?;
            let parts: Vec<_> = line.split(|c| c == ':').collect();
            if parts.len() < 3 {
                return Err(format!("invalid cgroup line: {}", line).into());
            }
            if parts[1].is_empty() {
                let name = parts[2].trim_start_matches('/');
                return Ok(PathBuf::from("/sys/fs/cgroup").join(name));
            }
        }
        Err("cannot find cgroup path".into())
    }
}
