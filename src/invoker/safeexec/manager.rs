use std::fs::{create_dir, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use path_clean::PathClean;
use sbox::{BaseMounts, BinNewIdMapper, Cgroup, Container, Gid, OverlayMount, Uid};

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
    storage_path: PathBuf,
    user_mapper: BinNewIdMapper,
    cgroup: Cgroup,
    counter: AtomicI64,
}

const CGROUP_FS_PATH: &str = "/sys/fs/cgroup";

impl Manager {
    pub fn new(
        storage_path: impl Into<PathBuf>,
        cgroup_path: impl Into<PathBuf>,
    ) -> Result<Self, Error> {
        let cgroup_path = cgroup_path.into();
        let cgroup = if cgroup_path.is_absolute() {
            Cgroup::new(CGROUP_FS_PATH, cgroup_path.strip_prefix("/")?)?
        } else {
            // We use the parent cgroup of the current process because we cannot
            // create a child cgroup in a cgroup with any attached process.
            Cgroup::current()?
                .parent()
                .ok_or("Cannot get parent cgroup")?
                .child(cgroup_path)?
        };
        cgroup
            .create()
            .map_err(|err| format!("Cannot create cgroup: {err}"))?;
        let storage_path = storage_path.into().clean();
        assert!(storage_path.is_absolute());
        Self::setup_cgroup(cgroup.as_path())
            .map_err(|err| format!("cannot setup cgroup: {}", err))?;
        std::fs::create_dir_all(&storage_path)?;
        let user_mapper = BinNewIdMapper::new_root_subid(Uid::current(), Gid::current()).unwrap();
        Ok(Self {
            storage_path,
            user_mapper,
            cgroup,
            counter: AtomicI64::new(0),
        })
    }

    pub fn create_process(&self, config: ProcessConfig) -> Result<Process, Error> {
        let name = self.counter.fetch_add(1, Ordering::SeqCst).to_string();
        let state_path = self.storage_path.join(format!("sandbox-{name}"));
        create_dir(&state_path)?;
        let upper_path = state_path.join("upper");
        create_dir(&upper_path)?;
        let work_path = state_path.join("work");
        create_dir(&work_path)?;
        let rootfs = state_path.join("rootfs");
        create_dir(&rootfs)?;
        let cgroup = self.cgroup.child(format!("sandbox-{name}"))?;
        cgroup.create()?;
        let user_mapper = self.user_mapper.clone();
        let container = Container::options()
            .user_mapper(user_mapper.clone())
            .cgroup(cgroup.clone())
            .add_mount(OverlayMount::new(
                config.layers.clone(),
                upper_path,
                work_path,
            ))
            .add_mount(BaseMounts::new())
            .rootfs(rootfs)
            .hostname("sandbox")
            .create()?;
        Ok(Process {
            config,
            container,
            state_path,
            user_mapper,
            cgroup,
            shutdown: None,
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
}
