use std::fs::remove_dir_all;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Uid;
use sbox::{run_as_root, BinNewIdMapper, Cgroup, Gid, InitProcess};
use tokio::task::{spawn_blocking, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::core::{blocking_await, Error};

use super::ProcessConfig;

pub struct Report {
    pub exit_code: i32,
    pub memory: u64,
    pub time: Duration,
    pub real_time: Duration,
}

pub struct Process {
    pub(super) config: ProcessConfig,
    pub(super) container: sbox::Container,
    pub(super) state_path: PathBuf,
    pub(super) user_mapper: BinNewIdMapper,
    pub(super) cgroup: Cgroup,
    pub(super) shutdown: Option<CancellationToken>,
    pub(super) join_handle: Option<JoinHandle<Result<Report, Error>>>,
}

impl Process {
    pub async fn start(&mut self) -> Result<(), Error> {
        if self.join_handle.is_some() {
            return Err("process already started".into());
        }
        let config = self.config.clone();
        let process = InitProcess::options()
            .command(self.config.command.clone())
            .environ(self.config.environ.clone())
            .work_dir(self.config.work_dir.clone())
            .user(Uid::from(0), Gid::from(0))
            .start(&self.container)
            .map_err(|err| format!("Cannot start process: {err}"))?;
        let shutdown = CancellationToken::new();
        self.shutdown = Some(shutdown.clone());
        self.join_handle = Some(spawn_blocking(move || Self::run(process, config, shutdown)));
        Ok(())
    }

    pub async fn wait(&mut self) -> Result<Report, Error> {
        match self.join_handle.take() {
            Some(v) => v.await?,
            None => Err("Process is not started".into()),
        }
    }

    fn run(
        process: InitProcess,
        config: ProcessConfig,
        shutdown: CancellationToken,
    ) -> Result<Report, Error> {
        let start_time = Instant::now();
        let deadline = start_time + config.real_time_limit;
        let pid = process.as_pid();
        let status = loop {
            match waitpid(pid, Some(WaitPidFlag::WNOHANG | WaitPidFlag::__WALL))? {
                WaitStatus::StillAlive => {
                    if shutdown.is_cancelled() {
                        kill(pid, Signal::SIGKILL)?;
                        break waitpid(pid, Some(WaitPidFlag::__WALL))?;
                    }
                    let current_time = Instant::now();
                    if current_time > deadline {
                        kill(pid, Signal::SIGKILL)?;
                        break waitpid(pid, Some(WaitPidFlag::__WALL))?;
                    }
                    sleep(Duration::from_micros(500));
                }
                status => break status,
            }
        };
        let exit_code = match status {
            WaitStatus::Exited(_, code) => code,
            WaitStatus::Signaled(_, signal, _) => signal as i32,
            _ => Err(format!("Unexpected wait status: {:?}", status))?,
        };
        let current_time = Instant::now();
        let mut time = Duration::ZERO;
        let mut real_time = current_time - start_time;
        if time > config.time_limit || real_time > config.real_time_limit {
            time = config.time_limit + Duration::from_millis(1);
            real_time = config.real_time_limit + Duration::from_millis(1);
        }
        Ok(Report {
            exit_code,
            memory: 0,
            time,
            real_time,
        })
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.cancel();
        }
        let _ = blocking_await(self.wait());
        let remove_state = {
            let state_path = self.state_path.clone();
            move || Ok(remove_dir_all(state_path)?)
        };
        let _ = run_as_root(&self.user_mapper, remove_state);
        let _ = self.cgroup.remove();
    }
}
