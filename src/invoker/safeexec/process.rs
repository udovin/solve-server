use std::thread::{Builder, JoinHandle};
use std::time::Duration;

use crate::core::Error;

use super::ProcessConfig;

pub struct Report {
    pub exit_code: i32,
    pub memory: u64,
    pub time: Duration,
    pub real_time: Duration,
}

pub struct Process {
    pub(super) name: String,
    #[allow(unused)]
    pub(super) config: ProcessConfig,
    pub(super) container: Option<sbox::Container>,
    pub(super) join_handle: Option<JoinHandle<Result<Report, Error>>>,
}

impl Process {
    pub fn start(&mut self) -> Result<(), Error> {
        if self.join_handle.is_some() {
            return Err("process already started".into());
        }
        let process = self
            .container
            .as_mut()
            .unwrap()
            .start(sbox::ProcessConfig {
                command: self.config.command.clone(),
                environ: self.config.environ.clone(),
                work_dir: self.config.work_dir.clone(),
                ..Default::default()
            })
            .map_err(|v| v.to_string())?;
        let builder = Builder::new().name("safeexec:".to_owned() + &self.name);
        let config = self.config.clone();
        self.join_handle = Some(builder.spawn(move || Self::run(process, config))?);
        Ok(())
    }

    pub fn wait(&mut self) -> Result<Report, Error> {
        let join_handle = match self.join_handle.take() {
            Some(v) => v,
            None => return Err("process is not started".into()),
        };
        join_handle.join().unwrap()
    }

    #[allow(unused)]
    fn run(process: sbox::Process, config: ProcessConfig) -> Result<Report, Error> {
        let status = process.wait(None)?;
        todo!()
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            container.destroy().unwrap();
        }
    }
}
