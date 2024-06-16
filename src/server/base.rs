use std::sync::Arc;

use crate::{
    config,
    core::{Core, Error},
};

pub struct Server {
    core: Arc<Core>,
}

impl Server {
    pub fn new(core: Arc<Core>, config: &config::Server) -> Result<Self, Error> {
        Ok(Self { core })
    }
}
