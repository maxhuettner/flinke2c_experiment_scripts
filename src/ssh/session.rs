use russh::client::Handle;

use crate::ssh::{
    client::Client,
    comm_session::CommSession,
};

pub struct Session {
    handle: Handle<Client>,
}

impl Session {
    pub async fn from_handle(handle: Handle<Client>) -> Self {
        Self { handle }
    }

    pub async fn start_session(&self) -> anyhow::Result<CommSession> {
        let comm_session = CommSession::start_from_handle(&self.handle).await?;
        Ok(comm_session)
    }

    pub fn handle(&self) -> &Handle<Client> {
        &self.handle
    }

    pub fn handle_mut(&mut self) -> &mut Handle<Client> {
        &mut self.handle
    }
}
