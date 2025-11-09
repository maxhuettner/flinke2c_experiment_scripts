use russh::client::Handle;
use tracing::{Instrument, Span, info_span};

use crate::{
    ssh::{client::Client, comm_session::CommSession},
    tracing::session_info_span,
};

pub struct Session {
    handle: Handle<Client>,
    span: Span,
}

impl Session {
    pub fn from_handle(handle: Handle<Client>, session_id: &str) -> Self {
        let span = session_info_span(session_id);

        Self { handle, span }
    }

    pub async fn start_session(&self) -> anyhow::Result<CommSession> {
        let comm_session = CommSession::start_from_handle(&self.handle)
            .instrument(self.span.clone())
            .await?;
        Ok(comm_session)
    }

    pub fn handle(&self) -> &Handle<Client> {
        &self.handle
    }

    pub fn handle_mut(&mut self) -> &mut Handle<Client> {
        &mut self.handle
    }
}
