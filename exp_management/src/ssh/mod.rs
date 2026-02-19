pub mod comm_session;
pub mod russh_client;
pub mod utils;

use std::sync::Arc;

use russh::{
    client::{Config, Handle},
    keys::PrivateKeyWithHashAlg,
};
use russh_sftp::client::SftpSession;
use tracing::{Instrument, Span};

use crate::{
    ssh::{comm_session::CommSession, russh_client::RusshClient, utils::load_private_key},
    tracing::session_info_span,
};

pub struct Client {
    handle: Handle<RusshClient>,
    span: Span,
}

impl Client {
    pub async fn connect(address: &str, user: &str, key_path: &str, pass_phrase: Option<&str>) -> anyhow::Result<Self> {
        let config = Arc::new(Config::default());
        let span = session_info_span(address);

        let mut handle = russh::client::connect(config, address, RusshClient {}).await.unwrap();

        let hash_alg = handle.best_supported_rsa_hash().await?.flatten();
        let key = load_private_key(key_path, pass_phrase)?;

        handle
            .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg))
            .await?;

        Ok(Self { handle, span })
    }

    pub async fn start_sftp_session(&self) -> anyhow::Result<SftpSession> {
        let channel = self.handle.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        let session = SftpSession::new(channel.into_stream()).await?;

        Ok(session)
    }

    pub async fn start_session(&self) -> anyhow::Result<CommSession> {
        let channel = self.handle.channel_open_session().await?;
        let comm_session = CommSession::start_from_channel(channel)
            .instrument(self.span.clone())
            .await?;
        Ok(comm_session)
    }

    pub fn handle(&self) -> &Handle<RusshClient> {
        &self.handle
    }

    pub fn handle_mut(&mut self) -> &mut Handle<RusshClient> {
        &mut self.handle
    }
}
