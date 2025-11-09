pub mod client;
pub mod comm_session;
pub mod session;

use std::{env, path::PathBuf, sync::Arc};

use russh::{
    client::{Config, Handle},
    keys::{PrivateKeyWithHashAlg, load_secret_key},
};

use crate::ssh::{client::Client, session::Session};

fn resolve_key_path(key_path: &str) -> anyhow::Result<PathBuf> {
    if let Some(stripped) = key_path.strip_prefix("~/") {
        let home = env::var("HOME")?;
        Ok(PathBuf::from(home).join(stripped))
    } else {
        Ok(PathBuf::from(key_path))
    }
}

async fn auth_with_key(
    handle: &mut Handle<Client>,
    user: &str,
    pass_phrase: Option<&str>,
    key_path: &str,
) -> anyhow::Result<()> {
    let key_path = resolve_key_path(key_path)?;
    let key = load_secret_key(&key_path, pass_phrase)?;

    let hash = handle.best_supported_rsa_hash().await?.flatten();

    let is_auth_successful = handle
        .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash))
        .await?
        .success();

    anyhow::ensure!(is_auth_successful, "authentication failed");
    Ok(())
}

// TODO: Move inside Client
pub async fn connect(address: &str, user: &str, pass_phrase: Option<&str>, key_path: &str) -> anyhow::Result<Session> {
    let config = Arc::new(Config::default());

    let handle = russh::client::connect(config, address, Client {}).await.unwrap();
    let mut session = Session::from_handle(handle, address);
    auth_with_key(session.handle_mut(), user, pass_phrase, key_path).await?;

    Ok(session)
}
