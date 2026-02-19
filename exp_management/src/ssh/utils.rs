use std::{env, path::PathBuf};

use russh::keys::{PrivateKey, load_secret_key};

pub fn load_private_key(key_path: &str, pass_phrase: Option<&str>) -> anyhow::Result<PrivateKey> {
    let key_path = resolve_key_path(key_path)?;
    let key = load_secret_key(&key_path, pass_phrase)?;

    Ok(key)
}

pub fn resolve_key_path(key_path: &str) -> anyhow::Result<PathBuf> {
    if let Some(stripped) = key_path.strip_prefix("~/") {
        let home = env::var("HOME")?;
        Ok(PathBuf::from(home).join(stripped))
    } else {
        Ok(PathBuf::from(key_path))
    }
}
