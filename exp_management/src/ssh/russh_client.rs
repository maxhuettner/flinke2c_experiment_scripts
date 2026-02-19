use russh::{client::Handler, keys::PublicKey};

pub struct RusshClient {}

impl Handler for RusshClient {
    type Error = russh::Error;

    async fn check_server_key(&mut self, _server_public_key: &PublicKey) -> Result<bool, Self::Error> {
        Ok(true)
    }
}
