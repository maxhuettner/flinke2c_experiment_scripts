use russh::{ChannelMsg, client::Handle};
use tokio::{
    select,
    sync::mpsc::{self, UnboundedReceiver},
    task::{self},
};

use crate::ssh::client::Client;

pub struct Session {
    handle: Handle<Client>,
}

impl Session {
    pub async fn from_handle(handle: Handle<Client>) -> Self {
        Self { handle }
    }

    pub async fn send_command(&self, command: &str) -> anyhow::Result<UnboundedReceiver<String>> {
        let mut channel = self.handle.channel_open_session().await?;
        let (tx, rx) = mpsc::unbounded_channel::<String>();

        channel.exec(true, command).await?;
        task::spawn(async move {
            loop {
                let msg = select! {
                        msg = channel.wait() => {
                        msg
                    }
                    _ = tx.closed() => {
                        None
                    }
                };
                let Some(msg) = msg else {
                    break;
                };
                match msg {
                    ChannelMsg::Data { ref data } => {
                        tx.send(String::from_utf8(data.to_vec()).unwrap()).unwrap();
                    }
                    ChannelMsg::ExtendedData { ref data, ext: _ } => {
                        tx.send(String::from_utf8(data.to_vec()).unwrap()).unwrap();
                    }
                    ChannelMsg::ExitStatus { exit_status } => {
                        println!("Exited {}", exit_status)
                    }
                    _ => {}
                }
            }
        });

        Ok(rx)
    }

    pub fn handle(&self) -> &Handle<Client> {
        &self.handle
    }

    pub fn handle_mut(&mut self) -> &mut Handle<Client> {
        &mut self.handle
    }
}
