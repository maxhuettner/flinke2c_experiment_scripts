use std::sync::Arc;

use russh::{
    Channel, ChannelMsg, Sig,
    client::{Handle, Msg},
};
use tokio::{
    select,
    sync::mpsc::{self},
    task::{self, JoinHandle},
};
use tokio_util::sync::CancellationToken;

use crate::ssh::client::Client;

pub enum SessionCommand {
    Data(String),
    Signal(Sig),
}

pub struct CommSession {
    cancellation_token: Arc<CancellationToken>,
    join_handle: JoinHandle<anyhow::Result<()>>,
    command_tx: mpsc::UnboundedSender<SessionCommand>,
    response_rx: async_channel::Receiver<String>,
}

impl CommSession {
    pub async fn start_from_handle(handle: &Handle<Client>) -> anyhow::Result<Self> {
        let cancellation_token = Arc::new(CancellationToken::new());
        let (command_tx, command_rx) = mpsc::unbounded_channel::<SessionCommand>();
        let (response_tx, response_rx) = async_channel::unbounded::<String>();
        let channel = handle.channel_open_session().await?;

        let join_handle = task::spawn(Self::communication_task(
            channel,
            cancellation_token.clone(),
            command_rx,
            response_tx.clone(),
        ));

        Ok(Self {
            cancellation_token,
            join_handle,
            command_tx,
            response_rx,
        })
    }

    pub fn command_tx(&self) -> mpsc::UnboundedSender<SessionCommand> {
        self.command_tx.clone()
    }

    pub fn response_rx(&self) -> async_channel::Receiver<String> {
        self.response_rx.clone()
    }

    async fn communication_task(
        mut channel: Channel<Msg>,
        cancellation_token: Arc<CancellationToken>,
        mut rx: mpsc::UnboundedReceiver<SessionCommand>,
        tx: async_channel::Sender<String>,
    ) -> anyhow::Result<()> {
        loop {
            let msg = select! {
                    msg = channel.wait() => {
                    msg
                }
                _ = tx.closed() => {
                    None
                }
                cmd = rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            match cmd {
                                SessionCommand::Data(data) => {
                                    channel.exec(true, data.into_bytes()).await?;
                                },
                                SessionCommand::Signal(sig) => {
                                    channel.signal(sig).await?;
                                }
                            };
                            continue;
                        }
                        None => {
                            None
                        }
                    }
                }
                _ = cancellation_token.cancelled() => {
                    None
                }
            };
            let Some(msg) = msg else {
                break;
            };
            match msg {
                ChannelMsg::Data { ref data } => {
                    tx.send(String::from_utf8(data.to_vec())?).await?;
                }
                ChannelMsg::ExtendedData { ref data, ext: _ } => {
                    tx.send(String::from_utf8(data.to_vec())?).await?;
                }
                ChannelMsg::ExitStatus { exit_status } => {
                    println!("Exited {}", exit_status)
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub async fn stop(self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.response_rx.close();
        self.join_handle.await??;
        Ok(())
    }
}
