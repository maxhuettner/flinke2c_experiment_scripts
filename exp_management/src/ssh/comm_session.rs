use std::{sync::Arc, time::Duration};

use russh::{Channel, ChannelMsg, Sig, client::Msg};
use tokio::{
    select,
    sync::mpsc::{self},
    task::{self, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, info};

pub enum SessionCommand {
    Data(String),
    Signal(Sig),
}

pub struct CommandResponseChannels {
    pub commands: mpsc::UnboundedSender<SessionCommand>,
    pub responses: async_channel::Receiver<String>,
}

impl CommandResponseChannels {
    pub fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            responses: self.responses.clone(),
        }
    }

    pub fn close(&self) {
        self.responses.close();
    }

    pub async fn send_blocking(&self, command: SessionCommand, timeout: Option<Duration>) -> anyhow::Result<String> {
        self.commands.send(command)?;
        if let Some(dur) = timeout {
            let resp = tokio::time::timeout(dur, self.responses.recv()).await??;
            Ok(resp)
        } else {
            let resp = self.responses.recv().await?;
            Ok(resp)
        }
    }
}

pub struct CommSession {
    cancellation_token: Arc<CancellationToken>,
    join_handle: JoinHandle<anyhow::Result<()>>,
    command_response_channels: CommandResponseChannels,
}

impl CommSession {
    pub async fn start_from_channel(channel: Channel<Msg>) -> anyhow::Result<Self> {
        let cancellation_token = Arc::new(CancellationToken::new());
        let (command_tx, command_rx) = mpsc::unbounded_channel::<SessionCommand>();
        let (response_tx, response_rx) = async_channel::unbounded::<String>();
        let session_span = Span::current();

        let join_handle = task::spawn(
            Self::communication_task(channel, cancellation_token.clone(), command_rx, response_tx)
                .instrument(session_span),
        );

        Ok(Self {
            cancellation_token,
            join_handle,
            command_response_channels: CommandResponseChannels {
                commands: command_tx,
                responses: response_rx,
            },
        })
    }

    pub async fn send_command_blocking(
        &self,
        command: SessionCommand,
        timeout: Option<Duration>,
    ) -> anyhow::Result<String> {
        self.command_response_channels.send_blocking(command, timeout).await
    }

    pub fn send_command(&self, command: SessionCommand) -> anyhow::Result<()> {
        self.command_response_channels.commands.send(command)?;
        Ok(())
    }

    pub fn command_response_channels(&self) -> CommandResponseChannels {
        self.command_response_channels.clone()
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
                    info!("data");
                    tx.send(String::from_utf8_lossy(data).to_string()).await?;
                }
                ChannelMsg::ExtendedData { ref data, ext: _ } => {
                    info!("extended data");
                    tx.send(String::from_utf8_lossy(data).to_string()).await?;
                }
                ChannelMsg::ExitStatus { exit_status } => {
                    info!("Exited {}", exit_status)
                }
                ChannelMsg::ExitSignal {
                    signal_name,
                    core_dumped: _,
                    error_message: _,
                    lang_tag: _,
                } => {
                    info!("Exit signal {:?}", signal_name)
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub async fn stop(self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.command_response_channels.close();
        self.join_handle.await??;
        Ok(())
    }
}
