use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Command {
    #[serde(rename = "cmd")]
    Cmd { cmd: String },

    #[serde(rename = "sftp_create_dir")]
    SftpCreateDir { dir: String },
}
