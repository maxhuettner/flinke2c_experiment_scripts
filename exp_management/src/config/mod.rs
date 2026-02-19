pub mod command;

pub enum Action {
    Cmd { cmd: String },
    SftpCreateDir { dir: String },
}
