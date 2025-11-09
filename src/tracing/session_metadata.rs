use crate::tracing::session_color::pick_color;

pub(super) struct SessionMetadata {
    pub(super) prefix: String,
}

impl SessionMetadata {
    pub(super) fn new(session_id: &str) -> Self {
        let color = pick_color(session_id);
        let prefix = format!("\u{001b}[{}m[{session_id}]\u{001b}[0m ", color.ansi_code);
        Self { prefix }
    }
}
