use tracing::field::Visit;

use crate::tracing::SESSION_FIELD;

#[derive(Default)]
pub(super) struct SessionIdVisitor {
    pub(super) session_id: Option<String>,
}

impl Visit for SessionIdVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == SESSION_FIELD {
            self.session_id = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == SESSION_FIELD {
            self.session_id = Some(format!("{value:?}"));
        }
    }
}
