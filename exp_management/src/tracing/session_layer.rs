use tracing::{Subscriber, span};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use crate::tracing::{session_id_visitor::SessionIdVisitor, session_metadata::SessionMetadata};

pub(super) struct SessionLayer;

impl<S> Layer<S> for SessionLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };

        let mut visitor = SessionIdVisitor::default();
        attrs.record(&mut visitor);

        let Some(session_id) = visitor.session_id else {
            return;
        };

        let metadata = SessionMetadata::new(&session_id);
        span.extensions_mut().insert(metadata);
    }
}
