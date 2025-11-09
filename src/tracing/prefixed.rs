use tracing::{Event, Subscriber};
use tracing_subscriber::{
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    registry::LookupSpan,
};

use crate::tracing::session_metadata::SessionMetadata;

pub(super) struct Prefixed<F>(pub(super) F);

impl<S, N, F> FormatEvent<S, N> for Prefixed<F>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut w: Writer<'_>, event: &Event<'_>) -> std::fmt::Result {
        if let Some(prefix) = find_prefix(ctx) {
            write!(w, "{prefix}")?;
        }
        // Delegate to the wrapped formatter (keeps time/level/target etc.)
        self.0.format_event(ctx, w, event)
    }
}

fn find_prefix<S, N>(ctx: &FmtContext<'_, S, N>) -> Option<String>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    let current = ctx.lookup_current()?;
    // Search current → parents; return the first prefix we find.
    for span in current.scope() {
        if let Some(meta) = span.extensions().get::<SessionMetadata>() {
            return Some(meta.prefix.clone());
        }
    }
    None
}
