use tracing::{Span, info_span, level_filters::LevelFilter};
use tracing_subscriber::{
    EnvFilter, Registry,
    fmt::{format::Format, layer, time::SystemTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

use crate::tracing::{prefixed::Prefixed, session_layer::SessionLayer};

mod prefixed;
mod session_color;
mod session_id_visitor;
mod session_layer;
mod session_metadata;

pub const SESSION_FIELD: &str = "session.id";

pub fn init_tracing(level: LevelFilter) {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::OFF.into()) // silence dependencies
        .parse_lossy(format!("network_sim={level}"));

    let base_fmt = Format::default().with_timer(SystemTime);

    Registry::default()
        .with(filter)
        .with(SessionLayer)
        .with(layer().event_format(Prefixed(base_fmt)))
        .try_init()
        .unwrap();
}

pub fn session_info_span(session_id: &str) -> Span {
    info_span!("ssh_session", session.id = %session_id)
}
