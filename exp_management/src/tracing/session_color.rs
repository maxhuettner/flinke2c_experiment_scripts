use std::{
    collections::hash_map,
    hash::{Hash, Hasher},
};

pub(super) struct SessionColor {
    pub(super) ansi_code: &'static str,
}

const PALETTE: &[SessionColor] = &[
    SessionColor { ansi_code: "34" }, // blue
    SessionColor { ansi_code: "35" }, // magenta
    SessionColor { ansi_code: "36" }, // cyan
    SessionColor { ansi_code: "33" }, // yellow
    SessionColor { ansi_code: "32" }, // green
];

pub(super) fn pick_color(session_id: &str) -> &'static SessionColor {
    let mut hasher = hash_map::DefaultHasher::new();
    session_id.hash(&mut hasher);
    let idx = (hasher.finish() as usize) % PALETTE.len();
    &PALETTE[idx]
}
