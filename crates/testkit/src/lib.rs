//! Shared test infrastructure for FluxFS.
//!
//! This crate intentionally does not depend on a concrete metadata or chunk
//! engine. Tests exercise protocol invariants through owned commands so the
//! same harness can later target an in-process adapter or tonic transport.

pub mod benchmark;
pub mod failpoints;
pub mod model;
