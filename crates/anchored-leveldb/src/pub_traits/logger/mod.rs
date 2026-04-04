mod logger_trait;
mod tracing_impl;

// This module is not very high-priority.

pub use self::{logger_trait::Logger, tracing_impl::TracingLogger};
