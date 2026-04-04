use super::logger_trait::Logger;


#[derive(Debug, Clone, Copy)]
pub struct TracingLogger;

impl Logger for TracingLogger {}
