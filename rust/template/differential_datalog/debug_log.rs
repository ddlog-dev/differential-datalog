//! An implementation of the debugger interface that dumps events to the log.

use debug::*;
use log;

pub struct LogDebugger {
    level: log::Level,
}

impl LogDebugger {
    pub fn new(level: log::Level) -> LogDebugger {
        LogDebugger { level }
    }
}

impl IDebugger for LogDebugger {
    fn name(&self) -> &str {
        &"log debugger"
    }
    fn event(&self, e: DebugEvent) {
        log::log!(self.level, "{:?}", e)
    }
}
