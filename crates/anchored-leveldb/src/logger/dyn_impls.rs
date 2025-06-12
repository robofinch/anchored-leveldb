use std::{rc::Rc, sync::Arc};

use log::Level;

use super::Logger;


impl Logger for Box<dyn Logger> {
    fn log(&self, level: Level, msg: &str) {
        self.as_ref().log(level, msg);
    }
}

impl Logger for Rc<dyn Logger> {
    fn log(&self, level: Level, msg: &str) {
        self.as_ref().log(level, msg);
    }
}

impl Logger for Arc<dyn Logger> {
    fn log(&self, level: Level, msg: &str) {
        self.as_ref().log(level, msg);
    }
}
