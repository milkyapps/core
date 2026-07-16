//! Worker thread loop for the async runtime.

use crate::{async_rt::task::Task, sync::bounded::Receiver};
use std::sync::Arc;

/// Spawns a worker thread that pulls tasks from the queue until shutdown.
pub(crate) fn spawn(rx: Receiver<Arc<Task>>) -> crate::thread::JoinHandle<()> {
    crate::thread::spawn(move || {
        while let Some(task) = rx.recv() {
            task.run();
        }
    })
}
