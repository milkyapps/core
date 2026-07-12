//! Worker thread loop for the async runtime.

use std::sync::Arc;
use std::thread;

use crate::async_rt::queue::Queue;

/// Spawns a worker thread that pulls tasks from the queue until shutdown.
pub(crate) fn spawn(queue: Arc<Queue>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while let Some(task) = queue.pop() {
            task.run();
        }
    })
}
