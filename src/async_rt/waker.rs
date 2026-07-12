//! Waker implementation for async runtime tasks.

use std::sync::Arc;
use std::task::{RawWaker, RawWakerVTable, Waker};

use crate::async_rt::task::Task;

/// `RawWakerVTable` for task wakers.
///
/// Each waker holds a strong reference to its [`Task`]. Cloning increments the
/// reference count, dropping decrements it, and waking schedules the task
/// back onto the runtime queue.
static VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_task, wake_task, wake_task_by_ref, drop_task);

/// Creates a [`Waker`] from a task reference.
pub(crate) fn waker_from_task(task: Arc<Task>) -> Waker {
    let raw = Arc::into_raw(task);
    // SAFETY: `raw` came from a valid `Arc<Task>` and the vtable functions
    // correctly interpret it as such.
    unsafe { Waker::from_raw(RawWaker::new(raw.cast::<()>(), &VTABLE)) }
}

/// Clones the task reference held by a waker.
unsafe fn clone_task(data: *const ()) -> RawWaker {
    let task = data.cast::<Task>();
    // SAFETY: `data` is a valid `Arc<Task>` pointer from the vtable.
    let arc = unsafe { Arc::from_raw(task) };
    let cloned = arc.clone();
    // Do not drop the original reference held by the source waker.
    let _ = Arc::into_raw(arc);
    RawWaker::new(Arc::into_raw(cloned).cast::<()>(), &VTABLE)
}

/// Wakes a task, consuming the waker's reference.
unsafe fn wake_task(data: *const ()) {
    let task = data.cast::<Task>();
    // SAFETY: `data` is a valid `Arc<Task>` pointer from the vtable.
    let arc = unsafe { Arc::from_raw(task) };
    arc.schedule();
    // `arc` is dropped here, releasing the waker's reference.
}

/// Wakes a task without consuming the waker's reference.
unsafe fn wake_task_by_ref(data: *const ()) {
    let task = data.cast::<Task>();
    // SAFETY: `data` is a valid borrowed `Arc<Task>` pointer. We re-borrow
    // it, schedule the task, and then return the borrow.
    let arc = unsafe { Arc::from_raw(task) };
    arc.schedule();
    let _ = Arc::into_raw(arc);
}

/// Drops the task reference held by a waker.
unsafe fn drop_task(data: *const ()) {
    let task = data.cast::<Task>();
    // SAFETY: `data` is a valid `Arc<Task>` pointer from the vtable.
    let _ = unsafe { Arc::from_raw(task) };
}

/// A waker that does nothing when invoked.
///
/// Used by [`Runtime::block_on`](crate::async_rt::runtime::Runtime::block_on)
/// to poll the root future. The calling thread drives the root future directly,
/// so an explicit wake is not required.
pub(crate) fn noop_waker() -> Waker {
    static NOOP_VTABLE: RawWakerVTable =
        RawWakerVTable::new(noop_clone, noop_wake, noop_wake_by_ref, noop_drop);

    /// Cloning a noop waker yields another noop waker.
    unsafe fn noop_clone(_data: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &NOOP_VTABLE)
    }
    /// Waking a noop waker does nothing.
    unsafe fn noop_wake(_data: *const ()) {}
    /// Waking a noop waker by reference does nothing.
    unsafe fn noop_wake_by_ref(_data: *const ()) {}
    /// Dropping a noop waker does nothing.
    unsafe fn noop_drop(_data: *const ()) {}

    // SAFETY: The vtable ignores the data pointer entirely, so a null pointer
    // is safe and never dereferenced.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_VTABLE)) }
}
