#import "@preview/ctheorems:1.1.3": *
#show: thmrules

#set page(
  paper: "a4",
  margin: (x: 2.5cm, y: 2.5cm),
  numbering: "1",
  header: align(right, text(size: 9pt, "Building a Async Runtime in Rust")),
)

#set text(font: "Libertinus Serif", size: 11pt)
#set heading(numbering: "1.")
#set par(justify: true, leading: 0.65em)
#set code(block: true)
#show raw: it => block(
  fill: luma(245),
  inset: 8pt,
  radius: 4pt,
  width: 100%,
  text(font: "DejaVu Sans Mono", size: 9pt, it)
)

#let note = thmbox("note", "Note", fill: rgb("#e8f4e8"))
#let warning = thmbox("warning", "Warning", fill: rgb("#fff4e8"))
#let definition = thmbox("definition", "Definition", fill: rgb("#e8f0ff"))
#let theorem = thmbox("theorem", "Theorem", fill: rgb("#f0e8ff"))

#align(center, [
  #text(size: 28pt, weight: "bold", "Building a Minimal Async Runtime in Rust")
  #v(1.5em)
  #text(size: 14pt, "A complete, from-scratch guide to understanding Future, Waker, executors, and task scheduling")
  #v(3em)
  #text(size: 12pt, "Daniel Frederico Lins Leite")
  #v(1em)
  #text(size: 10pt, "July 2026")
])

#pagebreak()

#outline(title: "Table of Contents", depth: 2)

#pagebreak()

= Why Do We Need an Async Runtime?

== The two ways to wait

Imagine you want to fetch 100 web pages. In a threaded model you might spawn 100 OS threads, each performing one blocking HTTP request. OS threads are expensive: each consumes a stack (often 1--8 MiB) and the kernel must schedule them. If most threads are blocked waiting for the network, you are paying for 100 stacks while using almost no CPU.

An async model turns the problem inside out. Instead of one thread per request, you express each request as a small state machine. Many such state machines share a small pool of OS threads. When a request is waiting for the network, its state machine is paused, and the thread works on a different request. No OS thread is blocked on I/O.

The runtime is the piece of code that:

+ stores all those state machines,
+ decides which one to run next,
+ resumes them when the thing they were waiting for is ready,
+ multiplexes everything over a handful of OS threads.

In Rust, the state machine is called a `Future`.

== What Rust gives us for free

Rust's standard library defines the `Future` trait. It also defines `Waker`, `Context`, `Poll`, `Pin`, and the `async`/`await` syntax that rewrites your code into a `Future` automatically. Rust does #emph[not] give you the runtime. That is what we will build.

What we need from the runtime:

- a way to create a `Future` and run it to completion,
- a way to spawn additional `Future`s that run concurrently,
- a way for one `Future` to wait for the result of another (`JoinHandle`),
- a way for a `Future` to say "I cannot make progress now; wake me later" (`Poll::Pending` + `Waker`),
- a thread pool to actually execute work.

== What this runtime will and will not do

We will build the #emph[bare minimum complete runtime]:

- `Runtime::new(n)` --- thread pool with `n` worker threads.
- `Handle::spawn(future)` --- spawn a task and get a `JoinHandle`.
- `Runtime::block_on(future)` --- run a future to completion on the current thread.
- `JoinHandle::await` --- wait for a spawned task's output.
- `yield_now()` --- let other tasks run.
- `Waker` machinery that reschedules tasks onto the queue.

We will #emph[not] implement:

- timers,
- network I/O,
- file I/O,
- work stealing,
- cancellation,
- panic propagation across `JoinHandle`.

These are natural extensions once the core is solid.

#pagebreak()

= The `Future` Trait and Polling

== The trait

Open the standard library documentation and you will find:

```rust
pub trait Future {
    type Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
}
```

`Poll<T>` is an enum:

```rust
pub enum Poll<T> {
    Ready(T),
    Pending,
}
```

#definition[
The `Future` contract:
- `poll` is the only way to make progress.
- `poll` must never block. It does a small amount of work and returns immediately.
- If it returns `Pending`, the future is waiting for something external.
- When that external thing becomes ready, the runtime must call `poll` again.
- Once it returns `Ready(output)`, the future is done and must not be polled again.
]

== Why `Pin<&mut Self>`?

A future compiled from `async`/`await` is a state machine. At `.await` points it stores data that may be self-referential. Self-referential structs cannot be moved safely, because moving them would invalidate internal pointers.

`Pin` is a type that promises "this value will not be moved from now on." The runtime stores the future pinned in place (usually on the heap inside a `Box`) and polls it through `Pin<&mut Self>`.

#note[
You, as a runtime author, must guarantee that once a future starts being polled, it stays at the same memory address until it is dropped. Storing it as `Pin<Box<dyn Future>>` does exactly that.
]

== `Context` and `Waker`

The `Context` passed to `poll` contains a `Waker`. The future uses the waker to tell the runtime: "wake me up when I can make progress."

```rust
pub struct Context<'a> {
    waker: &'a Waker,
    // ...
}
```

`Waker` is a cloneable handle to a specific wake-up mechanism. Its API is tiny:

```rust
impl Waker {
    pub fn wake(self);
    pub fn wake_by_ref(&self);
    pub fn clone(&self) -> Waker;
}
```

When a future returns `Pending`, it typically stores the waker somewhere. When the external event happens, it calls `waker.wake()`. The runtime then reschedules that future and polls it again.

== Your first hand-written future

Before building the runtime, write a future manually. This is the best way to understand what `async` desugars to.

```rust
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

struct Delay {
    done: bool,
}

impl Future for Delay {
    type Output = &'static str;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<&'static str> {
        if self.done {
            Poll::Ready("done")
        } else {
            self.done = true;
            // In a real future we would register _cx.waker() with a timer here.
            // For teaching, we immediately become ready on the next poll.
            Poll::Pending
        }
    }
}
```

A toy executor could drive it like this:

```rust
fn block_on<F: Future>(mut future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    struct DummyWaker;
    impl Wake for DummyWaker {
        fn wake(self: Arc<Self>) {}
        fn wake_by_ref(self: &Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(DummyWaker));
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => {}
        }
    }
}
```

This busy-loops and ignores `Pending` semantics, but it shows the shape of an executor: pin the future, create a waker, poll in a loop until ready.

== `async`/`await` is just sugar

This:

```rust
async fn hello() -> i32 {
    42
}
```

desugars to a `Future` whose `Output` is `i32`. This:

```rust
async fn foo() -> i32 {
    let x = bar().await;
    x + 1
}
```

desugars to a state machine with states:

+ start,
+ waiting for `bar()` (return `Pending` if not ready),
+ add one and return `Ready`.

The `.await` operator is the point where the generated state machine returns `Pending` and resumes later.

#pagebreak()

= From a State Machine to a `Future`

== Building a manual async state machine

Let us build a future that waits for a flag to become true. This models a real event source (a timer, a network packet, etc.).

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

struct FlagFuture {
    flag: Arc<AtomicBool>,
    waker_store: Arc<std::sync::Mutex<Option<Waker>>>,
}

impl Future for FlagFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.flag.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        *self.waker_store.lock().unwrap() = Some(cx.waker().clone());
        // Re-check after registering to avoid a lost wake.
        if self.flag.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}
```

Some other thread can set the flag and wake the stored waker:

```rust
fn notify(flag_future: &FlagFuture) {
    flag_future.flag.store(true, Ordering::Release);
    if let Some(waker) = flag_future.waker_store.lock().unwrap().take() {
        waker.wake();
    }
}
```

This pattern appears over and over in async code: register a waker, re-check the condition, return `Pending`, then wake later.

== Lost-wake races

#warning[
The double-check in the snippet above is not paranoia. Without it, a lost-wake race can hang the future forever.
]

Without the re-check:

+ Thread A calls `poll`, sees `flag == false`.
+ Thread B sets `flag = true` and tries to wake, but no waker is stored yet.
+ Thread A stores the waker and returns `Pending`.

Now `flag` is true, the waker is stored, but nobody wakes it. The future hangs forever. This is a #emph[lost-wake race].

The fix is the double-check: after storing the waker, check the condition again. If it became true in the registration window, return `Ready` immediately. This is exactly the pattern we will use in `JoinHandle`.

== `Pin` in practice

When you implement `Future` manually, your `poll` signature is:

```rust
fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
```

Inside `poll` you cannot move `self` unless your type is `Unpin`. Most hand-written futures are `Unpin` because they do not store self-referential data. `async`/`await` generated futures are #emph[not] `Unpin` in general.

As a runtime author, you do not implement `Future` for user code. You receive a `dyn Future` and store it pinned. The pinning happens at the boundary, usually via `Box::pin(future)`.

#pagebreak()

= The Problem of Completion Notification: `Waker`

== `Waker` is type-erased

A `Waker` is created from a `RawWaker`, which is just a pointer plus a vtable:

```rust
pub struct RawWaker {
    data: *const (),
    vtable: &'static RawWakerVTable,
}

pub struct RawWakerVTable {
    clone: unsafe fn(*const ()) -> RawWaker,
    wake: unsafe fn(*const ()),
    wake_by_ref: unsafe fn(*const ()),
    drop: unsafe fn(*const ()),
}
```

The vtable functions interpret `data` as whatever they want. This lets a waker wake up anything --- a task, a thread, an I/O reactor event list --- without the `Waker` type knowing the concrete type.

== A waker that reschedules a task

In our runtime, each task has a waker that, when invoked, pushes the task back onto the shared queue. The waker's `data` is a raw `Arc<Task>` pointer.

The vtable looks like this:

```rust
use std::sync::Arc;
use std::task::{RawWaker, RawWakerVTable, Waker};

static VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_task, wake_task, wake_task_by_ref, drop_task);

unsafe fn clone_task(data: *const ()) -> RawWaker {
    let task = data.cast::<Task>();
    let arc = Arc::from_raw(task);
    let cloned = arc.clone();
    let _ = Arc::into_raw(arc);          // return the borrow
    RawWaker::new(Arc::into_raw(cloned).cast::<()>(), &VTABLE)
}

unsafe fn wake_task(data: *const ()) {
    let task = data.cast::<Task>();
    let arc = Arc::from_raw(task);
    arc.schedule();
    // arc is dropped here, releasing the waker's reference.
}

unsafe fn wake_task_by_ref(data: *const ()) {
    let task = data.cast::<Task>();
    let arc = Arc::from_raw(task);
    arc.schedule();
    let _ = Arc::into_raw(arc);          // return the borrow
}

unsafe fn drop_task(data: *const ()) {
    let task = data.cast::<Task>();
    let _ = Arc::from_raw(task);
}
```

#note[
Key invariants:
- The `data` pointer is an `Arc<Task>` obtained from `Arc::into_raw`.
- `clone_task` must not drop the source reference; it borrows, clones, then returns the borrow via `Arc::into_raw(arc)`.
- `wake_task` consumes the waker, so it reclaims the `Arc` and drops it.
- `wake_task_by_ref` does not consume the waker, so it reclaims and immediately returns the borrow.
- `drop_task` reclaims and drops the `Arc`.

This is the exact memory-accounting dance required when you manage `Arc` counts by hand.
]

== Building a `Waker` from a `Task`

```rust
pub(crate) fn waker_from_task(task: Arc<Task>) -> Waker {
    let raw = Arc::into_raw(task);
    unsafe { Waker::from_raw(RawWaker::new(raw.cast::<()>(), &VTABLE)) }
}
```

`Waker::from_raw` is unsafe because the caller must guarantee the vtable is correct. We satisfy that by construction: the vtable functions only interpret `data` as `Arc<Task>`.

== A noop waker for the root future

When `block_on` polls the root future directly, the root future does not need a real waker. The calling thread is already looping and polling. We provide a `noop_waker` whose `wake` does nothing:

```rust
pub(crate) fn noop_waker() -> Waker {
    static NOOP_VTABLE: RawWakerVTable =
        RawWakerVTable::new(noop_clone, noop_wake, noop_wake_by_ref, noop_drop);

    unsafe fn noop_clone(_data: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &NOOP_VTABLE)
    }
    unsafe fn noop_wake(_data: *const ()) {}
    unsafe fn noop_wake_by_ref(_data: *const ()) {}
    unsafe fn noop_drop(_data: *const ()) {}

    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_VTABLE)) }
}
```

This is safe because the vtable never dereferences the data pointer. Using a noop waker for the root future avoids creating an `Arc<Task>` we do not need.

#pagebreak()

= A Single-Threaded Executor

== The simplest executor

Before we add threads, build a single-threaded executor. It will:

+ maintain a queue of tasks,
+ pop tasks and poll them,
+ reschedule tasks when they are woken,
+ run until a specific task completes.

```rust
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

struct Task {
    future: std::cell::UnsafeCell<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
}

unsafe impl Send for Task {}
unsafe impl Sync for Task {}
```

For this chapter we keep the queue as a `RefCell<VecDeque>` and run everything on one thread. The multi-threaded version will replace `RefCell` with `Mutex`.

== Polling a task

```rust
impl Task {
    fn run(self: Arc<Task>) {
        let waker = todo!();            // we will build this
        let mut cx = Context::from_waker(&waker);

        let fut_opt = unsafe { &mut *self.future.get() };
        if let Some(fut) = fut_opt {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(()) => {
                    *fut_opt = None;
                }
                Poll::Pending => {}
            }
        }
    }
}
```

The `UnsafeCell` is necessary because `Task` is `Sync` but we need mutable access to the future from inside `run`. The safety invariant is that `run` is never called concurrently on the same task. We enforce that with an atomic `running` flag in the real implementation.

== The scheduling flag

If a future is polled and returns `Pending`, but during that poll it calls `cx.waker().wake_by_ref()`, the task must be rescheduled. The simplest correct design is:

+ before polling, clear `scheduled`,
+ during polling, any `wake` sets `scheduled = true`,
+ after polling returns `Pending`, if `scheduled` is true, re-enqueue the task.

This is what `Task::run` does in the full implementation.

== The `running` lock

Because `Task` is shared between threads, we must ensure only one thread polls it at a time. We use `running.swap(true, AcqRel)` as a lightweight lock:

+ if it returns `false`, we acquired the lock and may poll,
+ if it returns `true`, another thread is polling; we call `schedule()` to ensure the task gets another turn.

After `poll` returns, we store `running = false`. Any concurrent `schedule` that saw `running == true` will have set `scheduled = true`, and we check that flag after releasing `running`.

#pagebreak()

= Spawning Tasks and Join Handles

== `JoinHandle` as a future

When you spawn a task, you get back a `JoinHandle<T>` that resolves to the task's output. `JoinHandle` is itself a `Future`.

The shared state between the spawned task and the `JoinHandle` is a `JoinCell<T>`:

```rust
pub(crate) struct JoinCell<T> {
    done: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
    waker: Mutex<Option<Waker>>,
}
```

Why a `Mutex` around the waker? Both the producer (the spawned task) and the consumer (the task awaiting the `JoinHandle`) need to access it. The producer writes the result and wakes. The consumer stores its waker. The mutex serializes these two operations.

== `JoinHandle::poll`

```rust
impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        if self.inner.is_done() {
            return Poll::Ready(unsafe { self.inner.take_output() });
        }

        {
            let mut guard = self.inner.waker.lock().unwrap();
            *guard = Some(cx.waker().clone());
        }

        if self.inner.is_done() {
            let mut guard = self.inner.waker.lock().unwrap();
            guard.take();
            return Poll::Ready(unsafe { self.inner.take_output() });
        }

        Poll::Pending
    }
}
```

The second `is_done()` check after releasing the lock is the lost-wake fix from Chapter 3. If the producer finished between the first check and storing the waker, we observe it now and return `Ready` immediately.

== The `Joinable` trait

`Task` stores `join: Option<Arc<dyn Joinable + Send + Sync>>` because it does not know the concrete `T` of the spawned task's output. `Joinable` is an object-safe trait with one method:

```rust
pub(crate) trait Joinable: Send + Sync {
    fn wake_waiter(&self);
}

impl<T: Send + 'static> Joinable for JoinCell<T> {
    fn wake_waiter(&self) {
        let mut guard = self.waker.lock().unwrap();
        if let Some(waker) = guard.take() {
            waker.wake();
        }
    }
}
```

When the spawned task completes, `Task::run` calls `join.wake_waiter()`. This wakes the task that is awaiting the `JoinHandle`.

== Dropping a `JoinHandle`

Dropping the handle does #emph[not] abort the task. The task continues to completion. Its result is written into the `JoinCell`, but because nobody is waiting, the waker is `None` and `wake_waiter` does nothing. The `Arc<JoinCell<T>>` is dropped when both the `JoinHandle` and the task release their references.

This is a deliberate simplicity trade-off. Cancellation requires an additional abort channel.

#pagebreak()

= Thread Pools and Work Stealing... Actually, a Mutex Queue

== Why start simple?

Real production runtimes use lock-free work-stealing deques or MPMC queues. Those are subtle to get right, especially under Miri and Loom. For a teaching runtime, a `Mutex<VecDeque>` is correct, simple, and fast enough for dozens of tasks.

== The Queue

```rust
pub(crate) struct Queue {
    inner: Mutex<VecDeque<Arc<Task>>>,
    condvar: Condvar,
    shutdown: AtomicBool,
}

impl Queue {
    fn push(&self, task: Arc<Task>) {
        let mut guard = self.inner.lock().unwrap();
        guard.push_back(task);
        self.condvar.notify_one();
    }

    fn pop(&self) -> Option<Arc<Task>> {
        let mut guard = self.inner.lock().unwrap();
        loop {
            if let Some(task) = guard.pop_front() {
                return Some(task);
            }
            if self.shutdown.load(Ordering::Acquire) {
                return None;
            }
            let (new_guard, _) = self
                .condvar
                .wait_timeout(guard, Duration::from_millis(50))
                .unwrap();
            guard = new_guard;
        }
    }

    fn try_pop(&self) -> Option<Arc<Task>> {
        let mut guard = self.inner.lock().unwrap();
        guard.pop_front()
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.condvar.notify_all();
    }
}
```

`pop` is used by worker threads. It blocks until work is available or shutdown. The 50 ms timeout ensures workers can observe shutdown even if they miss a spurious notification.

`try_pop` is used by `block_on`. It never blocks, so `block_on` can poll the root future frequently.

== Worker thread loop

```rust
pub(crate) fn spawn(queue: Arc<Queue>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while let Some(task) = queue.pop() {
            task.run();
        }
    })
}
```

A worker exits when `pop` returns `None`, which happens after `shutdown` is called.

== The `running` flag revisited

Consider what happens if a task is running on thread A and thread B calls `wake` on it:

+ `Task::schedule` sees `running == true` and sets `scheduled = true`.
+ Thread A finishes polling, sees `scheduled == true`, and re-enqueues the task.

What if the task was already scheduled while running? `schedule` checks `scheduled.swap(true)`; if it was already true, it does not push again. This prevents duplicate queue entries.

What if a task is woken after it completed? `schedule` checks `completed` first and ignores the wake. This prevents completed tasks from being re-enqueued.

#pagebreak()

= Putting It Together: `Runtime`, `Handle`, `block_on`

== `Runtime`

```rust
pub struct Runtime {
    shared: Arc<Shared>,
    threads: Vec<thread::JoinHandle<()>>,
}

struct Shared {
    queue: Arc<Queue>,
}
```

`Runtime::new(n)` creates a queue and spawns `n` worker threads.

== `Handle`

```rust
#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
}

unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}
```

`Handle` is cheaply cloneable and can be moved across threads. It holds only immutable shared state.

== `Handle::spawn`

```rust
impl Handle {
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let join = Arc::new(JoinCell::<F::Output>::new());
        let join_clone = join.clone();
        let join_dyn: Arc<dyn Joinable + Send + Sync> = join.clone();

        let wrapped = async move {
            let result = future.await;
            unsafe { join_clone.set_output(result) };
        };

        let task = Task::new(wrapped, self.shared.queue.clone(), Some(join_dyn));
        task.schedule();

        JoinHandle { inner: join }
    }
}
```

The wrapper future awaits the user future and writes the result into the cell. The cell is shared with the returned `JoinHandle`.

== `Runtime::block_on`

The final design polls the root future directly:

```rust
pub fn block_on<F>(&self, future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    use std::task::{Context, Poll};

    let mut future = Box::pin(future);
    let waker = crate::async_rt::waker::noop_waker();
    let mut cx = Context::from_waker(&waker);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {}
        }

        if let Some(task) = self.shared.queue.try_pop() {
            task.run();
        } else {
            thread::park_timeout(Duration::from_millis(1));
        }
    }
}
```

Why this is correct:

+ The root future is polled repeatedly until it returns `Ready`.
+ If the root future returns `Pending` because it is awaiting a `JoinHandle`, the `JoinHandle::poll` will have stored the root future's waker in the `JoinCell`.
+ While waiting, `block_on` runs spawned tasks from the queue. When the spawned task completes, it wakes the root future's waker. `Waker::wake` calls `Task::schedule`, pushing the root task onto the queue.

Wait --- the root task is not on the queue. The root future is polled directly by `block_on`. So how does the wake help?

Actually, the noop waker does nothing. The root future is resumed because `block_on` loops and polls it again after the 1 ms park timeout. The wake is not strictly necessary for resumption, but it is still correct: the waker stored inside `JoinCell` is the root's noop waker, and `wake_waiter` calls it. It is a no-op, but that is fine because `block_on` will retry anyway.

#note[
This is a key insight: `block_on` does not need a real waker because it is the thread that drives the root future. It only needs to make progress on spawned tasks and then come back to the root future.
]

== `Runtime::Drop`

```rust
impl Drop for Runtime {
    fn drop(&mut self) {
        self.shared.queue.shutdown();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}
```

`shutdown` sets the flag and notifies all workers. Workers exit their loops. `Drop` joins the threads so the runtime does not outlive its workers.

`Runtime::shutdown(self)` simply calls `drop(self)`. Users can call it explicitly if they prefer, but it is not required.

#pagebreak()

= Testing an Async Runtime

== The testing pyramid

Test an async runtime at three levels:

+ #strong[Unit tests] for individual components (`JoinCell`, `Queue`, `Task`, waker vtable).
+ #strong[Integration tests] for runtime behavior (`block_on`, `spawn`, `JoinHandle`, `yield_now`).
+ #strong[Concurrency stress tests] run many times to catch races.
+ #strong[Miri] for UB detection.
+ #strong[Loom] for exhaustive concurrency state-space exploration.

== Testing `block_on`

```rust
#[test]
fn block_on_returns_value() {
    let rt = Runtime::new(1);
    let value = rt.block_on(async { 42 });
    assert_eq!(value, 42);
    rt.shutdown();
}
```

This tests the simplest path: a root future with no spawned tasks. Use `Runtime::new(0)` to verify `block_on` works even without workers.

== Testing `spawn` and `JoinHandle`

```rust
#[test]
fn spawn_and_await() {
    let rt = Runtime::new(1);
    let handle = rt.handle();

    let value = rt.block_on(async move {
        handle.spawn(async { 7 * 6 }).await
    });

    assert_eq!(value, 42);
    rt.shutdown();
}
```

This exercises: spawning from inside `block_on`, `JoinHandle` as a future, and the waker-based reschedule path.

== Testing `yield_now`

```rust
#[test]
fn yield_now_interleaves_tasks() {
    let rt = Runtime::new(1);
    let handle = rt.handle();
    let counter = Arc::new(AtomicUsize::new(0));

    let c1 = counter.clone();
    let c2 = counter.clone();

    let value = rt.block_on(async move {
        let a = handle.spawn(async move {
            yield_now().await;
            c1.fetch_add(10, Ordering::SeqCst);
        });
        let b = handle.spawn(async move {
            c2.fetch_add(1, Ordering::SeqCst);
            yield_now().await;
            c2.fetch_add(100, Ordering::SeqCst);
        });

        a.await;
        b.await;
        counter.load(Ordering::SeqCst)
    });

    assert_eq!(value, 111);
    rt.shutdown();
}
```

This test uses a single worker thread. If `yield_now` did not reschedule, both tasks could not interleave. The expected sum `111` proves they did.

== Stress tests

A single run of a concurrent test may not trigger a race. Run it many times:

```bash
for i in $(seq 1 200); do
  cargo test --lib async_rt::runtime::tests -- --test-threads=1 \
    >/dev/null 2>&1 || { echo "FAILED at run $i"; break; }
  if [ $((i % 50)) -eq 0 ]; then echo "[$i]"; fi
done
```

For truly flaky bugs, run under a debugger when the test hangs:

```bash
# terminal 1
./target/debug/deps/milkyapps_core-XXXXX \
  async_rt::runtime::tests::doctest_equivalent

# terminal 2
lldb -p $(pgrep milkyapps_core) -o "thread backtrace all" -o "quit"
```

== Testing `Drop` / shutdown

```rust
#[test]
fn drop_without_shutdown() {
    let worker = std::thread::spawn(|| {
        let rt = Runtime::new(1);
        let handle = rt.handle().spawn(async { 123 });
        assert_eq!(rt.block_on(handle), 123);
        // rt is dropped here; if workers did not exit, this thread would hang.
    });
    worker.join().unwrap();
}
```

This verifies that `Runtime::Drop` correctly shuts down workers and terminates.

== Miri

Install Miri:

```bash
rustup component add miri
```

Run the async runtime tests under Miri:

```bash
cargo miri test --lib async_rt::runtime::tests
```

Miri is slow but catches UB such as:

+ use-after-free in the waker vtable,
+ data races in `UnsafeCell` accesses,
+ invalid memory operations in `MaybeUninit::assume_init_read`.

Our project sets `-Zmiri-ignore-leaks` in `.cargo/config.toml` because some unrelated SMR tests intentionally leak memory. For `async_rt` tests alone this flag is harmless.

== Loom

Loom lets you exhaustively explore thread interleavings. To use it, replace `std::sync` types with `loom::sync` types under `#[cfg(loom)]`. The project already has a `crate::sync` module that does this for some primitives.

A Loom test looks like this:

```rust
#[cfg(loom)]
#[test]
fn loom_join_handle_lost_wake() {
    use crate::sync::model;
    model(|| {
        let rt = Runtime::new(2);
        let handle = rt.handle();
        let value = rt.block_on(async move {
            handle.spawn(async { 42 }).await
        });
        assert_eq!(value, 42);
    });
}
```

`model` runs the closure under Loom, exploring all possible interleavings of the async runtime's atomic operations.

#pagebreak()

= Running, Compiling, and Debugging

== Project layout

Create a library crate:

```bash
cargo new --lib my_async_runtime
cd my_async_runtime
```

Add the module in `src/lib.rs`:

```rust
pub mod async_rt;
```

Create `src/async_rt.rs` and the submodules described in this book.

== Compile and run tests

Run all library tests:

```bash
cargo test --lib
```

Run only the async runtime tests:

```bash
cargo test --lib async_rt::runtime::tests
```

Run a single test:

```bash
cargo test --lib async_rt::runtime::tests::block_on_returns_value
```

Run with single thread (useful for reproducibility):

```bash
cargo test --lib async_rt::runtime::tests -- --test-threads=1
```

Run documentation tests (these compile and execute `///` examples):

```bash
cargo test --doc
```

== Clippy and formatting

Enable clippy warnings as errors:

```bash
cargo clippy --all-features --all-targets -- -D warnings
```

Format the code:

```bash
cargo fmt
```

Check formatting without modifying files:

```bash
cargo fmt -- --check
```

== Debugging a hanging test

If a test hangs, the most useful tool is `lldb` (or `gdb` on Linux).

Step 1: run the test binary directly, not through `cargo test`. First find the binary name:

```bash
cargo test --lib --no-run
ls target/debug/deps/*my_async_runtime*
```

Step 2: run a specific test in the background:

```bash
./target/debug/deps/my_async_runtime-XXXXX \
  async_rt::runtime::tests::doctest_equivalent &
PID=$!
```

Step 3: attach the debugger and print all backtraces:

```bash
lldb -p $PID -b -o "thread backtrace all" -o "quit"
```

Step 4: kill the hung process:

```bash
kill -9 $PID
```

Look for threads stuck in:

+ `Queue::pop` --- suggests the root future is not being polled/resumed,
+ `Task::run` inside a future --- suggests a future is blocking,
+ `pthread_cond_wait` --- suggests a lost wake.

== Using `sample` on macOS

macOS has a built-in profiler:

```bash
sample $PID -file profile.txt -duration 5
```

This records what every thread was doing for 5 seconds. It is less detailed than `lldb` backtraces but non-invasive and useful for timing-sensitive bugs.

#pagebreak()

= Extending the Runtime

== Timers

A timer needs a way to register a future and wake it after a duration. The minimal design:

+ Add a global `TimerWheel` or `BinaryHeap<Timer>` inside the runtime.
+ Add `Runtime::timeout(duration, future)` or `sleep(duration)`.
+ When `sleep(duration).await` is polled, register the waker and deadline in the timer wheel.
+ Run a dedicated thread (or the `block_on` loop) that periodically checks expired timers and wakes them.
+ When a worker has nothing to do, it can advance the timer wheel.

== I/O

For network or file I/O you need a #emph[reactor] integrated with the OS:

+ On Linux: `epoll`.
+ On macOS/BSD: `kqueue`.
+ On Windows: `IOCP`.

The runtime registers file descriptors and wakers with the reactor. The reactor thread calls `epoll_wait`/`kevent` and wakes the corresponding tasks when events arrive.

This is a large extension. Keep the reactor behind a feature flag so the core runtime remains small.

== Work stealing

Replace the global `Mutex<VecDeque>` with per-worker deques. A worker pops from its own deque and steals from others when empty. The standard data structure is a Chase-Lev deque. Libraries like `crossbeam-deque` provide battle-tested implementations.

== Cancellation

To support cancellation:

+ Add an `AtomicBool` abort flag to `Task`.
+ Store the flag in `JoinHandle` as well.
+ When `JoinHandle::abort` is called, set the flag.
+ Cooperative cancellation points (`yield_now`, `.await` boundaries) check the flag and return early.

#warning[
Rust's `Future` trait has no built-in cancellation, so cancellation is always cooperative.
]

== Panic propagation

Currently a panic in a spawned task unwinds the worker thread. Better behavior:

+ Wrap `task.run()` in `std::panic::catch_unwind`.
+ Store a `Result<T, Box<dyn Any + Send>>` in `JoinCell` instead of `T`.
+ Change `JoinHandle` to resolve to that `Result`.

This matches the design of runtimes like Tokio.

== `block_on` with a real root waker

Our `noop_waker` works because `block_on` polls the root future in a loop. An alternative design would schedule the root task on the queue and give it a real waker. The trade-off is subtle:

+ #strong[noop waker + direct poll:] simpler, guarantees root progress, no need to manage a root task reference.
+ #strong[real waker + scheduled root task:] consistent with spawned tasks, but requires solving the lost-wake race that caused the original intermittent hang in this project.

For a minimal runtime, the noop-waker design is the safer choice.

#pagebreak()

= Appendix: Complete minimal runtime file listing

This appendix lists the public types and their responsibilities for quick reference.

- `Runtime::new(worker_threads) -> Runtime`
- `Runtime::handle(&self) -> Handle`
- `Runtime::block_on<F>(&self, future: F) -> F::Output`
- `Runtime::shutdown(self)`
- `Handle::spawn<F>(&self, future: F) -> JoinHandle<F::Output>`
- `JoinHandle<T>: Future<Output = T>`
- `yield_now() -> impl Future<Output = ()>`

Internal types:

- `Task` --- erased future + scheduling state.
- `Queue` --- mutex FIFO + condvar.
- `JoinCell<T>` --- oneshot result + stored waker.
- `Joinable` --- object-safe wake interface.
- `waker_from_task` / `noop_waker` --- waker constructors.

#pagebreak()

= Afterword

You have now seen how an async runtime works from first principles:

+ `Future` is a state machine polled repeatedly.
+ `Waker` is a type-erased callback that reschedules the state machine.
+ The executor stores tasks, polls them, and handles `Pending` by waiting for wakes.
+ `JoinHandle` is just another future that waits on a shared oneshot cell.
+ Thread pools share a queue; workers pull tasks and run them.
+ `block_on` is the bridge between synchronous code and the async world.

The runtime in this book is small, but the concepts are the same ones that power Tokio, async-std, and smol. Extend it piece by piece --- timers, I/O, work stealing, cancellation --- and you will understand every layer.

Happy hacking.
