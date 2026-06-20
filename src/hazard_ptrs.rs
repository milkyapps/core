//! Hazard pointers — safe memory reclamation for lock-free data structures.
//!
//! # What it is
//!
//! Hazard pointers are a technique for *safe memory reclamation* in
//! concurrent, lock-free data structures. The core idea, introduced by Maged
//! M. Michael, is described in the seminal paper:
//!
//! > Maged M. Michael, *"Hazard Pointers: Safe Memory Reclamation for
//! > Lock-Free Objects,"* IEEE Transactions on Parallel and Distributed
//! > Systems, vol. 15, no. 8, pp. 491–504, August 2004.
//!
//! The paper motivates the technique as follows:
//!
//! > "A new technique for dynamic memory reclamation for concurrent lock-free
//! > objects is presented. The technique uses hazard pointers, one per thread,
//! > to indicate to concurrent threads that the referenced objects are
//! > currently in use and should not be reclaimed."
//!
//! In the same paper, the mechanism is summarized succinctly:
//!
//! > "A hazard pointer is a pointer to a memory location that a thread is
//! > currently accessing. ... If a thread's hazard pointer holds the address of
//! > an object, then no other thread may reclaim that object."
//!
//! Concretely, each thread publishes the address of any object it is about to
//! dereference into a shared *hazard slot* before reading it. A thread that
//! wants to retire (i.e., unlink and eventually free) an object publishes it
//! to a *retirement list* and defers its reclamation: the object is only freed
//! once a scan confirms that no thread's hazard slot still references it. This
//! guarantees that an object is never freed while a thread holds an
//! unprotected reference to it — the central safety property of the technique.
//!
//! # Why use it
//!
//! Lock-free algorithms unlink objects from a structure (e.g. a node removed
//! from a stack) before they are sure that no other thread is still reading
//! them. Naïvely freeing the memory immediately is unsound: another thread may
//! have loaded a pointer to that node and be about to dereference it, leading
//! to a use-after-free. The alternatives each have drawbacks:
//!
//! - **Leaking** the memory (never freeing) is safe but unbounded in size.
//! - **Reference counting** is hard to make lock-free without strong atomic
//!   updates and can suffer from the ABA problem.
//! - **Garbage collection** is not generally available in Rust and introduces
//!   pauses and runtime cost.
//!
//! Hazard pointers solve this with bounded overhead, no GC, and — unlike
//! epoch-based reclamation (e.g. Crossbeam's `epoch`) — they reclaim memory
//! *promptly* and do not require global quiescence. Michael's paper notes the
//! technique's main properties:
//!
//! > "The technique has low overhead, does not require special operating
//! > system or hardware support, and is independent of the number of threads
//! > and the number of processors."
//!
//! In exchange for these properties, hazard pointers pay a per-access cost
//! (publishing the pointer on each dereference) and limit how many distinct
//! objects a thread can protect simultaneously per slot.
//!
//! This crate implements a small, direct version of the scheme:
//! - A fixed-size global array of hazard slots ([`HAZARD_ARRAY`]),
//!   with availability tracked in [`SLOT_AVAILABLE`].
//! - Per-thread slot ownership recorded in the thread-local [`ID`].
//! - A lock-free singly-linked retirement list rooted at [`RETIRE_HEAD`].
//! - RAII protection via [`Guard`] and the [`protect`]/[`unprotect`]/[`retire`]
//!   functions, plus a [`reclaim`] pass that frees the safe nodes.
//!
//! # Example
//!
//! A producer thread publishes a pointer into a hazard slot while a consumer
//! retires it; the memory is reclaimed only once the producer is done:
//!
//! ```ignore
//! use std::sync::Barrier;
//! use milkyapps_core::hazard_ptrs::{install, protect, unprotect, retire, reclaim};
//!
//! std::thread::scope(|scope| {
//!     // Thread A: read the value while protected.
//!     scope.spawn(|| {
//!         install();
//!         let mut value = Box::new(42u64);
//!         let ptr = value.as_mut() as *mut u64 as *mut ();
//!
//!         let guard = protect(ptr); // publish 'ptr' as a hazard pointer
//!         // ... dereference 'ptr' here; it is guaranteed not to be freed ...
//!         unprotect(guard);        // release protection
//!     });
//!
//!     // Thread B: retire the same object (queue it for later reclamation).
//!     scope.spawn(|| {
//!         install();
//!         let mut value = Box::new(42u64);
//!         let ptr = value.as_mut() as *mut u64 as *mut ();
//!
//!         let guard = protect(ptr);
//!         retire(guard); // pointer is queued, not yet freed
//!
//!         // Safe to call periodically: frees only objects that no
//!         // thread is currently protecting.
//!         let mut reclaimed = Vec::new();
//!         reclaim(&mut reclaimed);
//!     });
//! });
//! ```
//!
//! Notice that `reclaim` will *not* free `ptr` while Thread A's hazard slot
//! still references it — only once Thread A calls [`unprotect`] does the object
//! become eligible for reclamation on a subsequent `reclaim`. That is the
//! guarantee at the heart of hazard pointers.

use std::{
    cell::Cell,
    mem::MaybeUninit,
    panic,
    ptr::null_mut,
    sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicPtr, Ordering},
    },
};

/// The number of hazard pointers in the array.
const HAZARD_ARRAY_LEN: usize = 8;

/// A tracking mechanism for available hazard pointer slots.
static SLOT_AVAILABLE: LazyLock<[AtomicBool; HAZARD_ARRAY_LEN]> = LazyLock::new(|| {
    let mut data: [MaybeUninit<AtomicBool>; HAZARD_ARRAY_LEN] =
        [const { std::mem::MaybeUninit::uninit() }; HAZARD_ARRAY_LEN];

    for elem in data.iter_mut() {
        elem.write(AtomicBool::new(true));
    }

    // 3. Transmute to initialized type
    // Safe because all elements are now initialized.
    unsafe { std::mem::transmute::<_, [_; HAZARD_ARRAY_LEN]>(data) }
});
/// The global array of hazard pointers, indexed by slot.
///
/// Each entry holds the pointer currently protected by the thread that owns
/// that slot, or a null pointer when the slot is free or not actively
/// protecting anything.
static HAZARD_ARRAY: LazyLock<[AtomicPtr<()>; HAZARD_ARRAY_LEN]> = LazyLock::new(|| {
    let mut data: [MaybeUninit<AtomicPtr<()>>; HAZARD_ARRAY_LEN] =
        [const { std::mem::MaybeUninit::uninit() }; HAZARD_ARRAY_LEN];

    for elem in data.iter_mut() {
        elem.write(AtomicPtr::new(std::ptr::null_mut()));
    }

    // 3. Transmute to initialized type
    // Safe because all elements are now initialized.
    unsafe { std::mem::transmute::<_, [_; HAZARD_ARRAY_LEN]>(data) }
});

/// A node in the singly-linked retirement list.
///
/// Retired pointers are queued here until a subsequent [`reclaim`] call
/// determines they are no longer protected by any hazard pointer and frees them.
#[derive(Debug)]
struct RetireNode {
    /// The retired pointer awaiting reclamation.
    ptr: *mut (),
    /// Pointer to the next node in the retirement list.
    next: AtomicPtr<RetireNode>,
}

/// The head of the global singly-linked retirement list.
///
/// New retirements are pushed onto the front of this list; [`reclaim`] takes
/// ownership of the entire list, drops the safe nodes, and reattaches the rest.
static RETIRE_HEAD: LazyLock<AtomicPtr<RetireNode>> = LazyLock::new(|| AtomicPtr::new(null_mut()));

/// Allocates a [`RetireNode`] for `ptr` and atomically pushes it onto the
/// front of the retirement list.
///
/// Returns a pointer to the newly inserted node.
fn push_retire_head(ptr: *mut ()) -> *mut RetireNode {
    let new = Box::leak(Box::new(RetireNode {
        ptr,
        next: AtomicPtr::new(null_mut()),
    }));
    let mut current = RETIRE_HEAD.load(Ordering::Acquire);
    loop {
        new.next = AtomicPtr::new(current);
        match RETIRE_HEAD.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break new,
            Err(new_head) => current = new_head,
        }
    }
}

/// Use only in debug.
///
/// Walks the retirement list from `head` and dumps every node with `dbg!`.
/// This is intended only for ad-hoc debugging: it dereferences raw pointers
/// without any synchronization guarantees.
///
/// # Safety
///
/// Iterate the data structures without any lock. Caller must guarantee nothing is running whilst
/// this is called.
#[allow(unused)]
unsafe fn debug_retire_head(head: &AtomicPtr<RetireNode>) {
    let mut nodes = vec![];

    let mut current = head.load(Ordering::SeqCst);
    while !current.is_null() {
        let node = unsafe { &mut *current };
        current = node.next.load(Ordering::SeqCst);
        nodes.push(node);
    }

    dbg!(nodes);
}

// Per-thread index of the hazard-pointer slot owned by the current thread.
//
// Set by `install` when a thread claims a slot, and read by `protect`,
// `unprotect`, and `retire` to find that thread's slot without passing it
// around explicitly. (Documented here rather than with a doc comment because
// rustdoc does not generate documentation for macro invocations.)
thread_local! {
    static ID: Cell<usize> = const { Cell::new(0) };
}

/// An RAII guard guarding a pointer that is currently published in a hazard slot.
///
/// Dropping a guard without first consuming it via [`unprotect`] or [`retire`]
/// is a programmer error: in debug builds the [`Drop`] implementation panics
/// (a "drop bomb") to catch leaked protections early.
pub struct Guard {
    /// The protected pointer, or null once the guard has been defused.
    ptr: *mut (),
}

impl Drop for Guard {
    /// Asserts that the guard was properly disposed of.
    ///
    /// In debug builds this panics if the guard still holds a non-null pointer,
    /// i.e. it was dropped without a call to [`unprotect`] or [`retire`].
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // On debug we will panic, but we must unprotect in case
            // the panic is catched
            unprotect_with_id(ID.get(), self.ptr);

            if cfg!(debug_assertions) {
                panic!(
                    "Hazard Pointer dropped without calling `unprotect` or `retire` (drop_bomb)"
                );
            } else {
                // TODO
                // warn
            }
        }
    }
}

/// Resets all global hazard-pointer state to its initial, empty configuration.
///
/// Marks every slot as available, clears every hazard pointer, and empties the
/// retirement list. Intended for use between tests; calling this while threads
/// are actively using the system is unsafe.
///
/// # Safety
///
/// This method is unsafe because it clears all the data structures without locks.
/// So the caller must guarantee there is nothing running whilst this is called.
pub unsafe fn clear() {
    for i in 0..HAZARD_ARRAY_LEN {
        SLOT_AVAILABLE[i].store(true, Ordering::SeqCst);
    }
    for i in 0..HAZARD_ARRAY_LEN {
        HAZARD_ARRAY[i].store(null_mut(), Ordering::SeqCst);
    }
    RETIRE_HEAD.store(null_mut(), Ordering::SeqCst);
}

/// Claims an available hazard-pointer slot for the current thread.
///
/// Scans [`SLOT_AVAILABLE`] for a free slot, atomically reserves it, and records
/// its index in the thread-local [`ID`]. Panics if all slots are already taken.
pub fn install() {
    for i in 0..HAZARD_ARRAY_LEN {
        if SLOT_AVAILABLE[i]
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            ID.set(i);
            return;
        }
    }

    panic!("cannot install")
}

/// Publishes `ptr` in the hazard slot identified by `id`.
///
/// The slot must currently hold a null pointer; attempting to protect a second
/// pointer in the same slot panics, since each thread may protect only one
/// pointer at a time.
pub fn protect_with_id(id: usize, ptr: *mut ()) {
    debug_assert!(id < HAZARD_ARRAY.len());

    match HAZARD_ARRAY[id].compare_exchange(null_mut(), ptr, Ordering::AcqRel, Ordering::Relaxed) {
        Ok(_) => {}
        Err(_) => {
            panic!("Do not protect more than one pointer")
        }
    }
}

/// Publishes `ptr` in the current thread's hazard slot and returns a [`Guard`].
///
/// Relies on [`install`] having previously claimed a slot for this thread.
pub fn protect(ptr: *mut ()) -> Guard {
    let id = ID.get();
    protect_with_id(id, ptr);
    Guard { ptr }
}

/// Clears the hazard slot identified by `id`, releasing protection of `ptr`.
///
/// Panics if the slot was not currently protecting exactly `ptr`.
fn unprotect_with_id(id: usize, ptr: *mut ()) {
    debug_assert!(id < HAZARD_ARRAY.len());

    match HAZARD_ARRAY[id].compare_exchange(ptr, null_mut(), Ordering::AcqRel, Ordering::Relaxed) {
        Ok(_) => {}
        Err(_) => {
            panic!("This pointer was not being protected")
        }
    }
}

/// Releases protection of the pointer held by `g`, consuming the guard.
///
/// Clears the current thread's hazard slot and defuses the guard so its [`Drop`]
/// does not trigger the drop bomb.
pub fn unprotect(mut g: Guard) {
    let id = ID.get();
    unprotect_with_id(id, g.ptr);

    // Defuse drop bomb
    g.ptr = null_mut();
}

/// Queues the pointer held by the slot identified by `id` for retirement and
/// then clears that slot's protection.
///
/// After this call the pointer is no longer protected by `id`, but is not yet
/// reclaimed — it will be freed by a later [`reclaim`] once no slot protects it.
fn retire_with_id(id: usize, ptr: *mut ()) {
    debug_assert!(id < HAZARD_ARRAY.len());

    push_retire_head(ptr);
    unprotect_with_id(id, ptr);
}

/// Retires the pointer guarded by `g`, consuming the guard.
///
/// Pushes the pointer onto the retirement list and clears the current thread's
/// hazard slot, defusing the guard so its [`Drop`] does not trigger the drop bomb.
/// The memory is reclaimed lazily by a subsequent [`reclaim`].
pub fn retire(mut g: Guard) {
    let id = ID.get();
    retire_with_id(id, g.ptr);

    // Defuse drop bomb
    g.ptr = null_mut();
}

pub fn reclaim(v: &mut Vec<*mut ()>) {
    // Take control of the whole list
    let mut current = RETIRE_HEAD.swap(null_mut(), Ordering::Acquire);
    // We control the whole list now

    // List is empty
    if current.is_null() {
        return;
    }

    // check if the current.ptr is inside the hazard array.
    // If it is, we cannot drop this item as it is still being used
    // If it isnt, we delete its memory and connect the last node to the next
    let mut first: *mut RetireNode = current;
    let mut last: *mut RetireNode = current;

    while !current.is_null() {
        assert!(!first.is_null());
        assert!(!last.is_null());

        // SAFETY: Guaranteed to not be null here
        // Only we have access to current, so it will live for this whole function
        let c = unsafe { &mut (*current) };

        let ptr = c.ptr;
        let is_safe_to_delete = HAZARD_ARRAY
            .iter()
            .position(|item| ptr == item.load(Ordering::Acquire))
            .is_none();

        let next = c.next.load(Ordering::Relaxed);

        if is_safe_to_delete {
            // SAFETY: We allocated using Box::leak(Box::new(...)) above
            let c = unsafe { Box::from_raw(current) };
            v.push(c.ptr);

            if first == current {
                first = next;
                last = next;
            } else {
                if !next.is_null() {
                    // SAFETY: Deref last is safe because it started as current which is not null
                    unsafe { (*last).next.store(next, Ordering::Relaxed) };
                }
            }
        } else {
            last = current;
        }

        current = next;
    }

    // If last is null, we deleted all nodes
    if last.is_null() {
        assert!(first.is_null());
        return;
    }

    // prepend the current list to head
    let mut current = RETIRE_HEAD.load(Ordering::Acquire);
    loop {
        unsafe { (*last).next.store(current, Ordering::Relaxed) };

        match RETIRE_HEAD.compare_exchange_weak(current, first, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => break,
            Err(new_head) => current = new_head,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, MutexGuard};

    struct TestContext {
        guard: Option<MutexGuard<'static, ()>>,
    }

    macro_rules! fixture {
        (config; ) => { };
        (config; setup: fn () $setup_block:block, $($tokens:tt)*) => {
            fn setup () $setup_block

            fixture!{config; $($tokens)*}
        };
        (config; serial: true, $($tokens:tt)*) => {
            static MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());
            fixture!{config; $($tokens)*}
        };
        (config; serial: false, $($tokens:tt)*) => {
            fixture!{config; $($tokens)*}
        };

        // Lock guard if needed
        (before_each_test; $ctx:ident; ) => { };
        (before_each_test; $ctx:ident; setup: fn () $setup_block:block, $($tokens:tt)*) => {
            fixture!{before_each_test; $ctx; $($tokens)*}
        };
        (before_each_test; $ctx:ident; serial: true, $($tokens:tt)*) => {
            $ctx.guard = Some(
                MUTEX.lock().unwrap_or_else(|e| e.into_inner())
            );
            fixture!{before_each_test; $ctx; $($tokens)*}
        };
        (before_each_test; $ctx:ident; serial: false, $($tokens:tt)*) => {
            fixture!{before_each_test; $ctx; $($tokens)*}
        };

        // Drop guard if needed
        (after_each_test; ) => { };
        (after_each_test; setup: fn () $setup_block:block, $($tokens:tt)*) => {
            fixture!{after_each_test; $($tokens)*}
        };
        (after_each_test; serial: true, $($tokens:tt)*) => {
            fixture!{after_each_test; $($tokens)*}
        };
        (after_each_test; serial: false, $($tokens:tt)*) => {
            fixture!{after_each_test; $($tokens)*}
        };

        // Main arm
        ($name:ident; { $($tokens:tt)* }; $(#[test] fn $fn_name:ident() $test_lock:block)*) => {
            mod $name {
                use super::*;

                fixture!{config; $($tokens)*}

                fn before_each_test() -> TestContext {
                    #[allow(unused_mut)]
                    let mut ctx = TestContext {
                        guard: None,
                    };
                    fixture!{before_each_test; ctx; $($tokens)*}
                    ctx
                }

                fn after_each_test(#[allow(unused)]ctx: TestContext) {
                    fixture!{after_each_test; $($tokens)*}
                }

                $(
                    #[test]
                    fn $fn_name() {
                        let __ctx = before_each_test();
                        setup();
                        $test_lock
                        after_each_test(__ctx);
                    }
                )*
            }
        };
    }

    fixture! {hazard_serial_tests; {
        serial: true,
        setup: fn() {
            unsafe { clear() };
            install();
        },
    };
        #[test]
        fn protect_unprotect_must_add_remove_from_array() {
            let mut value = Box::new(42u64);
            let ptr = value.as_mut() as *mut u64 as *mut ();

            // Ensure it's actually in the hazard array
            let id = ID.get();
            assert!(HAZARD_ARRAY[id].load(Ordering::SeqCst).is_null(), "Thread should not be protecting any pointer");
            let g = protect(ptr);
            assert_eq!(HAZARD_ARRAY[id].load(Ordering::SeqCst), ptr, "Thread should be protecting ptr");
            unprotect(g);
            assert!(HAZARD_ARRAY[id].load(Ordering::SeqCst).is_null(), "Thread should not be protecting ptr anymore");
        }

        #[test]
        fn reclaim_empty() {
            // Should not panic or crash
            assert!(RETIRE_HEAD.load(Ordering::SeqCst).is_null(), "Retire list should be empty");
            let mut v = Vec::new();
            reclaim(&mut v);
            assert!(v.is_empty(), "reclaim vec should be empty");
            assert!(RETIRE_HEAD.load(Ordering::SeqCst).is_null(), "Retire list should be empty");
        }

        #[test]
        fn single_protect_retire_and_reclaim() {
            let mut value = Box::new(42u64);
            let ptr = value.as_mut() as *mut u64 as *mut ();

            let g = protect(ptr);
            retire(g);

            let mut v = Vec::with_capacity(16);
            reclaim(&mut v);

            assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
            assert_eq!(v[0], ptr, "Reclaimed pointer should be the retired one");
        }

        #[test]
        fn protect_prevents_reclaim() {
            let mut value = Box::new(42u64);
            let ptr = value.as_mut() as *mut u64 as *mut ();

            // Simulate two threads that are protecting the same ptr
            let g1 = protect(ptr);

            protect_with_id(ID.get() + 1, ptr);
            retire_with_id(ID.get() + 1, ptr);

            assert!(!RETIRE_HEAD.load(Ordering::SeqCst).is_null(), "Retire List should not be empty");

            // Because 'ptr' is still in the Hazard Array, it should NOT be reclaimed
            let mut v = Vec::new();
            reclaim(&mut v);
            assert!(
                v.is_empty(),
                "ptr should still be protected and not reclaimed"
            );

            // Now ptr is no long protected and should be reclaimed
            unprotect(g1);
            reclaim(&mut v);
            assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
            assert_eq!(v[0], ptr, "Reclaimed pointer should be the retired one");

            assert!(RETIRE_HEAD.load(Ordering::SeqCst).is_null(), "Retire List should be empty");
        }

        #[test]
        fn multiple_retirements() {
            let ptr1 = Box::leak(Box::new(1u64)) as *mut u64 as *mut ();
            let ptr2 = Box::leak(Box::new(2u64)) as *mut u64 as *mut ();
            let ptr3 = Box::leak(Box::new(3u64)) as *mut u64 as *mut ();

            // Simulate three retirements without active protections
            push_retire_head(ptr1);
            push_retire_head(ptr2);
            push_retire_head(ptr3);

            let mut v = Vec::new();
            reclaim(&mut v);
            assert_eq!(v.len(), 3, "All retired pointer should have been reclaimed");
        }

        #[test]
        fn partial_reclaim() {
            let ptr1 = Box::leak(Box::new(1u64)) as *mut u64 as *mut ();
            let ptr2 = Box::leak(Box::new(2u64)) as *mut u64 as *mut ();

            // Protect p1, leave p2 unprotected
            // Put both into the retired list
            protect_with_id(ID.get(), ptr1);
            push_retire_head(ptr1);
            push_retire_head(ptr2);

            // Only ptr2 should be reclaimed because ptr1 is still in Hazard Array
            let mut v = Vec::new();
            reclaim(&mut v);
            assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
            assert_eq!(v[0], ptr2, "Only ptr2 is not protected");
        }

        #[test]
        fn install_exhaustion() {
            unsafe { clear() };
            for _ in 0..HAZARD_ARRAY_LEN {
                install();
            }
        }

        #[test]
        fn protect_multi_thread() {
            unsafe { clear() };
            let qty_threads = HAZARD_ARRAY_LEN;
            let protect_barrier = Barrier::new(qty_threads + 1);
            let wait_asserts_barrier = Barrier::new(qty_threads + 1);

            std::thread::scope(|scope| {
                for _ in 0..qty_threads {
                    scope.spawn(|| {
                        install();
                        let p = Box::leak(Box::new(1u64)) as *mut u64 as *mut ();
                        let g = protect(p);

                        protect_barrier.wait();
                        wait_asserts_barrier.wait();

                        unprotect(g);
                    });
                }

                protect_barrier.wait();

                for i in 0..HAZARD_ARRAY_LEN {
                    assert!(!HAZARD_ARRAY[i].load(Ordering::SeqCst).is_null(), "Thread has protected its pointer");
                }

                wait_asserts_barrier.wait();
            });

            for i in 0..HAZARD_ARRAY_LEN {
                assert!(HAZARD_ARRAY[i].load(Ordering::SeqCst).is_null(), "Thread has unprotected its pointer");
            }
        }

        #[test]
        fn high_contention_retire() {
            unsafe { clear() };
            let qty_threads = HAZARD_ARRAY_LEN;
            let items_per_thread = 100;
            let barrier = Barrier::new(qty_threads);

            std::thread::scope(|scope| {
                for _ in 0..qty_threads {
                    scope.spawn(|| {
                        let ptrs = (0..items_per_thread).map(|i| {
                            Box::leak(Box::new(i as u64)) as *mut u64 as *mut ()
                        }).collect::<Vec<_>>();

                        // Sync start to increase contention
                        barrier.wait();

                        install();
                        for ptr in ptrs {
                            let g = protect(ptr);
                            retire(g);
                        }
                    });
                }
            });

            // Final reclamation of everything
            let mut v = Vec::new();
            reclaim(&mut v);
            assert_eq!(v.len(), qty_threads * items_per_thread, "All ptrs were retired and should be reclaimed");
        }

        #[test]
        fn mixed_concurrent_access() {
            // This simulates the real-world hazard pointer use case:
            // Thread A is reading/protecting a value,
            // while Thread B is trying to retire it.
            unsafe { clear() };
            install();

            let ptr = Box::leak(Box::new(100u64)) as *mut u64;
            let ptr = ptr as u64;

            // thread 1: g is protected
            // thread 2: g is protected and retired
            let barrier1 = Barrier::new(2);

            // thread 1: g is still protected
            // thread 2: reclaim is called
            let barrier2 = Barrier::new(2);

            std::thread::scope(|scope| {
                scope.spawn(|| {
                    install();

                    let g = protect(ptr as *mut ());
                    barrier1.wait();
                    barrier2.wait();
                    unprotect(g);
                });

                scope.spawn(|| {
                    install();

                    let g = protect(ptr as *mut ());
                    retire(g);
                    barrier1.wait();

                    let mut v = Vec::new();
                    reclaim(&mut v);
                    assert!(v.is_empty(), "g is still protected by thread 1");

                    barrier2.wait();
                });
            });

            let mut v = Vec::new();
            reclaim(&mut v);
            assert_eq!(v.len(), 1, "Now g is reclaimed");
            assert_eq!(v[0], ptr as *mut (), "Now g is reclaimed");
        }

        #[test]
        fn guard_drop() {
            unsafe { clear() };
            install();

            let mut value = Box::new(11u64);
            let ptr = value.as_mut() as *mut u64 as *mut ();

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                assert!(HAZARD_ARRAY[ID.get()].load(Ordering::SeqCst).is_null(), "slot should be free");
                let _g = protect(ptr);
                assert!(!HAZARD_ARRAY[ID.get()].load(Ordering::SeqCst).is_null(), "ptr is protected");
            }));

            assert!(HAZARD_ARRAY[ID.get()].load(Ordering::SeqCst).is_null(), "slot should be free now");

            if cfg!(debug_assertions) {
                assert!(
                    result.is_err(),
                    "On Debug, dropping a live guard must trip the drop bomb"
                );
            } else {
                assert!(
                    result.is_ok(),
                    "On Release, dropping a live guard does not panic"
                );
            }
        }
    }
}
