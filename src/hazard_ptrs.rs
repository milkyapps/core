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

const HAZARD_ARRAY_LEN: usize = 8;
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

#[derive(Debug)]
struct RetireNode {
    ptr: *mut (),
    next: AtomicPtr<RetireNode>,
}
static RETIRE_HEAD: LazyLock<AtomicPtr<RetireNode>> = LazyLock::new(|| AtomicPtr::new(null_mut()));

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

// Not safe. Use only in debug
#[allow(unused)]
fn debug_retire_head(head: &AtomicPtr<RetireNode>) {
    let mut nodes = vec![];

    let mut current = head.load(Ordering::SeqCst);
    while !current.is_null() {
        let node = unsafe { &mut *current };
        current = node.next.load(Ordering::SeqCst);
        nodes.push(node);
    }

    dbg!(nodes);
}

thread_local! {
    static ID: Cell<usize> = const { Cell::new(0) };
}

pub struct Guard {
    ptr: *mut (),
}

impl Drop for Guard {
    fn drop(&mut self) {
        if cfg!(debug_assertions) {
            if !self.ptr.is_null() {
                panic!("Drop bomb! Call unprotect or retire");
            }
        } else {
            // TODO
            // warn
        }
    }
}

pub fn clear() {
    for i in 0..HAZARD_ARRAY_LEN {
        SLOT_AVAILABLE[i].store(true, Ordering::SeqCst);
    }
    for i in 0..HAZARD_ARRAY_LEN {
        HAZARD_ARRAY[i].store(null_mut(), Ordering::SeqCst);
    }
    RETIRE_HEAD.store(null_mut(), Ordering::SeqCst);
}

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

pub fn protect_with_id(id: usize, ptr: *mut ()) {
    match HAZARD_ARRAY[id].compare_exchange(null_mut(), ptr, Ordering::AcqRel, Ordering::Relaxed) {
        Ok(_) => {}
        Err(_) => {
            panic!("Do not protect more than one pointer")
        }
    }
}

pub fn protect(ptr: *mut ()) -> Guard {
    let id = ID.get();
    protect_with_id(id, ptr);
    Guard { ptr }
}

fn unprotect_with_id(id: usize, ptr: *mut ()) {
    match HAZARD_ARRAY[id].compare_exchange(ptr, null_mut(), Ordering::AcqRel, Ordering::Relaxed) {
        Ok(_) => {}
        Err(_) => {
            panic!("This pointer was not being protected")
        }
    }
}

pub fn unprotect(mut g: Guard) {
    let id = ID.get();
    unprotect_with_id(id, g.ptr);

    // Defuse drop bomb
    g.ptr = null_mut();
}

fn retire_with_id(id: usize, ptr: *mut ()) {
    push_retire_head(ptr);
    unprotect_with_id(id, ptr);
}

pub fn retire(mut g: Guard) {
    let id = ID.get();
    retire_with_id(id, g.ptr);

    // Defuse drop bomb
    g.ptr = null_mut();
}

/// A standard impl here would have the issue of no guarantee that deref current is safe
/// To avoid this we will "take" thw whole list down for a time.
/// That means that some "drop" calls will spourisly think the retire list is empty.
/// Which is not a problem.
///
/// ```ignore
/// let mut current = RETIRE_HEAD.load(Ordering::Acquire);
/// loop {
///     // SAFETY: No guarantee that deref current is safe
///     let new = unsafe { (*current).next.load(Ordering::Acquire) };
///     match RETIRE_HEAD.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire) {
///         Ok(_) => {}
///         Err(new_head) => current = new_head,
///     }
/// }
/// ```
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
            clear();
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
            clear();
            for _ in 0..HAZARD_ARRAY_LEN {
                install();
            }
        }

        #[test]
        fn protect_multi_thread() {
            clear();
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
            clear();
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
            clear();
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
    }
}
