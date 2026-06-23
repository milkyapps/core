/// Scoped-thread abstraction: transparent under `std`, and backed by
/// `loom::thread` under the `loom` cfg (which has no `thread::scope`).
#[cfg(not(loom))]
#[allow(unused_imports)]
pub(crate) use std::thread::scope;

#[cfg(loom)]
mod inner {
    use std::cell::RefCell;
    use std::marker::PhantomData;

    pub struct Scope<'env> {
        handles: RefCell<Vec<loom::thread::JoinHandle<()>>>,
        _marker: PhantomData<&'env ()>,
    }

    impl<'env> Scope<'env> {
        pub fn spawn<F>(&self, f: F)
        where
            F: FnOnce() + 'env,
        {
            // SAFETY: loom runs every spawned thread on the same OS thread
            // and `scope` joins all handles before returning, so the
            // borrowed `'env` data stays alive for the whole
            // (cooperatively scheduled) execution of `f`. The lifetime is
            // only extended to `'static` to satisfy
            // `loom::thread::spawn`'s bounds.
            let f: Box<dyn FnOnce() + 'env> = Box::new(f);
            let f: Box<dyn FnOnce() + 'static> = unsafe { std::mem::transmute(f) };
            let handle = loom::thread::spawn(f);
            self.handles.borrow_mut().push(handle);
        }
    }

    pub fn scope<'env, F, R>(f: F) -> R
    where
        F: FnOnce(&Scope<'env>) -> R,
    {
        let scope = Scope {
            handles: RefCell::new(Vec::new()),
            _marker: PhantomData,
        };
        let ret = f(&scope);
        for handle in scope.handles.into_inner() {
            // Propagate panics from spawned threads, mirroring `std`'s scope.
            handle.join().unwrap();
        }
        ret
    }
}

#[cfg(loom)]
pub(crate) use inner::*;

#[cfg(test)]
pub(crate) struct ThreadSafePtr<T>(pub *mut T);

#[cfg(test)]
unsafe impl<T> Send for ThreadSafePtr<T> {}
#[cfg(test)]
unsafe impl<T> Sync for ThreadSafePtr<T> {}

#[cfg(test)]
impl<T> ThreadSafePtr<T> {
    #[cfg(test)]
    pub(crate) fn ptr(&self) -> *mut T {
        self.0
    }
}
