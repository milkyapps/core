use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

const LOWER_48_BITS_SET: usize = (1 << 48) - 1;
const TAG_SHIFT: usize = 48;

fn usize_to_ptr_tag<T>(inner: usize) -> (*mut T, u16) {
    let ptr = (inner & LOWER_48_BITS_SET) as *mut T;
    let tag = (inner >> TAG_SHIFT) as u16;

    (ptr, tag)
}

fn ptr_tag_to_usize<T>(ptr: *mut T, tag: u16) -> usize {
    // SAFETY: Under the assumption that ptr always fit, so we do not need to mask it here
    ((tag as usize) << TAG_SHIFT) | (ptr as usize)
}

pub struct TaggedPtr<T> {
    inner: usize, // packed (ptr | (tag << 48))
    _phantom: PhantomData<*mut T>,
}

impl<T> std::fmt::Debug for TaggedPtr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (ptr, tag) = usize_to_ptr_tag::<()>(self.inner);
        f.debug_tuple("TaggedPtr")
            .field(&self.inner)
            .field(&ptr)
            .field(&tag)
            .finish()
    }
}

impl<T> Clone for TaggedPtr<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

impl<T> Copy for TaggedPtr<T> {}

impl<T> TaggedPtr<T> {
    pub fn new(ptr: *mut T, tag: u16) -> TaggedPtr<T> {
        TaggedPtr {
            inner: ptr_tag_to_usize(ptr, tag),
            _phantom: PhantomData,
        }
    }

    pub fn from_inner(inner: usize) -> TaggedPtr<T> {
        TaggedPtr {
            inner,
            _phantom: PhantomData,
        }
    }

    pub fn ptr(&self) -> *mut T {
        usize_to_ptr_tag(self.inner).0
    }

    pub unsafe fn byte_add<P>(self, n: usize) -> TaggedPtr<P> {
        let (ptr, tag) = usize_to_ptr_tag::<P>(self.inner);
        TaggedPtr {
            inner: ptr_tag_to_usize::<P>(unsafe { ptr.byte_add(n) }, tag),
            _phantom: PhantomData,
        }
    }

    pub unsafe fn byte_sub<P>(self, n: usize) -> TaggedPtr<P> {
        let (ptr, tag) = usize_to_ptr_tag::<P>(self.inner);
        TaggedPtr {
            inner: ptr_tag_to_usize::<P>(unsafe { ptr.byte_sub(n) }, tag),
            _phantom: PhantomData,
        }
    }

    pub fn wrapping_add_tag(&mut self, rhs: u16) {
        let (ptr, tag) = usize_to_ptr_tag::<T>(self.inner);
        let new_tag = tag.wrapping_add(rhs);
        self.inner = ptr_tag_to_usize(ptr, new_tag);
    }
}

pub struct AtomicTagged<T> {
    inner: AtomicUsize,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for AtomicTagged<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.load(Ordering::SeqCst);
        let (ptr, tag) = usize_to_ptr_tag::<T>(inner);
        f.debug_struct("AtomicTagged")
            .field("ptr", &ptr)
            .field("tag", &tag)
            .field("inner", &inner)
            .finish()
    }
}

impl<T> AtomicTagged<T> {
    pub const fn new(v: TaggedPtr<T>) -> AtomicTagged<T> {
        AtomicTagged {
            inner: AtomicUsize::new(v.inner),
            _phantom: PhantomData,
        }
    }

    pub fn load(&self, ord: Ordering) -> TaggedPtr<T> {
        TaggedPtr::from_inner(self.inner.load(ord))
    }

    pub fn store(&self, ptr: TaggedPtr<T>, ord: Ordering) {
        self.inner.store(ptr.inner, ord);
    }

    pub fn compare_exchange_weak(
        &self,
        current: TaggedPtr<T>,
        new: TaggedPtr<T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<TaggedPtr<T>, TaggedPtr<T>> {
        self.inner
            .compare_exchange_weak(current.inner, new.inner, success, failure)
            .map(TaggedPtr::from_inner)
            .map_err(TaggedPtr::from_inner)
    }
}

// #[test]
// fn tagged_ptr() {
//     let mut data = 100u32;

//     unsafe {
//         let tagged: TaggedPtr<u32> = TaggedPtr::new(&mut data, 0);
//         println!("Initial tag: {}", tagged.tag());

//         // Increment the version atomicaly
//         let new_v = tagged.increment_version();
//         println!("New tag: {}", new_v);
//         println!("Current tag via getter: {}", tagged.tag());

//         assert_eq!(*tagged.ptr(), 100);
//     }
// }
