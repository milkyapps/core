use std::sync::atomic::{AtomicPtr, Ordering};

const LOWER_48_BITS_SET: usize = (1 << 48) - 1;
const TAG_SHIFT: usize = 48;

fn tagged_to_ptr_tag(inner: usize) -> (usize, u16) {
    let ptr = inner & LOWER_48_BITS_SET;
    let tag = u16::try_from(inner >> TAG_SHIFT).unwrap();

    (ptr, tag)
}

fn ptr_tag_to_tagged(ptr: usize, tag: u16) -> usize {
    // SAFETY: Under the assumption that ptr always fit, so we do not need to mask it here
    ((tag as usize) << TAG_SHIFT) | ptr
}

pub struct TaggedPtr<T> {
    inner: *mut T,
}

unsafe impl<T: Send> Sync for TaggedPtr<T> {}
unsafe impl<T: Send> Send for TaggedPtr<T> {}

impl<T> std::fmt::Debug for TaggedPtr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (_, tag) = tagged_to_ptr_tag(self.inner.addr());
        f.debug_tuple("TaggedPtr")
            .field(&self.inner)
            .field(&self.inner.map_addr(|inner| {
                let (ptr, _) = tagged_to_ptr_tag(inner);
                ptr
            }))
            .field(&tag)
            .finish()
    }
}

impl<T> Clone for TaggedPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for TaggedPtr<T> {}

impl<T> TaggedPtr<T> {
    pub fn new(ptr: *mut T, tag: u16) -> TaggedPtr<T> {
        TaggedPtr {
            inner: ptr.map_addr(|ptr| ptr_tag_to_tagged(ptr, tag)),
        }
    }

    // pub fn from_inner(inner: usize) -> TaggedPtr<T> {
    //     TaggedPtr {
    //         inner,
    //         _phantom: PhantomData,
    //     }
    // }

    pub fn ptr(&self) -> *mut T {
        self.inner.map_addr(|inner| {
            let (ptr, _) = tagged_to_ptr_tag(inner);
            ptr
        })
    }

    pub unsafe fn byte_add<P>(self, n: usize) -> TaggedPtr<P> {
        let inner = self
            .inner
            .map_addr(|inner| {
                let (ptr, tag) = tagged_to_ptr_tag(inner);
                let new_ptr = ptr + n;
                ptr_tag_to_tagged(new_ptr, tag)
            })
            .cast::<P>();
        TaggedPtr { inner }
    }

    pub unsafe fn byte_sub<P>(self, n: usize) -> TaggedPtr<P> {
        let inner = self
            .inner
            .map_addr(|inner| {
                let (ptr, tag) = tagged_to_ptr_tag(inner);
                let new_ptr = ptr - n;
                ptr_tag_to_tagged(new_ptr, tag)
            })
            .cast::<P>();
        TaggedPtr { inner }
    }

    pub fn wrapping_add_tag(&mut self, rhs: u16) {
        self.inner = self.inner.map_addr(|inner| {
            let (ptr, tag) = tagged_to_ptr_tag(inner);
            let new_tag = tag.wrapping_add(rhs);
            ptr_tag_to_tagged(ptr, new_tag)
        });
    }
}

pub struct AtomicTagged<T> {
    inner: AtomicPtr<T>,
}

impl<T> std::fmt::Debug for AtomicTagged<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.load(Ordering::SeqCst);
        let (_, tag) = tagged_to_ptr_tag(inner.addr());
        f.debug_struct("AtomicTagged")
            .field(
                "ptr",
                &inner.map_addr(|inner| {
                    let (ptr, _) = tagged_to_ptr_tag(inner);
                    ptr
                }),
            )
            .field("tag", &tag)
            .field("inner", &inner)
            .finish()
    }
}

impl<T> AtomicTagged<T> {
    pub const fn new(v: TaggedPtr<T>) -> AtomicTagged<T> {
        AtomicTagged {
            inner: AtomicPtr::new(v.inner),
        }
    }

    pub fn load(&self, ord: Ordering) -> TaggedPtr<T> {
        TaggedPtr {
            inner: self.inner.load(ord),
        }
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
            .map(|inner| TaggedPtr { inner })
            .map_err(|inner| TaggedPtr { inner })
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
