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
    inner: AtomicUsize,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for TaggedPtr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.load(Ordering::SeqCst);
        let (ptr, tag) = usize_to_ptr_tag::<T>(inner);
        f.debug_struct("TaggedPtr")
            .field("ptr", &ptr)
            .field("tag", &tag)
            .field("inner", &inner)
            .finish()
    }
}

impl<T> TaggedPtr<T> {
    /// Create a new TaggedPtr from a raw pointer and a 16-bit tag.
    ///
    /// # Safety
    /// The provided pointer must be valid for the lifetime of the TaggedPtr.
    pub fn new(ptr: *mut T, tag: u16) -> Self {
        let addr = ptr as usize;

        assert!(
            addr <= LOWER_48_BITS_SET,
            "Pointer address exceeds 48-bit limit"
        );

        Self {
            inner: AtomicUsize::new(ptr_tag_to_usize(ptr, tag)),
            _phantom: PhantomData,
        }
    }

    pub fn from_inner(inner: usize) -> TaggedPtr<T> {
        TaggedPtr {
            inner: AtomicUsize::new(inner),
            _phantom: PhantomData::default(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.load().0.is_null()
    }

    pub fn load(&self) -> (*mut T, usize) {
        let inner = self.inner.load(Ordering::Relaxed);
        let (ptr, _) = usize_to_ptr_tag(inner);
        (ptr, inner)
    }

    pub fn store(&self, inner: usize) {
        self.inner.store(inner, Ordering::Release);
    }

    /// Returns the 16-bit version/tag stored in the high bits.
    pub fn tag(&self) -> u16 {
        let inner = self.inner.load(Ordering::Relaxed);
        usize_to_ptr_tag::<T>(inner).1
    }

    // pub fn update_and_wrapping_add_tag(
    //     &self,
    //     set_order: Ordering,
    //     fetch_order: Ordering,
    //     mut f: impl FnMut(*mut T) -> *mut T,
    // ) {
    //     self.inner.update(set_order, fetch_order, |current| {
    //         let old_ptr = (current & LOWER_48_BITS_SET) as *mut T;
    //         let old_tag = (current >> TAG_SHIFT_LOWER_16_BITS) as u16;

    //         let new_tag = old_tag.wrapping_add(1);
    //         let new_ptr = f(old_ptr) as usize;

    //         ((new_tag as usize) << TAG_SHIFT_LOWER_16_BITS) | new_ptr
    //     });
    // }

    // pub fn store_and_wrapping_incr_tag(&self, ptr: *mut T) {
    //     let current = self.inner.load(Ordering::Acquire);

    //     let old_tag = (current >> Self::TAG_SHIFT_LOWER_16_BITS) as u16;
    //     let new_tag = old_tag.wrapping_add(1);

    //     self.inner.store(
    //         ((new_tag as usize) << Self::TAG_SHIFT_LOWER_16_BITS) | (ptr as usize),
    //         Ordering::Release,
    //     );
    // }

    pub unsafe fn byte_add<P>(&self, count: usize) -> TaggedPtr<P> {
        let inner = self.inner.load(Ordering::Acquire);
        let (ptr, tag) = usize_to_ptr_tag::<P>(inner);
        let inner = ptr_tag_to_usize(unsafe { ptr.byte_add(count) }, tag);

        TaggedPtr {
            inner: AtomicUsize::new(inner),
            _phantom: PhantomData::default(),
        }
    }

    pub unsafe fn byte_sub<P>(&self, count: usize) -> TaggedPtr<P> {
        let inner = self.inner.load(Ordering::Acquire);
        let (ptr, tag) = usize_to_ptr_tag::<P>(inner);
        let inner = ptr_tag_to_usize(unsafe { ptr.byte_sub(count) }, tag);

        TaggedPtr {
            inner: AtomicUsize::new(inner),
            _phantom: PhantomData::default(),
        }
    }

    pub fn compare_exchange_weak(
        &self,
        current: usize,
        new: usize,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(*mut T, u16, usize), usize> {
        match self
            .inner
            .compare_exchange_weak(current, new, success, failure)
        {
            Ok(inner) => {
                let (ptr, tag) = usize_to_ptr_tag(inner);
                Ok((ptr, tag, inner))
            }
            Err(inner) => Err(inner),
        }
    }

    pub(crate) fn incr_tag(&self) {
        let inner = self.inner.load(Ordering::Acquire);
        let (ptr, tag) = usize_to_ptr_tag::<T>(inner);
        let inner = ptr_tag_to_usize(ptr, tag.wrapping_add(1));
        self.inner.store(inner, Ordering::Release);
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
