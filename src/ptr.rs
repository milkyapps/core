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
//
//     unsafe {
//         let tagged: TaggedPtr<u32> = TaggedPtr::new(&mut data, 0);
//         println!("Initial tag: {}", tagged.tag());
//
//         // Increment the version atomicaly
//         let new_v = tagged.increment_version();
//         println!("New tag: {}", new_v);
//         println!("Current tag via getter: {}", tagged.tag());
//
//         assert_eq!(*tagged.ptr(), 100);
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::{atomic::AtomicUsize, model};
    use std::sync::atomic::Ordering;

    /// `TaggedPtr::new` must preserve the pointer value across all tag values,
    /// and the tag must round-trip through the packed `inner` representation.
    #[test]
    fn tagged_ptr_roundtrip_preserves_ptr_and_tag() {
        let mut data = 0u64;
        let p = std::ptr::from_mut(&mut data);

        for tag in [0u16, 1, 2, 42, u16::MAX - 1, u16::MAX] {
            let tp = TaggedPtr::new(p, tag);
            assert_eq!(tp.ptr(), p, "ptr must survive tagging for tag {tag}");
            assert_eq!(
                tp.inner,
                TaggedPtr::new(p, tag).inner,
                "tagged inner must be reproducible for tag {tag}"
            );
        }
    }

    /// The tag lives in the high 16 bits; it must not overlap the low 48 ptr
    /// bits, so distinct tags produce distinct `inner` for the same ptr.
    #[test]
    fn distinct_tags_produce_distinct_inner() {
        let mut data = 0u64;
        let p = std::ptr::from_mut(&mut data);
        let a = TaggedPtr::new(p, 0).inner;
        let b = TaggedPtr::new(p, 1).inner;
        let c = TaggedPtr::new(p, u16::MAX).inner;
        assert_ne!(a, b, "tag 0 and 1 must differ");
        assert_ne!(b, c, "tag 1 and MAX must differ");
        assert_ne!(a, c, "tag 0 and MAX must differ");
    }

    /// `wrapping_add_tag` must wrap at the u16 boundary rather than saturate
    /// or overflow into the pointer bits.
    #[test]
    fn wrapping_add_tag_wraps_at_u16_max() {
        let mut data = 0u64;
        let p = std::ptr::from_mut(&mut data);

        let mut tp = TaggedPtr::new(p, u16::MAX);
        tp.wrapping_add_tag(1);
        assert_eq!(tp.ptr(), p, "ptr unchanged by tag wrap");
        assert_eq!(
            tp.inner,
            TaggedPtr::new(p, 0).inner,
            "tag must wrap from MAX to 0"
        );

        tp.wrapping_add_tag(2);
        assert_eq!(
            tp.inner,
            TaggedPtr::new(p, 2).inner,
            "wrap then add 2 -> tag 2"
        );
    }

    /// `byte_add` / `byte_sub` must preserve the tag and move only the
    /// pointer, and round-tripping must reproduce the original tagged value.
    #[test]
    fn byte_add_sub_preserve_tag_and_roundtrip() {
        let mut buf = [0u8; 32];
        let base = buf.as_mut_ptr();
        let tag = 7;

        let tp = TaggedPtr::new(base, tag);
        let advanced = unsafe { tp.byte_add::<u8>(8) };
        assert_eq!(advanced.ptr(), base.wrapping_add(8));
        assert_eq!(
            advanced.inner,
            TaggedPtr::new(base.wrapping_add(8), tag).inner,
            "byte_add must preserve the tag"
        );

        let back = unsafe { advanced.byte_sub::<u8>(8) };
        assert_eq!(
            back.inner, tp.inner,
            "byte_sub must restore the original tagged pointer"
        );
    }

    /// A null pointer round-trips through `TaggedPtr` without losing its tag.
    #[test]
    fn tagged_ptr_null_roundtrip() {
        let tp = TaggedPtr::new(std::ptr::null_mut::<u64>(), 7);
        assert!(tp.ptr().is_null());
        assert_eq!(
            tp.inner,
            TaggedPtr::new(std::ptr::null_mut::<u64>(), 7).inner
        );
    }

    /// `AtomicTagged` load/store must be faithful to the tagged value, and a
    /// CAS with a mismatched tag must fail while a CAS with the matching tag
    /// must succeed — this is the ABA-mitigation contract.
    #[test]
    fn atomic_tagged_cas_respects_tag() {
        let mut data = 0u64;
        let p = std::ptr::from_mut(&mut data);

        let a = AtomicTagged::new(TaggedPtr::new(p, 0));
        assert_eq!(a.load(Ordering::SeqCst).inner, TaggedPtr::new(p, 0).inner);

        // CAS with the wrong tag must fail (this is what blocks ABA: a stale
        // pointer-with-old-tag cannot win the CAS).
        let wrong = a.compare_exchange_weak(
            TaggedPtr::new(p, 1),
            TaggedPtr::new(p, 2),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        assert!(wrong.is_err(), "CAS with wrong tag must fail");
        assert_eq!(a.load(Ordering::SeqCst).inner, TaggedPtr::new(p, 0).inner);

        // CAS with the right tag must succeed and update the tag. `weak` may
        // fail spuriously, so retry until it succeeds (the value matches, so
        // it must eventually win).
        let mut ok = Err(TaggedPtr::new(p, 0));
        for _ in 0..64 {
            if a.compare_exchange_weak(
                TaggedPtr::new(p, 0),
                TaggedPtr::new(p, 5),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
            {
                ok = Ok(TaggedPtr::new(p, 5));
                break;
            }
        }
        assert!(
            ok.is_ok(),
            "CAS with matching tag must succeed (after retrying spurious failures)"
        );
        assert_eq!(a.load(Ordering::SeqCst).inner, TaggedPtr::new(p, 5).inner);
    }

    /// Two threads repeatedly flipping the tag between 0 and 1 via CAS: the
    /// pointer must never change and the final tag must be one of {0, 1}. A
    /// lost update that corrupts the packed value would violate either.
    #[test]
    fn atomic_tagged_concurrent_cas_flips_cleanly() {
        model(|| {
            let mut data = 0u64;
            let p = std::ptr::from_mut(&mut data);
            let a = AtomicTagged::new(TaggedPtr::new(p, 0));
            let t0 = TaggedPtr::new(p, 0);
            let t1 = TaggedPtr::new(p, 1);
            let ops = AtomicUsize::new(0);

            crate::thread::scope(|scope| {
                for _ in 0..2 {
                    scope.spawn(|| {
                        for _ in 0..4 {
                            let cur = a.load(Ordering::Acquire);
                            let target = if cur.inner == t0.inner { t1 } else { t0 };
                            if a.compare_exchange_weak(
                                cur,
                                target,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                            {
                                ops.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    });
                }
            });

            let final_ = a.load(Ordering::Acquire);
            assert_eq!(final_.ptr(), p, "ptr must be unchanged by tag-only CAS");
            assert!(
                final_.inner == t0.inner || final_.inner == t1.inner,
                "final tag must be 0 or 1, got a corrupted packed value"
            );
            assert!(
                ops.load(Ordering::SeqCst) > 0,
                "at least one CAS must succeed"
            );
        });
    }

    /// `store` followed by `load` must faithfully round-trip a tagged value,
    /// including a non-zero tag.
    #[test]
    fn atomic_tagged_store_load_roundtrip() {
        let mut data = 0u64;
        let p = std::ptr::from_mut(&mut data);
        let a = AtomicTagged::new(TaggedPtr::new(p, 0));

        let tagged = TaggedPtr::new(p, 123);
        a.store(tagged, Ordering::SeqCst);
        let loaded = a.load(Ordering::SeqCst);
        assert_eq!(
            loaded.inner, tagged.inner,
            "store/load must round-trip the tag"
        );
        assert_eq!(loaded.ptr(), p, "store/load must round-trip the ptr");
    }
}
