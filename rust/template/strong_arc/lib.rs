// Copyright 2012-2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Fork of Arc for Servo. This has the following advantages over std::sync::Arc:
//!
//! * We don't waste storage on the weak reference count.
//! * We don't do extra RMU operations to handle the possibility of weak references.
//! * We can experiment with arena allocation (todo).
//! * We can add methods to support our custom use cases [1].
//! * We have support for dynamically-sized types (see from_header_and_iter).
//! * We have support for thin arcs to unsized types (see ThinArc).
//! * We have support for references to static data, which don't do any
//!   refcounting.
//!
//! [1]: https://bugzilla.mozilla.org/show_bug.cgi?id=1360883

// The semantics of `Arc` are already documented in the Rust docs, so we don't
// duplicate those here.
#![allow(missing_docs)]

use std::alloc::{self, Layout};
use std::borrow;
use std::cmp::Ordering;
use std::convert::From;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem::{self, align_of, size_of};
use std::ops::{Deref, DerefMut};
use std::os::raw::c_void;
use std::process;
use std::ptr;
use std::sync::atomic;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::{i32, u32};

/// A soft limit on the amount of references that may be made to an `Arc`.
///
/// Going above this limit will abort your program (although not
/// necessarily) at _exactly_ `MAX_REFCOUNT + 1` references.
const MAX_REFCOUNT: u32 = (i32::MAX) as u32;

/// Special refcount value that means the data is not reference counted,
/// and that the `Arc` is really acting as a read-only static reference.
const STATIC_REFCOUNT: u32 = u32::MAX;

/// An atomically reference counted shared pointer
///
/// See the documentation for [`Arc`] in the standard library. Unlike the
/// standard library `Arc`, this `Arc` does not support weak reference counting.
///
/// See the discussion in https://github.com/rust-lang/rust/pull/60594 for the
/// usage of PhantomData.
///
/// [`Arc`]: https://doc.rust-lang.org/stable/std/sync/struct.Arc.html
///
/// cbindgen:derive-eq=false
/// cbindgen:derive-neq=false
#[repr(C)]
pub struct Arc<T: ?Sized> {
    p: ptr::NonNull<ArcInner<T>>,
    phantom: PhantomData<T>,
}

/// An `Arc` that is known to be uniquely owned
///
/// When `Arc`s are constructed, they are known to be
/// uniquely owned. In such a case it is safe to mutate
/// the contents of the `Arc`. Normally, one would just handle
/// this by mutating the data on the stack before allocating the
/// `Arc`, however it's possible the data is large or unsized
/// and you need to heap-allocate it earlier in such a way
/// that it can be freely converted into a regular `Arc` once you're
/// done.
///
/// `UniqueArc` exists for this purpose, when constructed it performs
/// the same allocations necessary for an `Arc`, however it allows mutable access.
/// Once the mutation is finished, you can call `.shareable()` and get a regular `Arc`
/// out of it.
///
/// Ignore the doctest below there's no way to skip building with refcount
/// logging during doc tests (see rust-lang/rust#45599).
///
/// ```rust,ignore
/// # use servo_arc::UniqueArc;
/// let data = [1, 2, 3, 4, 5];
/// let mut x = UniqueArc::new(data);
/// x[4] = 7; // mutate!
/// let y = x.shareable(); // y is an Arc<T>
/// ```
pub struct UniqueArc<T: ?Sized>(Arc<T>);

impl<T> UniqueArc<T> {
    #[inline]
    /// Construct a new UniqueArc
    pub fn new(data: T) -> Self {
        UniqueArc(Arc::new(data))
    }

    /// Construct an uninitialized arc
    #[inline]
    pub fn new_uninit() -> UniqueArc<mem::MaybeUninit<T>> {
        unsafe {
            let layout = Layout::new::<ArcInner<mem::MaybeUninit<T>>>();
            let ptr = alloc::alloc(layout);
            let mut p = ptr::NonNull::new(ptr)
                .unwrap_or_else(|| alloc::handle_alloc_error(layout))
                .cast::<ArcInner<mem::MaybeUninit<T>>>();
            ptr::write(&mut p.as_mut().count, atomic::AtomicU32::new(1));

            #[cfg(feature = "gecko_refcount_logging")]
            {
                NS_LogCtor(p.as_ptr() as *mut _, b"ServoArc\0".as_ptr() as *const _, 8)
            }

            UniqueArc(Arc {
                p,
                phantom: PhantomData,
            })
        }
    }

    #[inline]
    /// Convert to a shareable Arc<T> once we're done mutating it
    pub fn shareable(self) -> Arc<T> {
        self.0
    }
}

impl<T> UniqueArc<mem::MaybeUninit<T>> {
    /// Convert to an initialized Arc.
    #[inline]
    pub unsafe fn assume_init(this: Self) -> UniqueArc<T> {
        UniqueArc(Arc {
            p: mem::ManuallyDrop::new(this).0.p.cast(),
            phantom: PhantomData,
        })
    }
}

impl<T> Deref for UniqueArc<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &*self.0
    }
}

impl<T> DerefMut for UniqueArc<T> {
    fn deref_mut(&mut self) -> &mut T {
        // We know this to be uniquely owned
        unsafe { &mut (*self.0.ptr()).data }
    }
}

unsafe impl<T: ?Sized + Sync + Send> Send for Arc<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for Arc<T> {}

/// The object allocated by an Arc<T>
#[repr(C)]
struct ArcInner<T: ?Sized> {
    count: atomic::AtomicU32,
    data: T,
}

unsafe impl<T: ?Sized + Sync + Send> Send for ArcInner<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for ArcInner<T> {}

/// Computes the offset of the data field within ArcInner.
fn data_offset<T>() -> usize {
    let size = size_of::<ArcInner<()>>();
    let align = align_of::<T>();
    // https://github.com/rust-lang/rust/blob/1.36.0/src/libcore/alloc.rs#L187-L207
    size.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1)
}

impl<T> Arc<T> {
    /// Construct an `Arc<T>`
    #[inline]
    pub fn new(data: T) -> Self {
        let ptr = Box::into_raw(Box::new(ArcInner {
            count: atomic::AtomicU32::new(1),
            data,
        }));

        #[cfg(feature = "gecko_refcount_logging")]
        unsafe {
            // FIXME(emilio): Would be so amazing to have
            // std::intrinsics::type_name() around, so that we could also report
            // a real size.
            NS_LogCtor(ptr as *mut _, b"ServoArc\0".as_ptr() as *const _, 8);
        }

        unsafe {
            Arc {
                p: ptr::NonNull::new_unchecked(ptr),
                phantom: PhantomData,
            }
        }
    }

    /// Construct an intentionally-leaked arc.
    #[inline]
    pub fn new_leaked(data: T) -> Self {
        let arc = Self::new(data);
        arc.mark_as_intentionally_leaked();
        arc
    }

    /// Convert the Arc<T> to a raw pointer, suitable for use across FFI
    ///
    /// Note: This returns a pointer to the data T, which is offset in the allocation.
    ///
    /// It is recommended to use RawOffsetArc for this.
    #[inline]
    pub fn into_raw(self) -> *const T {
        let ptr = unsafe { &((*self.ptr()).data) as *const _ };
        mem::forget(self);
        ptr
    }

    /// Reconstruct the Arc<T> from a raw pointer obtained from into_raw()
    ///
    /// Note: This raw pointer will be offset in the allocation and must be preceded
    /// by the atomic count.
    ///
    /// It is recommended to use RawOffsetArc for this
    #[inline]
    pub unsafe fn from_raw(ptr: *const T) -> Self {
        // To find the corresponding pointer to the `ArcInner` we need
        // to subtract the offset of the `data` field from the pointer.
        let ptr = (ptr as *const u8).sub(data_offset::<T>());
        Arc {
            p: ptr::NonNull::new_unchecked(ptr as *mut ArcInner<T>),
            phantom: PhantomData,
        }
    }

    /// Create a new static Arc<T> (one that won't reference count the object)
    /// and place it in the allocation provided by the specified `alloc`
    /// function.
    ///
    /// `alloc` must return a pointer into a static allocation suitable for
    /// storing data with the `Layout` passed into it. The pointer returned by
    /// `alloc` will not be freed.
    #[inline]
    pub unsafe fn new_static<F>(alloc: F, data: T) -> Arc<T>
    where
        F: FnOnce(Layout) -> *mut u8,
    {
        let ptr = alloc(Layout::new::<ArcInner<T>>()) as *mut ArcInner<T>;

        let x = ArcInner {
            count: atomic::AtomicU32::new(STATIC_REFCOUNT),
            data,
        };

        ptr::write(ptr, x);

        Arc {
            p: ptr::NonNull::new_unchecked(ptr),
            phantom: PhantomData,
        }
    }

    /// Returns the address on the heap of the Arc itself -- not the T within it -- for memory
    /// reporting.
    ///
    /// If this is a static reference, this returns null.
    pub fn heap_ptr(&self) -> *const c_void {
        if self.inner().count.load(Relaxed) == STATIC_REFCOUNT {
            ptr::null()
        } else {
            self.p.as_ptr() as *const ArcInner<T> as *const c_void
        }
    }
}

impl<T: ?Sized> Arc<T> {
    #[inline]
    fn inner(&self) -> &ArcInner<T> {
        // This unsafety is ok because while this arc is alive we're guaranteed
        // that the inner pointer is valid. Furthermore, we know that the
        // `ArcInner` structure itself is `Sync` because the inner data is
        // `Sync` as well, so we're ok loaning out an immutable pointer to these
        // contents.
        unsafe { &*self.ptr() }
    }

    #[inline(always)]
    fn record_drop(&self) {
        #[cfg(feature = "gecko_refcount_logging")]
        unsafe {
            NS_LogDtor(self.ptr() as *mut _, b"ServoArc\0".as_ptr() as *const _, 8);
        }
    }

    /// Marks this `Arc` as intentionally leaked for the purposes of refcount
    /// logging.
    ///
    /// It's a logic error to call this more than once, but it's not unsafe, as
    /// it'd just report negative leaks.
    #[inline(always)]
    pub fn mark_as_intentionally_leaked(&self) {
        self.record_drop();
    }

    // Non-inlined part of `drop`. Just invokes the destructor and calls the
    // refcount logging machinery if enabled.
    #[inline(never)]
    unsafe fn drop_slow(&mut self) {
        self.record_drop();
        let _ = Box::from_raw(self.ptr());
    }

    /// Test pointer equality between the two Arcs, i.e. they must be the _same_
    /// allocation
    #[inline]
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        this.ptr() == other.ptr()
    }

    fn ptr(&self) -> *mut ArcInner<T> {
        self.p.as_ptr()
    }
}

#[cfg(feature = "gecko_refcount_logging")]
extern "C" {
    fn NS_LogCtor(
        aPtr: *mut std::os::raw::c_void,
        aTypeName: *const std::os::raw::c_char,
        aSize: u32,
    );
    fn NS_LogDtor(
        aPtr: *mut std::os::raw::c_void,
        aTypeName: *const std::os::raw::c_char,
        aSize: u32,
    );
}

impl<T: ?Sized> Clone for Arc<T> {
    #[inline]
    fn clone(&self) -> Self {
        // NOTE(emilio): If you change anything here, make sure that the
        // implementation in layout/style/ServoStyleConstsInlines.h matches!
        //
        // Using a relaxed ordering to check for STATIC_REFCOUNT is safe, since
        // `count` never changes between STATIC_REFCOUNT and other values.
        if self.inner().count.load(Relaxed) != STATIC_REFCOUNT {
            // Using a relaxed ordering is alright here, as knowledge of the
            // original reference prevents other threads from erroneously deleting
            // the object.
            //
            // As explained in the [Boost documentation][1], Increasing the
            // reference counter can always be done with memory_order_relaxed: New
            // references to an object can only be formed from an existing
            // reference, and passing an existing reference from one thread to
            // another must already provide any required synchronization.
            //
            // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
            let old_size = self.inner().count.fetch_add(1, Relaxed);

            // However we need to guard against massive refcounts in case someone
            // is `mem::forget`ing Arcs. If we don't do this the count can overflow
            // and users will use-after free. We racily saturate to `isize::MAX` on
            // the assumption that there aren't ~2 billion threads incrementing
            // the reference count at once. This branch will never be taken in
            // any realistic program.
            //
            // We abort because such a program is incredibly degenerate, and we
            // don't care to support it.
            if old_size > MAX_REFCOUNT {
                process::abort();
            }
        }

        unsafe {
            Arc {
                p: ptr::NonNull::new_unchecked(self.ptr()),
                phantom: PhantomData,
            }
        }
    }
}

impl<T: ?Sized> Deref for Arc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.inner().data
    }
}

impl<T: Clone> Arc<T> {
    /// Makes a mutable reference to the `Arc`, cloning if necessary
    ///
    /// This is functionally equivalent to [`Arc::make_mut`][mm] from the standard library.
    ///
    /// If this `Arc` is uniquely owned, `make_mut()` will provide a mutable
    /// reference to the contents. If not, `make_mut()` will create a _new_ `Arc`
    /// with a copy of the contents, update `this` to point to it, and provide
    /// a mutable reference to its contents.
    ///
    /// This is useful for implementing copy-on-write schemes where you wish to
    /// avoid copying things if your `Arc` is not shared.
    ///
    /// [mm]: https://doc.rust-lang.org/stable/std/sync/struct.Arc.html#method.make_mut
    #[inline]
    pub fn make_mut(this: &mut Self) -> &mut T {
        if !this.is_unique() {
            // Another pointer exists; clone
            *this = Arc::new((**this).clone());
        }

        unsafe {
            // This unsafety is ok because we're guaranteed that the pointer
            // returned is the *only* pointer that will ever be returned to T. Our
            // reference count is guaranteed to be 1 at this point, and we required
            // the Arc itself to be `mut`, so we're returning the only possible
            // reference to the inner data.
            &mut (*this.ptr()).data
        }
    }
}

impl<T: ?Sized> Arc<T> {
    /// Provides mutable access to the contents _if_ the `Arc` is uniquely owned.
    #[inline]
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        if this.is_unique() {
            unsafe {
                // See make_mut() for documentation of the threadsafety here.
                Some(&mut (*this.ptr()).data)
            }
        } else {
            None
        }
    }

    /// Whether or not the `Arc` is a static reference.
    #[inline]
    pub fn is_static(&self) -> bool {
        // Using a relaxed ordering to check for STATIC_REFCOUNT is safe, since
        // `count` never changes between STATIC_REFCOUNT and other values.
        self.inner().count.load(Relaxed) == STATIC_REFCOUNT
    }

    /// Whether or not the `Arc` is uniquely owned (is the refcount 1?) and not
    /// a static reference.
    #[inline]
    pub fn is_unique(&self) -> bool {
        // See the extensive discussion in [1] for why this needs to be Acquire.
        //
        // [1] https://github.com/servo/servo/issues/21186
        self.inner().count.load(Acquire) == 1
    }
}

impl<T: ?Sized> Drop for Arc<T> {
    #[inline]
    fn drop(&mut self) {
        // NOTE(emilio): If you change anything here, make sure that the
        // implementation in layout/style/ServoStyleConstsInlines.h matches!
        if self.is_static() {
            return;
        }

        // Because `fetch_sub` is already atomic, we do not need to synchronize
        // with other threads unless we are going to delete the object.
        if self.inner().count.fetch_sub(1, Release) != 1 {
            return;
        }

        // FIXME(bholley): Use the updated comment when [2] is merged.
        //
        // This load is needed to prevent reordering of use of the data and
        // deletion of the data.  Because it is marked `Release`, the decreasing
        // of the reference count synchronizes with this `Acquire` load. This
        // means that use of the data happens before decreasing the reference
        // count, which happens before this load, which happens before the
        // deletion of the data.
        //
        // As explained in the [Boost documentation][1],
        //
        // > It is important to enforce any possible access to the object in one
        // > thread (through an existing reference) to *happen before* deleting
        // > the object in a different thread. This is achieved by a "release"
        // > operation after dropping a reference (any access to the object
        // > through this reference must obviously happened before), and an
        // > "acquire" operation before deleting the object.
        //
        // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
        // [2]: https://github.com/rust-lang/rust/pull/41714
        self.inner().count.load(Acquire);

        unsafe {
            self.drop_slow();
        }
    }
}

impl<T: ?Sized + PartialEq> PartialEq for Arc<T> {
    fn eq(&self, other: &Arc<T>) -> bool {
        Self::ptr_eq(self, other) || *(*self) == *(*other)
    }
}

impl<T: ?Sized + PartialOrd> PartialOrd for Arc<T> {
    fn partial_cmp(&self, other: &Arc<T>) -> Option<Ordering> {
        (**self).partial_cmp(&**other)
    }

    fn lt(&self, other: &Arc<T>) -> bool {
        *(*self) < *(*other)
    }

    fn le(&self, other: &Arc<T>) -> bool {
        *(*self) <= *(*other)
    }

    fn gt(&self, other: &Arc<T>) -> bool {
        *(*self) > *(*other)
    }

    fn ge(&self, other: &Arc<T>) -> bool {
        *(*self) >= *(*other)
    }
}
impl<T: ?Sized + Ord> Ord for Arc<T> {
    fn cmp(&self, other: &Arc<T>) -> Ordering {
        (**self).cmp(&**other)
    }
}
impl<T: ?Sized + Eq> Eq for Arc<T> {}

impl<T: ?Sized + fmt::Display> fmt::Display for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized> fmt::Pointer for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Pointer::fmt(&self.ptr(), f)
    }
}

impl<T: Default> Default for Arc<T> {
    fn default() -> Arc<T> {
        Arc::new(Default::default())
    }
}

impl<T: ?Sized + Hash> Hash for Arc<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}

impl<T> From<T> for Arc<T> {
    #[inline]
    fn from(t: T) -> Self {
        Arc::new(t)
    }
}

impl<T: ?Sized> borrow::Borrow<T> for Arc<T> {
    #[inline]
    fn borrow(&self) -> &T {
        &**self
    }
}

impl<T: ?Sized> AsRef<T> for Arc<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        &**self
    }
}

#[cfg(feature = "servo")]
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Arc<T> {
    fn deserialize<D>(deserializer: D) -> Result<Arc<T>, D::Error>
    where
        D: ::serde::de::Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Arc::new)
    }
}

#[cfg(feature = "servo")]
impl<T: Serialize> Serialize for Arc<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        (**self).serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Drop;
    use std::sync::atomic;
    use std::sync::atomic::Ordering::SeqCst;

    #[derive(PartialEq)]
    struct Canary(*mut atomic::AtomicU32);

    impl Drop for Canary {
        fn drop(&mut self) {
            unsafe {
                (*self.0).fetch_add(1, SeqCst);
            }
        }
    }
}
