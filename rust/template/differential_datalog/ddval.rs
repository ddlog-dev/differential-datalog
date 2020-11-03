//! DDValue: Generic value type stored in all differential-dataflow relations.
//!
//! Rationale: Differential dataflow allows the user to assign an arbitrary user-defined type to
//! each collection.  It relies on Rust's static dispatch mechanism to specialize its internal
//! machinery for each user-defined type.  Unfortunately, beyond very simple programs this leads to
//! extremely long compilation times.  One workaround that we used to rely on is to declare a
//! single enum type with a variant per concrete type used in at least one relation.  This make
//! compilation feasible, but still very slow (~6 minutes for a simple DDlog program and ~10
//! minutes for complex programs).
//!
//! Another alternative we implement here is to use a fixed value type that does not depend on
//! a concrete DDlog program and rely on dynamic dispatch to forward operations that DD expects
//! all values to implement (comparison, hashing, etc.) to their concrete implementations.  This
//! way this crate (differential-datalog) can be compiled all the way to binary code separately
//! from the DDlog program using it and does not need to be re-compiled when the DDlog program
//! changes.  Thus, the only part that must be re-compiled on changes to the DDlog code is the
//! auto-generated crate that declares concrete value types and rules.  This is much faster than
//! re-compiling both crates together.
//!
//! The next design decision is how to implement dynamic dispatch.  Rust trait objects is an
//! obvious choice, with value type being declared as `Box<dyn SomeTrait>`.  However, this proved
//! suboptimal in our experiments, as this design requires a dynamic memory allocation per value,
//! no matter how small.  Furthermore, cloning a value (which DD does a lot, e.g., during
//! compaction) requires another allocation.
//!
//! We improve over this naive design in two ways.  First, we use `Arc` instead of `Box`, which
//! introduces extra space overhead to store the reference count, but avoids memory allocation due
//! to cloning and shares the same heap allocation across multiple copies of the value.  Second, we
//! store small objects <=`usize` bytes inline instead of wrapping them in an Arc to avoid dynamic
//! memory allocation for such objects altogether.  Unfortunately, Rust's dynamic dispatch
//! mechanism does not support this, so we roll our own instead, with the following `DDValue`
//! declaration:
//!
//! ```
//! use differential_datalog::ddval::*;
//! pub struct DDValue {
//!    val: DDVal,
//!    vtable: &'static DDValMethods,
//! }
//! ```
//!
//! where `DDVal` is a `usize` that stores either an `Arc<T>` or `T` (where `T` is the actual type
//! of value stored in the DDlog relation), and `DDValMethods` is a virtual table of methods that
//! must be implemented for all DD values.
//!
//! This design still requires a separate heap allocation for each value >8 bytes, which slows
//! things down quite a bit.  Nevertheless, it has the same performance as our earlier
//! implementation using static dispatch and at least in some benchmarks uses less memory.  The
//! only way to improve things further I can think of is to somehow co-design this with DD to use
//! DD's knowledge of the context where a value is being created to, e.g., allocate blocks of
//! values when possible.
//!

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;

use serde::ser::Serialize;
use serde::ser::Serializer;

use abomonation::Abomonation;

use crate::record::IntoRecord;
use crate::record::Mutator;
use crate::record::Record;

/// Type-erased representation of a value.  Can store the actual value or a pointer to it.
/// This could be just a `usize`, but we wrap it in a struct as we don't want it to implement
/// `Copy`.
pub struct DDVal {
    pub v: usize,
}

/// DDValue: this type is stored in all DD collections.
/// It consists of value and associated vtable.
pub struct DDValue {
    pub val: DDVal,
    pub vtable: &'static DDValMethods,
}

/// vtable of methods to be implemented by every value stored in DD.
pub struct DDValMethods {
    pub clone: fn(this: &DDVal) -> DDVal,
    pub into_record: fn(this: DDVal) -> Record,
    pub eq: fn(this: &DDVal, other: &DDVal) -> bool,
    pub partial_cmp: fn(this: &DDVal, other: &DDVal) -> Option<std::cmp::Ordering>,
    pub cmp: fn(this: &DDVal, other: &DDVal) -> std::cmp::Ordering,
    pub hash: fn(this: &DDVal, state: &mut dyn Hasher),
    pub mutate: fn(this: &mut DDVal, record: &Record) -> Result<(), String>,
    pub fmt_debug: fn(this: &DDVal, f: &mut Formatter) -> Result<(), std::fmt::Error>,
    pub fmt_display: fn(this: &DDVal, f: &mut Formatter) -> Result<(), std::fmt::Error>,
    pub drop: fn(this: &mut DDVal),
    pub ddval_serialize: fn(this: &DDVal) -> &dyn erased_serde::Serialize,
}

impl Drop for DDValue {
    fn drop(&mut self) {
        (self.vtable.drop)(&mut self.val);
    }
}

impl DDValue {
    pub fn new(val: DDVal, vtable: &'static DDValMethods) -> DDValue {
        DDValue { val, vtable }
    }

    pub fn into_ddval(self) -> DDVal {
        let res = DDVal { v: self.val.v };
        std::mem::forget(self);
        res
    }
}

impl Mutator<DDValue> for Record {
    fn mutate(&self, x: &mut DDValue) -> Result<(), String> {
        (x.vtable.mutate)(&mut x.val, self)
    }
}

impl IntoRecord for DDValue {
    fn into_record(self) -> Record {
        (self.vtable.into_record)(self.into_ddval())
    }
}

impl Abomonation for DDValue {
    unsafe fn entomb<W: std::io::Write>(&self, _write: &mut W) -> std::io::Result<()> {
        panic!("DDValue::entomb: not implemented")
    }
    unsafe fn exhume<'a, 'b>(&'a mut self, _bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        panic!("DDValue::exhume: not implemented")
    }
    fn extent(&self) -> usize {
        panic!("DDValue::extent: not implemented")
    }
}

/* `Serialize` implementation simply forwards the `serialize` operation to the
 * inner object.
 * Note: we cannot provide a generic `Deserialize` implementation for `DDValue`,
 * as there is no object to forward `deserialize` to.  Instead, we are going
 * generate a `Deserialize` implementation for `Update<DDValue>` in the DDlog
 * compiler. This implementation will use relation id inside `Update` to figure
 * out which type to deserialize.  See `src/lib.rs` for more details.
 */
impl Serialize for DDValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        erased_serde::serialize((self.vtable.ddval_serialize)(&self.val), serializer)
    }
}

impl Display for DDValue {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        (self.vtable.fmt_display)(&self.val, f)
    }
}

impl Debug for DDValue {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        (self.vtable.fmt_debug)(&self.val, f)
    }
}

impl PartialOrd for DDValue {
    fn partial_cmp(&self, other: &DDValue) -> Option<std::cmp::Ordering> {
        (self.vtable.partial_cmp)(&self.val, &other.val)
    }
}

impl PartialEq for DDValue {
    fn eq(&self, other: &Self) -> bool {
        (self.vtable.eq)(&self.val, &other.val)
    }
}

impl Eq for DDValue {}

impl Ord for DDValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.vtable.cmp)(&self.val, &other.val)
    }
}

impl Clone for DDValue {
    fn clone(&self) -> Self {
        DDValue {
            val: (self.vtable.clone)(&self.val),
            vtable: self.vtable,
        }
    }
}

impl Hash for DDValue {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        (self.vtable.hash)(&self.val, state)
    }
}
