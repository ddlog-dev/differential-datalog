//! Data types used in debugging hooks.

use differential_datalog::program::RelId;
use differential_datalog::record::Record;

/// Identifies an operator inside a DDlog program.
///
/// * `relid` - Relation whose rule is being fired.
///
/// * `ruleidx` - Index of a rule for this relation.
///
/// * `opidx` - Index of an operator in the body of the rule.
///
#[derive(Debug)]
pub struct OpId {
    pub relid: RelId,
    pub ruleidx: usize,
    pub opidx: usize,
}

/// An event inside a DDlog program that must be reported to the debugger.
///
/// * `RelationUpdate` - similar to a **watchpoint** activation in a
/// conventional debugger, this event notifies the debugger that a value has
/// been added or removed from a relation.
///
/// * `Activation` - similar to a **breakpoint** activation in a conventional
/// debugger, this event notifies the debugger that an operator in one of
/// program's rules has been activated.
///
#[derive(Debug)]
pub enum DebugEvent {
    RelationUpdate {
        /// Relation being modified.
        relid: RelId,
        /// Value being added to or removed from the relation.
        val: Record,
        /// The number of derivations of `val` being added (`diff>0`) or removed (`diff<0`).
        /// removed (`diff<0`).
        diff: isize,
    },
    Activation {
        /// Operator being triggered.
        opid: OpId,
        /// Arguments to the operator.
        operands: Operands,
    },
}

/// Operands for each operator type.
#[derive(Debug)]
pub enum Operands {
    /// FlatMap: variables declared in previous operators that
    /// are used in the rest of the rule.
    FlatMap {
        vars: Vec<Record>,
    },
    /// Filter: variables declared in previous operators that
    /// are used in the rest of the rule.
    Filter {
        vars: Vec<Record>,
    },
    /// Join: variables declared in previous operators that
    /// are used in the rest of the rule.
    Join {
        prefix_vars: Vec<Record>,
        val: Record,
    },
    Semijoin {
        prefix_vars: Vec<Record>,
    },
    Antijoin {
        post_vars: Vec<Record>,
    },
    Aggregate {
        key: Vec<Record>,
        group: Vec<Record>,
    },
}
