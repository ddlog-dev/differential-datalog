//! Data types used in debugging hooks.

use program::RelId;
use record::Record;
use std::fmt;

/// Identifies an operator inside a DDlog program.
///
/// * `relid` - Relation whose rule is being fired.
///
/// * `ruleidx` - Index of a rule for this relation.
///
/// * `opidx` - Index of an operator in the body of the rule.
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

/// Context within which each operator is evaluated.  This only
/// includes values that the compiler maintains for the given
/// operator, as opposed to all values in the current syntactic scope.
/// For example, variables that are declared in previous literals
/// but are not used in the rest of the rule are not included
/// in the context.
#[derive(Debug)]
pub enum Operands {
    /// FlatMap: variables declared in previous operators that
    /// are used in the FlatMap operator or in the rest of the rule.
    FlatMap { vars: Vec<Record> },
    /// Filter: variables declared in previous operators that
    /// are used in the Filter operator or in the rest of the rule.
    Filter { vars: Vec<Record> },
    /// Join:
    /// * `prefix_vars` - variables declared in previous operators
    /// that are used in operators **following** the join.
    /// * `val` - record being joined with.
    Join {
        prefix_vars: Vec<Record>,
        val: Record,
    },
    /// Semijoin:
    /// * `key` - arrangement key.
    /// * `prefix_vars` - variables declared in previous operators
    /// that are used in operators **following** the join.
    Semijoin {
        key: Record,
        prefix_vars: Vec<Record>,
    },
    /// Antijoin:
    /// * `key` - arrangement key.
    /// * `prefix_vars` - variables declared in previous operators
    /// that are used in operators **following** the join.
    Antijoin {
        key: Record,
        prefix_vars: Vec<Record>,
    },
    /// Aggregate:
    /// * `group_by` - group-by variables.
    /// * `group` - all values in a group.
    Aggregate {
        group_by: Vec<Record>,
        group: Vec<Record>,
    },
}

pub struct Debugger {
    pub name: String,
    pub event: Box<dyn Fn(DebugEvent) + Send>,
}

impl fmt::Debug for Debugger {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Debugger ({})", self.name)
    }
}
