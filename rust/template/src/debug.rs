/// Identifies an operator inside a DDlog program.
///
/// * `relid` - Relation whose rule is being fired.
///
/// * `ruleidx` - Index of a rule for this relation.
///
/// * `opidx` - Index of an operator in the body of the rule.
///
struct OpId {
    relid: RelId,
    ruleidx: usize,
    opidx: usize
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
enum DebugEvent {
    RelationUpdate {
        /// Relation being modified.
        relid: RelId,
        /// Value being added to or removed from the relation.
        val: Record
        /// The number of derivations of `val` being added (`diff>0`) or
        /// removed (`diff<0`).
        diff: isize
    },
    Activation {
        /// Operator being triggered.
        opid:   OpId,
        /// Arguments to the operator.
        input:  Vec<Record>
    }
}
