#![allow(
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals,
    dead_code,
    clippy::block_in_if_condition_stmt,
    clippy::clone_on_copy,
    clippy::eq_op,
    clippy::cmp_owned,
    clippy::nonminimal_bool,
    clippy::toplevel_ref_arg,
    clippy::trivially_copy_pass_by_ref
)]

use std::collections::btree_map::{BTreeMap, Entry};
use std::collections::btree_set::BTreeSet;
use std::iter::FromIterator;
use std::sync::{Arc, Mutex};

use abomonation::Abomonation;
use fnv::FnvHashSet;
use serde::Deserialize;
use serde::Serialize;
use timely::communication::Allocator;
use timely::dataflow::scopes::*;
use timely::worker::Worker;

use differential_dataflow::operators::Join;
use differential_dataflow::Collection;

use crate::ddval::*;
use crate::program::*;
use crate::test_value::*;

const TEST_SIZE: u64 = 1000;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
struct P {
    f1: Q,
    f2: bool,
}

impl Abomonation for P {}

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
struct Q {
    f1: bool,
    f2: String,
}

impl Abomonation for Q {}

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
enum S {
    S1 {
        f1: u32,
        f2: String,
        f3: Q,
        f4: Uint,
    },
    S2 {
        e1: bool,
    },
    S3 {
        g1: Q,
        g2: Q,
    },
}

impl Abomonation for S {}

impl S {
    fn f1(&mut self) -> &mut u32 {
        match self {
            S::S1 { ref mut f1, .. } => f1,
            _ => panic!(""),
        }
    }
}

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
enum Value {
    empty(),
    bool(bool),
    Uint(Uint),
    String(String),
    u8(u8),
    u16(u16),
    u32(u32),
    u64(u64),
    i64(i64),
    BoolTuple((bool, bool)),
    Tuple2(Box<Value>, Box<Value>),
    Tuple3(Box<Value>, Box<Value>, Box<Value>),
    Q(Q),
    S(S),
}

impl Abomonation for Value {}

impl Default for Value {
    fn default() -> Value {
        Value::bool(false)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("test::Value")
    }
}

fn _filter_fun1(v: &Value) -> bool {
    match &v {
        Value::S(S::S1 { .. }) => true,
        _ => false,
    }
}

fn filter_map_ident(v: Value) -> Option<Value> {
    Some(v)
}

fn some_fun(x: &u32) -> u32 {
    (*x) + 1
}

fn _arrange_fun1(v: Value) -> Option<(Value, Value)> {
    let (x, _2) = match &v {
        Value::S(S::S1 {
            f1: ref x,
            f2: _0,
            f3: _2,
            f4: _3,
        }) if *_0 == "foo".to_string() && *_3 == (Uint::from_u64(32) & Uint::from_u64(0xff)) => {
            (x, _2)
        }
        _ => return None,
    };
    if (*_2).f1 && (*x) + 1 > 5 {
        return None;
    };
    if some_fun(x) > 5 {
        return None;
    };
    if some_fun(&((*x) + 1)) > 5 {
        return None;
    };
    if {
        let y = 5;
        some_fun(&y) > 5
    } {
        return None;
    };
    if {
        let x = 0;
        x < 4
    } {
        return None;
    };
    if (*_2
        == Q {
            f1: _2.f1,
            f2: _2.f2.clone(),
        })
    {
        return None;
    };
    if {
        let v = &((*x) + 1);
        (*v) > 0
    } {
        return None;
    };
    if {
        let v = &(*_2).f1;
        *v
    } {
        return None;
    };
    if {
        let V = _2.f1;
        V
    } {
        return None;
    };
    if {
        let s = &mut S::S1 {
            f1: 0,
            f2: "foo".to_string(),
            f3: Q {
                f1: true,
                f2: "bar".to_string(),
            },
            f4: Uint::from_u64(10),
        };
        *s.f1() = 5;
        let q = s.f1();
        (*q) > 0
        //q == Q{f1: false, f2: "buzz".to_string()}
    } {
        return None;
    };
    if {
        let ref mut p = P {
            f1: Q {
                f1: true,
                f2: "x".to_string(),
            },
            f2: true,
        };
        let ref mut b = false;
        p.f1 = Q {
            f1: true,
            f2: "x".to_string(),
        };
        let ref mut _pf1 = p.f1.clone();
        let ref mut _pf11 = p.f1.clone();
        let ref mut _pf2 = p.f1.f1 || p.f1.f1;
        let ref mut _pf22 = p.f2.clone();
        let ref mut z = true;
        let ref mut _pclone = p.clone();
        let Q {
            f1: ref mut qf1,
            f2: ref mut qf2,
        } = p.f1.clone();
        *qf1 = true;
        let ref mut neq = Q {
            f1: *qf1,
            f2: qf2.clone(),
        };
        *z = true;

        //*f2 = false.clone();
        //*f1 = Q{f1: true, f2: "x".to_string()};
        let (ref mut _v1, ref mut _v2): (Q, bool) = (
            Q {
                f1: true,
                f2: "x".to_string(),
            },
            false,
        );
        (*b) = false;

        let ref mut s = S::S1 {
            f1: 0,
            f2: "f2".to_string(),
            f3: neq.clone(),
            f4: Uint::from_u64(10),
        };
        match s {
            S::S1 { f1, .. } => {
                *f1 = 2;
            }
            _ => return None,
        };
        /*
        match p {
            P{f1, f2: true} => *f1 = p.f1.clone(),
            _               => return None
        };*/
        /*match &mut p.f1 {
            Q{f1: true, f2} => *f2 = p.f1.f2.clone(),
            _               => return None
        };*/
        let ref mut a: u64 = 5 as u64;
        let ref mut b: u64 = 5;
        let ref mut _c: u64 = *(a) + *(b);
        *a = *a + *a + *a;
        let ref mut str1: String = ("str1".to_string()) as String;
        let ref mut str2 = "str2".to_string();
        let ref mut _str3 = (*str1).push_str(str2.as_str());
        let (ref mut _str4, _) = ("str4".to_string(), "str5".to_string());
        match &(a, b) {
            (5, _) => (),
            _ => return None,
        }
        *z
    } {
        return None;
    };
    match *_2 {
        Q { f1: true, .. } => {}
        _ => return None,
    }
    Some((
        Value::S(S::S3 {
            g1: _2.clone(),
            g2: _2.clone(),
        }),
        v.clone(),
    ))
}

/*
fn arrange_fun1(v: Value) -> Option<(Value, Value)> {
    match v {
        Value::Tuple2(v1,v2) => Some((*v1, *v2)),
        _ => None
    }
}*/

type Delta = BTreeMap<Value, Weight>;

fn set_update<T>(_rel: &str, s: &Arc<Mutex<Delta<T>>>, x: &DDValue, w: Weight)
where
    T: Ord + DDValConvert + Clone,
{
    let mut delta = s.lock().unwrap();

    let entry = delta.entry(unsafe { T::from_ddvalue_ref(x).clone() });
    match entry {
        Entry::Vacant(vacant) => {
            vacant.insert(w);
        }
        Entry::Occupied(mut occupied) => {
            if *occupied.get() == -w {
                occupied.remove();
            } else {
                *occupied.get_mut() += w;
            }
        }
    };

    //println!("set_update({}) {:?} {}", rel, *x, insert);
}

/// Check that a program can be stopped multiple times without failure.
#[test]
fn test_multiple_stops() {
    let prog: Program = Program {
        nodes: vec![],
        init_data: vec![],
    };

    let mut running = prog.run(2).unwrap();
    running.stop().unwrap();
    running.stop().unwrap();
}

/// Test that we can attempt to insert a value into a non-existent
/// relation and fail gracefully.
#[test]
fn test_insert_non_existent_relation() {
    let prog: Program = Program {
        nodes: vec![],
        init_data: vec![],
    };

    let mut running = prog.run(3).unwrap();
    running.transaction_start().unwrap();
    let result = running.insert(1, U64(42).into_ddvalue());
    assert_eq!(
        &result.unwrap_err(),
        "apply_updates: unknown input relation 1"
    );
    running.transaction_commit().unwrap();
}

#[test]
#[should_panic(expected = "input relation (ancestor) in SCC")]
fn test_input_relation_nested() {
    let parent = {
        Relation {
            name: "parent".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: vec![],
            change_cb: None,
        }
    };
    let ancestor = Relation {
        name: "ancestor".to_string(),
        input: true,
        distinct: true,
        key_func: None,
        id: 2,
        rules: vec![],
        arrangements: vec![],
        change_cb: None,
    };
    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: parent },
            ProgNode::SCC {
                rels: vec![RecursiveRelation {
                    rel: ancestor,
                    distinct: true,
                }],
            },
        ],
        init_data: vec![],
    };

    // This call is expected to panic.
    let _err = prog.run(3).unwrap_err();
}

/* Test insertion/deletion into a database with a single table and no rules
 */
fn test_one_relation(nthreads: usize) {
    let relset: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel = {
        let relset1 = relset.clone();
        Relation {
            name: "T1".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T1", &relset1, v, w)
            })))),
        }
    };

    let prog: Program = Program {
        nodes: vec![ProgNode::Rel { rel }],
        init_data: vec![],
    };

    let mut running = prog.run(nthreads).unwrap();

    /* 1. Insertion */
    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set = BTreeMap::from_iter(vals.iter().map(|x| (U64(*x), 1)));

    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset.lock().unwrap(), set);

    /* 2. Deletion */
    let mut set2 = set.clone();
    running.transaction_start().unwrap();
    for (x, _) in &set {
        set2.remove(x);
        running.delete_value(1, x.clone().into_ddvalue()).unwrap();
        //assert_eq!(running.relation_clone_content(1).unwrap(), set2);
    }
    running.transaction_commit().unwrap();
    assert_eq!(relset.lock().unwrap().len(), 0);

    /* 3. Test set semantics: insert twice, delete once */
    running.transaction_start().unwrap();
    running.insert(1, U64(1).into_ddvalue()).unwrap();
    running.insert(1, U64(1).into_ddvalue()).unwrap();
    running.delete_value(1, U64(1).into_ddvalue()).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(relset.lock().unwrap().len(), 0);

    /* 4. Set semantics: delete before insert */
    running.transaction_start().unwrap();
    running.delete_value(1, U64(1).into_ddvalue()).unwrap();
    running.insert(1, U64(1).into_ddvalue()).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(relset.lock().unwrap().len(), 1);

    /* 5. Rollback */
    let before = relset.lock().unwrap().clone();
    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
    }
    //println!("delta: {:?}", *running.relation_delta(1).unwrap().lock().unwrap());
    //    assert_eq!(*relset.lock().unwrap(), set);
    //     assert_eq!(running.relation_clone_delta(1).unwrap().len(), set.len() - 1);
    running.transaction_rollback().unwrap();

    assert_eq!(*relset.lock().unwrap(), before);

    running.stop().unwrap();
}

#[test]
fn test_one_relation_1() {
    test_one_relation(1)
}

#[test]
fn test_one_relation_multi() {
    test_one_relation(16)
}

/* Two tables + 1 rule that keeps the two synchronized
 */
fn test_two_relations(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name: "T1".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T1", &relset1, v, w)
            })))),
        }
    };
    let relset2: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name: "T2".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 2,
            rules: vec![Rule::CollectionRule {
                description: "T2.R1".to_string(),
                rel: 1,
                xform: None,
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T2", &relset2, v, w)
            })))),
        }
    };

    let prog: Program = Program {
        nodes: vec![ProgNode::Rel { rel: rel1 }, ProgNode::Rel { rel: rel2 }],
        init_data: vec![],
    };

    let mut running = prog.run(nthreads).unwrap();

    /* 1. Populate T1 */
    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set = BTreeMap::from_iter(vals.iter().map(|x| (U64(*x), 1)));

    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        //assert_eq!(running.relation_clone_content(1).unwrap(),
        //           running.relation_clone_content(2).unwrap());
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset1.lock().unwrap(), set);
    assert_eq!(*relset1.lock().unwrap(), *relset2.lock().unwrap());

    /* 2. Clear T1 */
    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.delete_value(1, x.clone().into_ddvalue()).unwrap();
        //        assert_eq!(running.relation_clone_content(1).unwrap(),
        //                   running.relation_clone_content(2).unwrap());
    }
    running.transaction_commit().unwrap();

    assert_eq!(relset2.lock().unwrap().len(), 0);
    assert_eq!(*relset1.lock().unwrap(), *relset2.lock().unwrap());

    /* 3. Rollback */
    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        //assert_eq!(running.relation_clone_content(1).unwrap(),
        //           running.relation_clone_content(2).unwrap());
    }
    running.transaction_rollback().unwrap();

    assert_eq!(relset1.lock().unwrap().len(), 0);
    assert_eq!(*relset1.lock().unwrap(), *relset2.lock().unwrap());

    running.stop().unwrap();
}

#[test]
fn test_two_relations_1() {
    test_two_relations(1)
}

#[test]
fn test_two_relations_multi() {
    test_two_relations(16)
}

/* Semijoin
 */
fn test_semijoin(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name: "T1".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T1", &relset1, v, w)
            })))),
        }
    };
    fn fmfun1(v: DDValue) -> Option<DDValue> {
        let Tuple2(ref v1, ref _v2) = unsafe { Tuple2::<U64>::from_ddvalue(v) };
        Some(v1.clone().into_ddvalue())
    }
    fn afun1(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref v1, ref v2) = unsafe { Tuple2::<U64>::from_ddvalue(v) };
        Some((v1.clone().into_ddvalue(), v2.clone().into_ddvalue()))
    }

    let relset2: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name: "T2".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 2,
            rules: Vec::new(),
            arrangements: vec![Arrangement::Set {
                name: "arrange2.0".to_string(),
                fmfun: &(fmfun1 as FilterMapFunc),
                distinct: false,
            }],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T2", &relset2, v, w)
            })))),
        }
    };
    fn jfun(_key: &DDValue, v1: &DDValue, _v2: &()) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(unsafe { U64::from_ddvalue(v1.clone()) }),
                Box::new(unsafe { U64::from_ddvalue(v1.clone()) }),
            )
            .into_ddvalue(),
        )
    }

    let relset3: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name: "T3".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 3,
            rules: vec![Rule::CollectionRule {
                description: "T3.R1".to_string(),
                rel: 1,
                xform: Some(XFormCollection::Arrange {
                    description: "arrange by .0".to_string(),
                    afun: &(afun1 as ArrangeFunc),
                    next: Box::new(XFormArrangement::Semijoin {
                        description: "1.semijoin (2,0)".to_string(),
                        ffun: None,
                        arrangement: (2, 0),
                        jfun: &(jfun as SemijoinFunc),
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T3", &relset3, v, w)
            })))),
        }
    };

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel3 },
        ],
        init_data: vec![],
    };

    let mut running = prog.run(nthreads).unwrap();

    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set = BTreeMap::from_iter(
        vals.iter()
            .map(|x| (Tuple2(Box::new(U64(*x)), Box::new(U64(*x))), 1)),
    );

    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        running.insert(2, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset3.lock().unwrap(), set);

    running.stop().unwrap();
}

#[test]
fn test_semijoin_1() {
    test_semijoin(1)
}

#[test]
fn test_semijoin_multi() {
    test_semijoin(16)
}

/* Inner join
 */
fn test_join(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name: "T1".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T1", &relset1, v, w)
            })))),
        }
    };
    fn afun1(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref v1, ref v2) = unsafe { Tuple2::<U64>::from_ddvalue(v) };
        Some((v1.clone().into_ddvalue(), v2.clone().into_ddvalue()))
    }
    let relset2: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name: "T2".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 2,
            rules: Vec::new(),
            arrangements: vec![Arrangement::Map {
                name: "arrange2.0".to_string(),
                afun: &(afun1 as ArrangeFunc),
                queryable: true,
            }],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T2", &relset2, v, w)
            })))),
        }
    };
    fn jfun(_key: &DDValue, v1: &DDValue, v2: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new((*unsafe { U64::from_ddvalue_ref(v1) }).clone()),
                Box::new((*unsafe { U64::from_ddvalue_ref(v2) }).clone()),
            )
            .into_ddvalue(),
        )
    }

    let relset3: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name: "T3".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 3,
            rules: vec![Rule::CollectionRule {
                description: "T3.R1".to_string(),
                rel: 1,
                xform: Some(XFormCollection::Arrange {
                    description: "arrange by .0".to_string(),
                    afun: &(afun1 as ArrangeFunc),
                    next: Box::new(XFormArrangement::Join {
                        description: "1.semijoin (2,0)".to_string(),
                        ffun: None,
                        arrangement: (2, 0),
                        jfun: &(jfun as JoinFunc),
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T3", &relset3, v, w)
            })))),
        }
    };

    let relset4: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel4 = {
        let relset4 = relset4.clone();
        Relation {
            name: "T4".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 4,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T4", &relset4, v, w)
            })))),
        }
    };

    fn join_transformer() -> Box<
        dyn for<'a> Fn(
            &mut BTreeMap<RelId, Collection<Child<'a, Worker<Allocator>, TS>, Value, Weight>>,
        ),
    > {
        Box::new(|collections| {
            let rel4 = {
                let rel1 = collections.get(&1).unwrap();
                let rel1_kv = rel1.flat_map(afun1);
                let rel2 = collections.get(&2).unwrap();
                let rel2_kv = rel2.flat_map(afun1);
                rel1_kv.join(&rel2_kv).map(|(_, (v1, v2))| {
                    Tuple2(
                        Box::new(unsafe { U64::from_ddvalue(v1) }),
                        Box::new(unsafe { U64::from_ddvalue(v2) }),
                    )
                    .into_ddvalue()
                })
            };
            collections.insert(4, rel4);
        })
    }

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel3 },
            ProgNode::Apply {
                tfun: join_transformer,
            },
            ProgNode::Rel { rel: rel4 },
        ],
        init_data: vec![],
    };

    let mut running = prog.run(nthreads).unwrap();

    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set = BTreeMap::from_iter(
        vals.iter()
            .map(|x| (Tuple2(Box::new(U64(*x)), Box::new(U64(*x))), 1)),
    );

    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        running.insert(2, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset3.lock().unwrap(), set);
    assert_eq!(*relset4.lock().unwrap(), set);

    let rel2dump = running.dump_arrangement((2, 0)).unwrap();
    assert_eq!(
        rel2dump,
        BTreeSet::from_iter(vals.iter().map(|x| U64(*x).into_ddvalue()))
    );

    for key in vals.iter() {
        let vals = running
            .query_arrangement((2, 0), U64(*key).into_ddvalue())
            .unwrap();
        let mut expect = BTreeSet::new();
        expect.insert(U64(*key).into_ddvalue());
        assert_eq!(vals, expect);
    }

    running.stop().unwrap();
}

#[test]
fn test_join_1() {
    test_join(1)
}

#[test]
fn test_join_multi() {
    test_join(16)
}

/* Antijoin
 */
fn test_antijoin(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name: "T1".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T1", &relset1, v, w)
            })))),
        }
    };
    fn afun1(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref v1, _) = unsafe { Tuple2::<U64>::from_ddvalue_ref(&v) };
        Some(((*v1).clone().into_ddvalue(), v.clone()))
    }

    let relset2: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name: "T2".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 2,
            rules: Vec::new(),
            arrangements: vec![],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T2", &relset2, v, w)
            })))),
        }
    };

    fn mfun(v: DDValue) -> DDValue {
        let Tuple2(ref v1, _) = unsafe { Tuple2::<U64>::from_ddvalue(v) };
        v1.clone().into_ddvalue()
    }

    fn fmnull_fun(v: DDValue) -> Option<DDValue> {
        Some(v)
    }

    let relset21: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel21 = {
        let relset21 = relset21.clone();
        Relation {
            name: "T21".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 21,
            rules: vec![Rule::CollectionRule {
                description: "T21.R1".to_string(),
                rel: 2,
                xform: Some(XFormCollection::Map {
                    description: "map by .0".to_string(),
                    mfun: &(mfun as MapFunc),
                    next: Box::new(None),
                }),
            }],
            arrangements: vec![Arrangement::Set {
                name: "arrange2.1".to_string(),
                fmfun: &(fmnull_fun as FilterMapFunc),
                distinct: true,
            }],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T21", &relset21, v, w)
            })))),
        }
    };

    let relset3: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name: "T3".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 3,
            rules: vec![Rule::CollectionRule {
                description: "T3.R1".to_string(),
                rel: 1,
                xform: Some(XFormCollection::Arrange {
                    description: "arrange by .0".to_string(),
                    afun: &(afun1 as ArrangeFunc),
                    next: Box::new(XFormArrangement::Antijoin {
                        description: "1.antijoin (21,0)".to_string(),
                        ffun: None,
                        arrangement: (21, 0),
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T3", &relset3, v, w)
            })))),
        }
    };

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel21 },
            ProgNode::Rel { rel: rel3 },
        ],
        init_data: vec![],
    };

    let mut running = prog.run(nthreads).unwrap();

    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set = BTreeMap::from_iter(
        vals.iter()
            .map(|x| (Tuple2(Box::new(U64(*x)), Box::new(U64(*x))), 1)),
    );

    /* 1. T2 and T1 contain identical keys; antijoin is empty */
    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        running.insert(2, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(relset3.lock().unwrap().len(), 0);

    /* 1. T2 is empty; antijoin is identical to T1 */
    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.delete_value(2, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();
    assert_eq!(*relset3.lock().unwrap(), set);

    running.stop().unwrap();
}

#[test]
fn test_antijoin_1() {
    test_antijoin(1)
}

#[test]
fn test_antijoin_multi() {
    test_antijoin(16)
}

/* Delta queries 1
 */
fn test_delta(nthreads: usize) {
    let relset1: Arc<Mutex<Delta>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name: "T1".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: vec![
                Arrangement::Map {
                    name: "(_0,_)".to_string(),
                    afun: &(afun1 as ArrangeFunc<Value>),
                    queryable: false
                },
                Arrangement::Map {
                    name: "(_,_0)".to_string(),
                    afun: &(afun2 as ArrangeFunc<Value>),
                    queryable: false
                },
                Arrangement::Map {
                    name: "(_0,_1)".to_string(),
                    afun: &(afun3 as ArrangeFunc<Value>),
                    queryable: false
                },
                Arrangement::Map {
                    name: "(_1,_0)".to_string(),
                    afun: &(afun4 as ArrangeFunc<Value>),
                    queryable: false
                },
            ],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, pol| {
                set_update("T1", &relset1, v, pol)
            })))),
        }
    };
    fn afun1(v: Value) -> Option<(Value, Value)> {
        let (v1, v) = match &v {
            Value::Tuple2(v1, _) => ((**v1).clone(), v.clone()),
            _ => return None,
        };
        Some((v1, v))
    }
    fn afun2(v: Value) -> Option<(Value, Value)> {
        let (v2, v) = match &v {
            Value::Tuple2(_, v2) => ((**v2).clone(), v.clone()),
            _ => return None,
        };
        Some((v2, v))
    }
    fn afun3(v: Value) -> Option<(Value, Value)> {
        Some((v.clone(), v))
    }
    fn afun4(v: Value) -> Option<(Value, Value)> {
        let (k, val) = match &v {
            Value::Tuple2(v1, v2) => (Value::Tuple2(v2.clone(), v1.clone()), v.clone()),
            _ => return None,
        };
        Some((k, val))
    }

    fn sel_both(v: &Value) -> Value {
        match &v {
            Value::Tuple2(x, y) => Value::Tuple2(x.clone(), y.clone()),
            _ => panic!("delta.sel_both: not a Tuple 2"),
        }
    }
    fn sel_reverse(v: &Value) -> Value {
        match &v {
            Value::Tuple2(x, y) => Value::Tuple2(y.clone(), x.clone()),
            _ => panic!("delta.sel_reverese: not a Tuple 2"),
        }
    }

    fn propose2(pair: (Value, Value)) -> Option<Value> {
        Some(pair.0)
    }

    fn propose5(pair: (Value, Value)) -> Option<Value> {
        Some(pair.1)
    }

    let output_set: Arc<Mutex<Delta>> = Arc::new(Mutex::new(BTreeMap::default()));
    let output = {
        let output_set = output_set.clone();
        Relation {
            name: "Output".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 2,
            rules: vec![Rule::DeltaRule {
                description: "Output(x,y) :- T1(x,y), T1(y,x)".to_string(),
                deltas: vec![
                    /* dOutput/dT1(x,y) = dT1(x,y), T1'(y,x). */
                    DeltaRule {
                        rel: 1,
                        fmfun: &(filter_map_ident as FilterMapFunc<Value>),
                        ops: vec![DeltaOp::Join {
                            keyfunc: &(sel_both as RefMapFunc<Value>),
                            arrangement: (1, 3),
                            timestamp: OldNew::New,
                            pfunc: &(propose2 as ProposeFunc<Value>),
                        }],
                    },
                    /* dOutput/dT1(y,x) = dT1(y,x), T1(x,y) */
                    DeltaRule {
                        rel: 1,
                        fmfun: &(filter_map_ident as FilterMapFunc<Value>),
                        ops: vec![DeltaOp::Join {
                            keyfunc: &(sel_reverse as RefMapFunc<Value>),
                            arrangement: (1, 2),
                            timestamp: OldNew::Old,
                            pfunc: &(propose5 as ProposeFunc<Value>),
                        }],
                    },
                ],
            }],
            arrangements: vec![],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, pol| {
                //eprintln!("v: {:?}, pol: {}", *v, pol);
                set_update("Output", &output_set, v, pol)
            })))),
        }
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::Rel { rel: rel1 }, ProgNode::Rel { rel: output }],
        init_data: vec![
            (
                1,
                Value::Tuple2(Box::new(Value::u64(1)), Box::new(Value::u64(2))),
            ),
            (
                1,
                Value::Tuple2(Box::new(Value::u64(2)), Box::new(Value::u64(1))),
            ),
        ],
    };

    let mut running = prog.run(nthreads).unwrap();

    //let edges:Vec<(u64,u64)> = vec![(1,2), (2,1)];
    //let edge_set = FnvHashSet::from_iter(edges.iter().map(|(x,y)| Value::Tuple2(Box::new(Value::u64(*x)),Box::new(Value::u64(*y)))));

    let expected_output: Vec<(u64, u64)> = vec![(1, 2), (2, 1)];
    let expected_output_set = BTreeMap::from_iter(expected_output.iter().map(|(x, y)| {
        (
            Value::Tuple2(Box::new(Value::u64(*x)), Box::new(Value::u64(*y))),
            1,
        )
    }));
    /* 1. Initial */
    //    running.transaction_start().unwrap();
    //    let mut updates: Vec<Update<Value>> = Vec::new();
    //    for v in &edge_set {
    //        updates.push(Update::Insert{relid: 1, v: v.clone()});
    //        //running.insert(1, v.clone()).unwrap();
    //    };
    //    running.apply_updates(updates).unwrap();
    //    running.transaction_commit().unwrap();
    //
    assert_eq!(*output_set.lock().unwrap(), expected_output_set);

    running.stop().unwrap();
}

#[test]
fn test_delta_1() {
    test_delta(1)
}

#[test]
fn test_delta_multi() {
    test_delta(128)
}

/* Delta queries 2
 */
fn test_triangles(nthreads: usize) {
    let relset1: Arc<Mutex<Delta>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name: "T1".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: vec![
                Arrangement::Map {
                    name: "(_0,_)".to_string(),
                    afun: &(afun1 as ArrangeFunc<Value>),
                    queryable: false
                },
                Arrangement::Map {
                    name: "(_,_0)".to_string(),
                    afun: &(afun2 as ArrangeFunc<Value>),
                    queryable: false
                },
                Arrangement::Map {
                    name: "(_0,_1)".to_string(),
                    afun: &(afun3 as ArrangeFunc<Value>),
                    queryable: false
                },
            ],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, pol| {
                set_update("T1", &relset1, v, pol)
            })))),
        }
    };
    fn afun1(v: Value) -> Option<(Value, Value)> {
        let (v1, v) = match &v {
            Value::Tuple2(v1, _) => ((**v1).clone(), v.clone()),
            _ => return None,
        };
        Some((v1, v))
    }
    fn afun2(v: Value) -> Option<(Value, Value)> {
        let (v2, v) = match &v {
            Value::Tuple2(_, v2) => ((**v2).clone(), v.clone()),
            _ => return None,
        };
        Some((v2, v))
    }
    fn afun3(v: Value) -> Option<(Value, Value)> {
        Some((v.clone(), v))
    }

    fn sel1(v: &Value) -> Value {
        match &v {
            Value::Tuple2(v1, _) => v1.as_ref().clone(),
            _ => panic!("triangles.sel1: not a Tuple 2"),
        }
    }
    fn sel2(v: &Value) -> Value {
        match &v {
            Value::Tuple2(_, v2) => v2.as_ref().clone(),
            _ => panic!("triangles.sel2: not a Tuple 2"),
        }
    }
    fn sel_xz(v: &Value) -> Value {
        match &v {
            Value::Tuple3(x, _, z) => Value::Tuple2(x.clone(), z.clone()),
            _ => panic!("triangles.sel_xz: not a Tuple 3"),
        }
    }
    fn sel_yz(v: &Value) -> Value {
        match &v {
            Value::Tuple3(_, y, z) => Value::Tuple2(y.clone(), z.clone()),
            _ => panic!("triangles.sel_yz: not a Tuple 3"),
        }
    }

    fn propose1(pair: (Value, Value)) -> Option<Value> {
        match pair {
            (Value::Tuple2(x, y), Value::Tuple2(_, z)) => {
                Some(Value::Tuple3(x.clone(), y.clone(), z.clone()))
            }
            _ => None,
        }
    }

    fn propose2(pair: (Value, Value)) -> Option<Value> {
        Some(pair.0)
    }

    fn propose3(pair: (Value, Value)) -> Option<Value> {
        match pair {
            (Value::Tuple2(y, z), Value::Tuple2(x, _)) => {
                Some(Value::Tuple3(x.clone(), y.clone(), z.clone()))
            }
            _ => None,
        }
    }

    fn propose4(pair: (Value, Value)) -> Option<Value> {
        match pair {
            (Value::Tuple2(x, z), Value::Tuple2(_, y)) => {
                Some(Value::Tuple3(x.clone(), y.clone(), z.clone()))
            }
            _ => None,
        }
    }

    let triangle_set: Arc<Mutex<Delta>> = Arc::new(Mutex::new(BTreeMap::default()));
    let triangle = {
        let triangle_set = triangle_set.clone();
        Relation {
            name: "Triangle".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 2,
            rules: vec![Rule::DeltaRule {
                description: "Triangle(x,y,z) :- T1(x,y), T1(y,z), T1(x,z)".to_string(),
                deltas: vec![
                    /* dTriangle/dT1(x,y) = dT1(x,y), T1(y,z), T1(x,z) */
                    DeltaRule {
                        rel: 1,
                        fmfun: &(filter_map_ident as FilterMapFunc<Value>),
                        ops: vec![
                            DeltaOp::Join {
                                keyfunc: &(sel2 as RefMapFunc<Value>),
                                arrangement: (1, 0),
                                timestamp: OldNew::New,
                                pfunc: &(propose1 as ProposeFunc<Value>),
                            },
                            DeltaOp::Join {
                                keyfunc: &(sel_xz as RefMapFunc<Value>),
                                arrangement: (1, 2),
                                timestamp: OldNew::New,
                                pfunc: &(propose2 as ProposeFunc<Value>),
                            },
                        ],
                    },
                    /* dTriangle/dT1(y,z) = dT1(y,z), T1(x,y), T1(x,z) */
                    DeltaRule {
                        rel: 1,
                        fmfun: &(filter_map_ident as FilterMapFunc<Value>),
                        ops: vec![
                            DeltaOp::Join {
                                keyfunc: &(sel1 as RefMapFunc<Value>),
                                arrangement: (1, 1),
                                timestamp: OldNew::Old,
                                pfunc: &(propose3 as ProposeFunc<Value>),
                            },
                            DeltaOp::Join {
                                keyfunc: &(sel_xz as RefMapFunc<Value>),
                                arrangement: (1, 2),
                                timestamp: OldNew::New,
                                pfunc: &(propose2 as ProposeFunc<Value>),
                            },
                        ],
                    },
                    /* dTriangle/dT1(x,z) = dT1(x,z), T1(x,y), T1(y,z) */
                    DeltaRule {
                        rel: 1,
                        fmfun: &(filter_map_ident as FilterMapFunc<Value>),
                        ops: vec![
                            DeltaOp::Join {
                                keyfunc: &(sel1 as RefMapFunc<Value>),
                                arrangement: (1, 0),
                                timestamp: OldNew::Old,
                                pfunc: &(propose4 as ProposeFunc<Value>),
                            },
                            DeltaOp::Join {
                                keyfunc: &(sel_yz as RefMapFunc<Value>),
                                arrangement: (1, 2),
                                timestamp: OldNew::Old,
                                pfunc: &(propose2 as ProposeFunc<Value>),
                            },
                        ],
                    },
                ],
            }],
            arrangements: vec![],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, pol| {
                //eprintln!("v: {:?}, pol: {}", *v, pol);
                set_update("Triangle", &triangle_set, v, pol)
            })))),
        }
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::Rel { rel: rel1 }, ProgNode::Rel { rel: triangle }],
        init_data: vec![],
    };

    let mut running = prog.run(nthreads).unwrap();

    /* 1. Initial triangles */
    let edges: Vec<(u64, u64)> = vec![(1, 2), (2, 3), (1, 3), (3, 4), (4, 1), (3, 1)];
    let edge_set = FnvHashSet::from_iter(
        edges
            .iter()
            .map(|(x, y)| Value::Tuple2(Box::new(Value::u64(*x)), Box::new(Value::u64(*y)))),
    );

    let expected_triangles: Vec<(u64, u64, u64)> = vec![(1, 2, 3), (3, 4, 1)];
    let expected_triangle_set = BTreeMap::from_iter(expected_triangles.iter().map(|(x, y, z)| {
        (
            Value::Tuple3(
                Box::new(Value::u64(*x)),
                Box::new(Value::u64(*y)),
                Box::new(Value::u64(*z)),
            ),
            1,
        )
    }));
    running.transaction_start().unwrap();
    for v in &edge_set {
        running.insert(1, v.clone()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*triangle_set.lock().unwrap(), expected_triangle_set);

    /* 2. Delete edge */
    let delete_edges: Vec<(u64, u64)> = vec![(1, 3)];
    let delete_edge_set = FnvHashSet::from_iter(
        delete_edges
            .iter()
            .map(|(x, y)| Value::Tuple2(Box::new(Value::u64(*x)), Box::new(Value::u64(*y)))),
    );

    let expected_triangles2: Vec<(u64, u64, u64)> = vec![(3, 4, 1)];
    let expected_triangle_set2 =
        BTreeMap::from_iter(expected_triangles2.iter().map(|(x, y, z)| {
            (
                Value::Tuple3(
                    Box::new(Value::u64(*x)),
                    Box::new(Value::u64(*y)),
                    Box::new(Value::u64(*z)),
                ),
                1,
            )
        }));

    running.transaction_start().unwrap();
    for x in &delete_edge_set {
        running.delete_value(1, x.clone()).unwrap();
    }
    running.transaction_commit().unwrap();
    assert_eq!(*triangle_set.lock().unwrap(), expected_triangle_set2);

    /* 3. Insert edges */
    let insert_edges: Vec<(u64, u64)> = vec![(1, 5), (5, 2)];
    let insert_edge_set = FnvHashSet::from_iter(
        insert_edges
            .iter()
            .map(|(x, y)| Value::Tuple2(Box::new(Value::u64(*x)), Box::new(Value::u64(*y)))),
    );

    let expected_triangles3: Vec<(u64, u64, u64)> = vec![(3, 4, 1), (1, 5, 2)];
    let expected_triangle_set3 =
        BTreeMap::from_iter(expected_triangles3.iter().map(|(x, y, z)| {
            (
                Value::Tuple3(
                    Box::new(Value::u64(*x)),
                    Box::new(Value::u64(*y)),
                    Box::new(Value::u64(*z)),
                ),
                1,
            )
        }));

    running.transaction_start().unwrap();
    for x in &insert_edge_set {
        running.insert(1, x.clone()).unwrap();
    }
    running.transaction_commit().unwrap();
    assert_eq!(*triangle_set.lock().unwrap(), expected_triangle_set3);

    /* 4. Insert and delete */
    let insert_edges: Vec<(u64, u64)> = vec![(2, 6), (6, 3)];
    let insert_edge_set = FnvHashSet::from_iter(
        insert_edges
            .iter()
            .map(|(x, y)| Value::Tuple2(Box::new(Value::u64(*x)), Box::new(Value::u64(*y)))),
    );
    let delete_edges: Vec<(u64, u64)> = vec![(5, 2)];
    let delete_edge_set = FnvHashSet::from_iter(
        delete_edges
            .iter()
            .map(|(x, y)| Value::Tuple2(Box::new(Value::u64(*x)), Box::new(Value::u64(*y)))),
    );

    let expected_triangles4: Vec<(u64, u64, u64)> = vec![(3, 4, 1), (2, 6, 3)];
    let expected_triangle_set4 =
        BTreeMap::from_iter(expected_triangles4.iter().map(|(x, y, z)| {
            (
                Value::Tuple3(
                    Box::new(Value::u64(*x)),
                    Box::new(Value::u64(*y)),
                    Box::new(Value::u64(*z)),
                ),
                1,
            )
        }));

    running.transaction_start().unwrap();
    for x in &insert_edge_set {
        running.insert(1, x.clone()).unwrap();
    }
    for x in &delete_edge_set {
        running.delete_value(1, x.clone()).unwrap();
    }

    running.transaction_commit().unwrap();
    assert_eq!(*triangle_set.lock().unwrap(), expected_triangle_set4);

    running.stop().unwrap();
}

#[test]
fn test_triangles_1() {
    test_triangles(1)
}

#[test]
fn test_triangles_multi() {
    test_triangles(16)
}

/* Maps and filters
 */
fn test_map(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name: "T1".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T1", &relset1, v, w)
            })))),
        }
    };

    fn mfun(v: DDValue) -> DDValue {
        let U64(ref uv) = unsafe { U64::from_ddvalue_ref(&v) };
        U64(uv.clone() * 2).into_ddvalue()
    }

    fn gfun3(v: DDValue) -> Option<(DDValue, DDValue)> {
        Some((Empty {}.into_ddvalue(), v))
    }

    fn agfun3(_key: &DDValue, src: &[(&DDValue, Weight)]) -> DDValue {
        U64(src.len() as u64).into_ddvalue()
    }

    fn gfun4(v: DDValue) -> Option<(DDValue, DDValue)> {
        Some((v.clone(), v))
    }

    fn agfun4(key: &DDValue, _src: &[(&DDValue, Weight)]) -> DDValue {
        key.clone()
    }

    fn ffun(v: &DDValue) -> bool {
        let U64(ref uv) = unsafe { U64::from_ddvalue_ref(&v) };
        *uv > 10
    }

    fn fmfun(v: DDValue) -> Option<DDValue> {
        let U64(uv) = unsafe { U64::from_ddvalue_ref(&v) };
        if *uv > 12 {
            Some(U64(uv.clone() * 2).into_ddvalue())
        } else {
            None
        }
    }

    fn flatmapfun(v: DDValue) -> Option<Box<dyn Iterator<Item = DDValue>>> {
        let U64(i) = unsafe { U64::from_ddvalue_ref(&v) };
        if *i > 12 {
            Some(Box::new(
                vec![
                    I64(-(*i as i64)).into_ddvalue(),
                    I64(-(2 * (*i as i64))).into_ddvalue(),
                ]
                .into_iter(),
            ))
        } else {
            None
        }
    }

    let relset2: Arc<Mutex<Delta<I64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name: "T2".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 2,
            rules: vec![Rule::CollectionRule {
                description: "T2.R1".to_string(),
                rel: 1,
                xform: Some(XFormCollection::Map {
                    description: "map x2".to_string(),
                    mfun: &(mfun as MapFunc),
                    next: Box::new(Some(XFormCollection::Filter {
                        description: "filter >10".to_string(),
                        ffun: &(ffun as FilterFunc),
                        next: Box::new(Some(XFormCollection::FilterMap {
                            description: "filter >12 map x2".to_string(),
                            fmfun: &(fmfun as FilterMapFunc),
                            next: Box::new(Some(XFormCollection::FlatMap {
                                description: "flat-map >12 [-x,-2x]".to_string(),
                                fmfun: &(flatmapfun as FlatMapFunc),
                                next: Box::new(None),
                            })),
                        })),
                    })),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T2", &relset2, v, w)
            })))),
        }
    };
    let relset3: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name: "T3".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 3,
            rules: vec![Rule::CollectionRule {
                description: "T3.R1".to_string(),
                rel: 2,
                xform: Some(XFormCollection::Arrange {
                    description: "arrange by ()".to_string(),
                    afun: &(gfun3 as ArrangeFunc),
                    next: Box::new(XFormArrangement::Aggregate {
                        description: "2.aggregate".to_string(),
                        ffun: None,
                        aggfun: &(agfun3 as AggFunc),
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T3", &relset3, v, w)
            })))),
        }
    };

    let relset4: Arc<Mutex<Delta<I64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel4 = {
        let relset4 = relset4.clone();
        Relation {
            name: "T4".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 4,
            rules: vec![Rule::CollectionRule {
                description: "T4.R1".to_string(),
                rel: 2,
                xform: Some(XFormCollection::Arrange {
                    description: "arrange by self".to_string(),
                    afun: &(gfun4 as ArrangeFunc),
                    next: Box::new(XFormArrangement::Aggregate {
                        description: "2.aggregate".to_string(),
                        ffun: None,
                        aggfun: &(agfun4 as AggFunc),
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("T4", &relset4, v, w)
            })))),
        }
    };

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel3 },
            ProgNode::Rel { rel: rel4 },
        ],
        init_data: vec![],
    };

    let mut running = prog.run(nthreads).unwrap();

    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set = BTreeMap::from_iter(vals.iter().map(|x| (U64(*x), 1)));
    let expected2 = BTreeMap::from_iter(
        vals.iter()
            .map(|x| U64(*x).into_ddvalue())
            .map(|x| mfun(x))
            .filter(|x| ffun(&x))
            .filter_map(|x| fmfun(x))
            .flat_map(|x| match flatmapfun(x) {
                Some(iter) => iter,
                None => Box::new(None.into_iter()),
            })
            .map(|x| (unsafe { I64::from_ddvalue(x) }, 1)),
    );
    let mut expected3 = BTreeMap::default();
    expected3.insert(U64(expected2.len() as u64), 1);

    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset1.lock().unwrap(), set);
    assert_eq!(*relset2.lock().unwrap(), expected2);
    assert_eq!(*relset3.lock().unwrap(), expected3);
    assert_eq!(*relset4.lock().unwrap(), expected2);
    running.stop().unwrap();
}

#[test]
fn test_map_1() {
    test_map(1)
}

#[test]
fn test_map_multi() {
    test_map(16)
}

/* Recursion
*/
fn test_recursion(nthreads: usize) {
    fn arrange_by_fst(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref fst, ref snd) = unsafe { Tuple2::<String>::from_ddvalue_ref(&v) };
        Some(((*fst).clone().into_ddvalue(), (*snd).clone().into_ddvalue()))
    }
    fn arrange_by_snd(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref fst, ref snd) = unsafe { Tuple2::<String>::from_ddvalue_ref(&v) };
        Some((
            (**snd).clone().into_ddvalue(),
            (**fst).clone().into_ddvalue(),
        ))
    }
    fn anti_arrange1(v: DDValue) -> Option<(DDValue, DDValue)> {
        //println!("anti_arrange({:?})", v);
        let res = Some((v.clone(), v.clone()));
        //println!("res: {:?}", res);
        res
    }

    fn anti_arrange2(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref fst, ref snd) = unsafe { Tuple2::<String>::from_ddvalue_ref(&v) };
        Some((
            Tuple2(Box::new((**snd).clone()), Box::new((**fst).clone())).into_ddvalue(),
            v.clone(),
        ))
    }

    fn jfun(_parent: &DDValue, ancestor: &DDValue, child: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(unsafe { String::from_ddvalue_ref(ancestor) }.clone()),
                Box::new(unsafe { String::from_ddvalue_ref(child) }.clone()),
            )
            .into_ddvalue(),
        )
    }

    fn jfun2(_ancestor: &DDValue, descendant1: &DDValue, descendant2: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(unsafe { String::from_ddvalue_ref(descendant1) }.clone()),
                Box::new(unsafe { String::from_ddvalue_ref(descendant2) }.clone()),
            )
            .into_ddvalue(),
        )
    }

    fn fmnull_fun(v: DDValue) -> Option<DDValue> {
        Some(v)
    }

    let parentset: Arc<Mutex<Delta<Tuple2<String>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let parent = {
        let parentset = parentset.clone();
        Relation {
            name: "parent".to_string(),
            input: true,
            distinct: true,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: vec![Arrangement::Map {
                name: "arrange_by_parent".to_string(),
                afun: &(arrange_by_fst as ArrangeFunc),
                queryable: false,
            }],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("parent", &parentset, v, w)
            })))),
        }
    };

    let ancestorset: Arc<Mutex<Delta<Tuple2<String>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let ancestor = {
        let ancestorset = ancestorset.clone();
        Relation {
            name: "ancestor".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 2,
            rules: vec![
                Rule::CollectionRule {
                    description: "ancestor.R1".to_string(),
                    rel: 1,
                    xform: None,
                },
                Rule::ArrangementRule {
                    description: "ancestor.R2".to_string(),
                    arr: (2, 2),
                    xform: XFormArrangement::Join {
                        description: "(2,2).join (1,0)".to_string(),
                        ffun: None,
                        arrangement: (1, 0),
                        jfun: &(jfun as JoinFunc),
                        next: Box::new(None),
                    },
                },
            ],
            arrangements: vec![
                Arrangement::Map {
                    name: "arrange_by_ancestor".to_string(),
                    afun: &(arrange_by_fst as ArrangeFunc),
                    queryable: false,
                },
                Arrangement::Set {
                    name: "arrange_by_self".to_string(),
                    fmfun: &(fmnull_fun as FilterMapFunc),
                    distinct: true,
                },
                Arrangement::Map {
                    name: "arrange_by_snd".to_string(),
                    afun: &(arrange_by_snd as ArrangeFunc),
                    queryable: false,
                },
            ],
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("ancestor", &ancestorset, v, w)
            })))),
        }
    };

    fn ffun(v: &DDValue) -> bool {
        let Tuple2(ref fst, ref snd) = unsafe { Tuple2::<String>::from_ddvalue_ref(&v) };
        *fst != *snd
    }

    let common_ancestorset: Arc<Mutex<Delta<Tuple2<String>>>> =
        Arc::new(Mutex::new(BTreeMap::default()));
    let common_ancestor = {
        let common_ancestorset = common_ancestorset.clone();
        Relation {
            name: "common_ancestor".to_string(),
            input: false,
            distinct: true,
            key_func: None,
            id: 3,
            rules: vec![Rule::ArrangementRule {
                description: "common_ancestor.R1".to_string(),
                arr: (2, 0),
                xform: XFormArrangement::Join {
                    description: "(2,0).join (2,0)".to_string(),
                    ffun: None,
                    arrangement: (2, 0),
                    jfun: &(jfun2 as JoinFunc),
                    next: Box::new(Some(XFormCollection::Arrange {
                        description: "arrange by self".to_string(),
                        afun: &(anti_arrange1 as ArrangeFunc),
                        next: Box::new(XFormArrangement::Antijoin {
                            description: "(2,0).antijoin (2,1)".to_string(),
                            ffun: None,
                            arrangement: (2, 1),
                            next: Box::new(Some(XFormCollection::Arrange {
                                description: "arrange by .1".to_string(),
                                afun: &(anti_arrange2 as ArrangeFunc),
                                next: Box::new(XFormArrangement::Antijoin {
                                    description: "...join (2,1)".to_string(),
                                    ffun: None,
                                    arrangement: (2, 1),
                                    next: Box::new(Some(XFormCollection::Filter {
                                        description: "filter .0 != .1".to_string(),
                                        ffun: &(ffun as FilterFunc),
                                        next: Box::new(None),
                                    })),
                                }),
                            })),
                        }),
                    })),
                },
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(Mutex::new(Box::new(move |_, v, w| {
                set_update("common_ancestor", &common_ancestorset, v, w)
            })))),
        }
    };

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: parent },
            ProgNode::SCC {
                rels: vec![RecursiveRelation {
                    rel: ancestor,
                    distinct: true,
                }],
            },
            ProgNode::Rel {
                rel: common_ancestor,
            },
        ],
        init_data: vec![],
    };

    let mut running = prog.run(nthreads).unwrap();

    /* 1. Populate parent relation */
    /*
        A-->B-->C
        |   |-->D
        |
        |-->E
    */
    let vals = vec![
        Tuple2(
            Box::new(String("A".to_string())),
            Box::new(String("B".to_string())),
        ),
        Tuple2(
            Box::new(String("B".to_string())),
            Box::new(String("C".to_string())),
        ),
        Tuple2(
            Box::new(String("B".to_string())),
            Box::new(String("D".to_string())),
        ),
        Tuple2(
            Box::new(String("A".to_string())),
            Box::new(String("E".to_string())),
        ),
    ];
    let set = BTreeMap::from_iter(vals.iter().map(|x| (x.clone(), 1)));

    let expect_vals = vec![
        Tuple2(
            Box::new(String("A".to_string())),
            Box::new(String("B".to_string())),
        ),
        Tuple2(
            Box::new(String("B".to_string())),
            Box::new(String("C".to_string())),
        ),
        Tuple2(
            Box::new(String("B".to_string())),
            Box::new(String("D".to_string())),
        ),
        Tuple2(
            Box::new(String("A".to_string())),
            Box::new(String("E".to_string())),
        ),
        Tuple2(
            Box::new(String("A".to_string())),
            Box::new(String("D".to_string())),
        ),
        Tuple2(
            Box::new(String("A".to_string())),
            Box::new(String("C".to_string())),
        ),
    ];

    let expect_set = BTreeMap::from_iter(expect_vals.iter().map(|x| (x.clone(), 1)));

    let expect_vals2 = vec![
        Tuple2(
            Box::new(String("C".to_string())),
            Box::new(String("D".to_string())),
        ),
        Tuple2(
            Box::new(String("D".to_string())),
            Box::new(String("C".to_string())),
        ),
        Tuple2(
            Box::new(String("C".to_string())),
            Box::new(String("E".to_string())),
        ),
        Tuple2(
            Box::new(String("E".to_string())),
            Box::new(String("C".to_string())),
        ),
        Tuple2(
            Box::new(String("D".to_string())),
            Box::new(String("E".to_string())),
        ),
        Tuple2(
            Box::new(String("E".to_string())),
            Box::new(String("D".to_string())),
        ),
        Tuple2(
            Box::new(String("E".to_string())),
            Box::new(String("B".to_string())),
        ),
        Tuple2(
            Box::new(String("B".to_string())),
            Box::new(String("E".to_string())),
        ),
    ];
    let expect_set2 = BTreeMap::from_iter(expect_vals2.iter().map(|x| (x.clone(), 1)));

    running.transaction_start().unwrap();
    for (x, _) in &set {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();
    //println!("commit done");

    assert_eq!(*parentset.lock().unwrap(), set);
    assert_eq!(*ancestorset.lock().unwrap(), expect_set);
    assert_eq!(*common_ancestorset.lock().unwrap(), expect_set2);

    /* 2. Remove record from "parent" relation */
    running.transaction_start().unwrap();
    running
        .delete_value(1, vals[0].clone().into_ddvalue())
        .unwrap();
    running.transaction_commit().unwrap();

    let expect_vals3 = vec![
        Tuple2(
            Box::new(String("B".to_string())),
            Box::new(String("C".to_string())),
        ),
        Tuple2(
            Box::new(String("B".to_string())),
            Box::new(String("D".to_string())),
        ),
        Tuple2(
            Box::new(String("A".to_string())),
            Box::new(String("E".to_string())),
        ),
    ];
    let expect_set3 = BTreeMap::from_iter(expect_vals3.iter().map(|x| (x.clone(), 1)));

    assert_eq!(*ancestorset.lock().unwrap(), expect_set3);

    let expect_vals4 = vec![
        Tuple2(
            Box::new(String("C".to_string())),
            Box::new(String("D".to_string())),
        ),
        Tuple2(
            Box::new(String("D".to_string())),
            Box::new(String("C".to_string())),
        ),
    ];
    let expect_set4 = BTreeMap::from_iter(expect_vals4.iter().map(|x| (x.clone(), 1)));

    assert_eq!(*common_ancestorset.lock().unwrap(), expect_set4);

    /* 3. Test clear_relation() */
    running.transaction_start().unwrap();
    running.clear_relation(1).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(*parentset.lock().unwrap(), BTreeMap::default());
    assert_eq!(*ancestorset.lock().unwrap(), BTreeMap::default());
    assert_eq!(*common_ancestorset.lock().unwrap(), BTreeMap::default());

    running.stop().unwrap();
}

#[test]
fn test_recursion_1() {
    test_recursion(1)
}

#[test]
fn test_recursion_multi() {
    test_recursion(16)
}
