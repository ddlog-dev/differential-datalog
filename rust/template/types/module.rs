#![allow(
    path_statements,
    //unused_imports,
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals,
    unused_parens,
    non_shorthand_field_patterns,
    dead_code,
    overflowing_literals,
    unreachable_patterns,
    unused_variables,
    clippy::unknown_clippy_lints,
    clippy::missing_safety_doc,
    clippy::match_single_binding
)]

// Required for #[derive(Serialize, Deserialize)].
use ::serde::Deserialize;
use ::serde::Serialize;
use ::differential_datalog::record::FromRecord;
use ::differential_datalog::record::IntoRecord;
use ::differential_datalog::record::Mutator;

// Import statics from the main library.
use crate::*;

/*use ::differential_datalog::ddval::*;
use ::differential_datalog::decl_enum_into_record;
use ::differential_datalog::decl_record_mutator_enum;
use ::differential_datalog::decl_record_mutator_struct;
use ::differential_datalog::decl_struct_into_record;
use ::differential_datalog::int::*;
use ::differential_datalog::program::*;
use ::differential_datalog::record;
use ::differential_datalog::record::FromRecord;
use ::differential_datalog::record::IntoRecord;
use ::differential_datalog::uint::*;*/
