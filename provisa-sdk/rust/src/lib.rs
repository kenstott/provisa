// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

//! Provisa's optional SDK for Rust code sources (REQ-1984).
//!
//! Each registration is a doc-comment tag, spelled so the compiler holds the function to its
//! tag's rules. A registration at the crate's root
//!
//! ```ignore
//! provisa_sdk::table!(churn_risk, secret = "API_KEY");
//! ```
//!
//! is the same function to Provisa as
//!
//! ```ignore
//! /// The churn risk of one account.
//! ///
//! /// @table
//! /// @requires API_KEY:secret
//! pub fn churn_risk(inputs: RiskInputs) -> Result<Vec<Risk>, Error> { ... }
//! ```
//!
//! Provisa's reader finds a registration by the path the macro is invoked by: `provisa_sdk::`
//! (or the name `Cargo.toml` gives the dependency), or a name a root `use provisa_sdk::...`
//! brings in. It must be an item at the crate's root, naming a `pub fn` at the crate's root, and
//! every option's value is a literal. The SDK does nothing at run time: a registration expands to
//! a constant that only type-checks the function.
//!
//! | Registration | Tag |
//! |---|---|
//! | `table!(f)`, `stream!(f)` | `@table`, `@stream` |
//! | `command!(f)`, `query!(f)`, `columns!(f)` | `@command`, `@query`, `@columns` |
//! | `on_start!(f)`, `on_stop!(f)`, `health!(f)` | `@on-start`, `@on-stop`, `@health` |
//! | `change!(f, table = "t", key = ["k"], removed = "r", cursor = "c")` | `@change table=t`, `key=k`, `removed=r`, `cursor=c` |
//! | option `requires = "NAME"`, `secret = "NAME"` | `@requires NAME`, `@requires NAME:secret` |
//! | table option `loader = true`, `loader = N` | `@loader`, `@loader N` |
//! | command option `insert = "t"`, `update = "t"`, `delete = "t"` | `@insert t`, `@update t`, `@delete t` |
//!
//! The compiler checks a `table!`, `stream!`, `columns!`, `on_start!`, `on_stop!` or `health!`
//! function's signature against its tag's rules. A command, query or change function takes any
//! parameters, so its shape is checked when Provisa reads the code, as it is for the doc-comment
//! form.
//!
//! A `@table` or `@stream` function may take a read context after its inputs (REQ-2100): a
//! parameter of a type the code declares and names `ReadContext`, as it declares `TableRef`. The
//! SDK has no serde dependency, so it declares neither. `table!(f, loader = true)` is `@loader`
//! and `table!(f, loader = N)` is `@loader N`: a loader returns a map of each key's hash to its
//! rows, `HashMap<String, Vec<Row>>`.

/// The version of the registrations this SDK spells. A server refuses, by name, a registration
/// it does not read.
pub const SPELLING: u32 = 1;

// -- result shapes: aliases of what the reader already maps ----------------------------------------

/// A `@table`'s rows.
pub type Rows<T> = Vec<T>;
/// A `@stream`'s rows, each delivered as it is yielded; an item may be a `Result`, whose error
/// ends the stream.
pub type Stream<T> = Box<dyn Iterator<Item = T>>;
/// A `@columns` parameter: one input column's values for the batch.
pub type Batch<T> = Vec<T>;
/// A `@columns` result: one result row per input row.
pub type Results<T> = Vec<T>;

// -- column data types: aliases of the types Provisa already maps ----------------------------------

/// A text column.
pub type Text = String;
/// A bigint column.
pub type BigInt = i64;
/// A double column.
pub type Double = f64;
/// A boolean column.
pub type Boolean = bool;

/// The shapes each registration holds its function to: marker traits, implemented for exactly
/// the signatures Provisa reads. Their type parameters are markers that keep the implementations
/// apart; nothing here is called.
#[doc(hidden)]
pub mod check {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, LinkedList, VecDeque};
    use std::marker::PhantomData;

    pub struct Collection;
    pub struct Iteration;
    pub struct Keyed;
    pub struct Fallible<M>(PhantomData<M>);

    /// What a `@table` returns: a collection or an iterator of rows, optionally in a `Result`.
    pub trait Collected<M> {}

    macro_rules! collections {
        ($($ty:ident<$($p:ident),+>),+) => {$(
            impl<$($p),+> Collected<Collection> for $ty<$($p),+> {}
            impl<$($p),+, E> Collected<Fallible<Collection>> for Result<$ty<$($p),+>, E> {}
        )+};
    }
    collections!(Vec<R>, VecDeque<R>, LinkedList<R>, BTreeSet<R>, HashSet<R, S>);
    impl<I: Iterator> Collected<Iteration> for I {}
    impl<I: Iterator, E> Collected<Fallible<Iteration>> for Result<I, E> {}
    // REQ-2100: a @loader's map of each key's hash to its rows.
    impl<R, S> Collected<Keyed> for HashMap<String, Vec<R>, S> {}
    impl<R, S, E> Collected<Fallible<Keyed>> for Result<HashMap<String, Vec<R>, S>, E> {}
    impl<R> Collected<Keyed> for BTreeMap<String, Vec<R>> {}
    impl<R, E> Collected<Fallible<Keyed>> for Result<BTreeMap<String, Vec<R>>, E> {}

    /// What a `@stream` returns: an iterator of rows (each item a row, or a row in a `Result`),
    /// optionally in a `Result`.
    pub trait Streamed<M> {}
    impl<I: Iterator> Streamed<Iteration> for I {}
    impl<I: Iterator, E> Streamed<Fallible<Iteration>> for Result<I, E> {}

    /// A `@table` function: it takes nothing, its inputs struct or its read context, or its
    /// inputs struct and then its read context.
    pub trait ReadFn<Marker> {}
    impl<F, I, C, O, M> ReadFn<(I, C, O, M)> for F
    where
        F: Fn(I, C) -> O,
        O: Collected<M>,
    {
    }
    impl<F, O, M> ReadFn<(O, M)> for F
    where
        F: Fn() -> O,
        O: Collected<M>,
    {
    }
    impl<F, I, O, M> ReadFn<(I, O, M)> for F
    where
        F: Fn(I) -> O,
        O: Collected<M>,
    {
    }

    /// A `@stream` function: the parameters of a [`ReadFn`], returning an iterator.
    pub trait StreamFn<Marker> {}
    impl<F, I, C, O, M> StreamFn<(I, C, O, M)> for F
    where
        F: Fn(I, C) -> O,
        O: Streamed<M>,
    {
    }
    impl<F, O, M> StreamFn<(O, M)> for F
    where
        F: Fn() -> O,
        O: Streamed<M>,
    {
    }
    impl<F, I, O, M> StreamFn<(I, O, M)> for F
    where
        F: Fn(I) -> O,
        O: Streamed<M>,
    {
    }

    /// What a lifecycle function returns: nothing, or a `Result` of nothing.
    pub trait Done<M> {}
    impl Done<()> for () {}
    impl<E> Done<Fallible<()>> for Result<(), E> {}

    /// The setup values an `@on-start` function may take: one map of text names.
    pub trait Values {}
    impl<V, S> Values for HashMap<String, V, S> {}
    impl<V> Values for BTreeMap<String, V> {}

    /// An `@on-start` function: it takes nothing, or the setup values as one map.
    pub trait StartFn<Marker> {}
    impl<F, O, M> StartFn<(O, M)> for F
    where
        F: Fn() -> O,
        O: Done<M>,
    {
    }
    impl<F, V, O, M> StartFn<(V, O, M)> for F
    where
        F: Fn(V) -> O,
        V: Values,
        O: Done<M>,
    {
    }

    /// An `@on-stop` function: it takes nothing.
    pub trait StopFn<Marker> {}
    impl<F, O, M> StopFn<(O, M)> for F
    where
        F: Fn() -> O,
        O: Done<M>,
    {
    }

    /// What a `@health` function returns: its status as text, optionally in a `Result`.
    pub trait Status<M> {}
    impl Status<()> for String {}
    impl Status<()> for &'static str {}
    impl<E> Status<Fallible<()>> for Result<String, E> {}
    impl<E> Status<Fallible<()>> for Result<&'static str, E> {}

    /// A `@health` function: it takes nothing and returns its status as text.
    pub trait HealthFn<Marker> {}
    impl<F, O, M> HealthFn<(O, M)> for F
    where
        F: Fn() -> O,
        O: Status<M>,
    {
    }

    /// What a `@columns` function returns: one result row per input row.
    pub trait ColumnResults<M> {}
    impl<R> ColumnResults<()> for Vec<R> {}
    impl<R, E> ColumnResults<Fallible<()>> for Result<Vec<R>, E> {}

    /// A function of any parameters (a command, a query, a change function), and a `@columns`
    /// function: each parameter one input column's values, a `Vec`.
    pub trait AnyFn<Marker> {}
    pub trait ColumnsFn<Marker> {}

    macro_rules! arities {
        ($(($($a:ident),*)),+) => {$(
            impl<F, O, $($a),*> AnyFn<(O, $($a,)*)> for F where F: Fn($($a),*) -> O {}
            impl<F, O, M, $($a),*> ColumnsFn<(O, M, $($a,)*)> for F
            where
                F: Fn($(Vec<$a>),*) -> O,
                O: ColumnResults<M>,
            {
            }
        )+};
    }
    arities!(
        (),
        (A1),
        (A1, A2),
        (A1, A2, A3),
        (A1, A2, A3, A4),
        (A1, A2, A3, A4, A5),
        (A1, A2, A3, A4, A5, A6),
        (A1, A2, A3, A4, A5, A6, A7),
        (A1, A2, A3, A4, A5, A6, A7, A8),
        (A1, A2, A3, A4, A5, A6, A7, A8, A9),
        (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10),
        (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11),
        (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12)
    );

    pub fn table<F: ReadFn<Marker>, Marker>(_: F) {}
    pub fn stream<F: StreamFn<Marker>, Marker>(_: F) {}
    pub fn command<F: AnyFn<Marker>, Marker>(_: F) {}
    pub fn columns<F: ColumnsFn<Marker>, Marker>(_: F) {}
    pub fn on_start<F: StartFn<Marker>, Marker>(_: F) {}
    pub fn on_stop<F: StopFn<Marker>, Marker>(_: F) {}
    pub fn health<F: HealthFn<Marker>, Marker>(_: F) {}
    pub fn text(_: &str) {}

    /// A `loader` option's value (REQ-2100): `true`, or the largest batch, a whole number.
    pub trait LoaderOption {}
    impl LoaderOption for bool {}
    impl LoaderOption for i32 {}
    impl LoaderOption for i64 {}
    impl LoaderOption for u32 {}
    impl LoaderOption for u64 {}
    impl LoaderOption for usize {}
    pub fn loader<V: LoaderOption>(_: V) {}
}

/// The options of a registration, each `key = literal`: which keys each registration takes, and
/// that each value is a text literal. A key it does not take, or a value that is not a literal,
/// matches no rule, and the code does not build.
#[doc(hidden)]
#[macro_export]
macro_rules! __provisa_option {
    (table, requires = $v:literal) => {
        $crate::check::text($v)
    };
    (table, secret = $v:literal) => {
        $crate::check::text($v)
    };
    (table, loader = $v:literal) => {
        $crate::check::loader($v)
    };
    (read, requires = $v:literal) => {
        $crate::check::text($v)
    };
    (read, secret = $v:literal) => {
        $crate::check::text($v)
    };
    (command, requires = $v:literal) => {
        $crate::check::text($v)
    };
    (command, secret = $v:literal) => {
        $crate::check::text($v)
    };
    (command, insert = $v:literal) => {
        $crate::check::text($v)
    };
    (command, update = $v:literal) => {
        $crate::check::text($v)
    };
    (command, delete = $v:literal) => {
        $crate::check::text($v)
    };
}

/// One registration: the function type-checked by `$check`, each option by its rule. It expands
/// to a constant holding a closure that is never called, so nothing runs.
#[doc(hidden)]
#[macro_export]
macro_rules! __provisa_register {
    ($check:ident, $options:ident, $f:ident $(, $key:ident = $value:tt)*) => {
        const _: () = {
            let _ = || {
                $crate::check::$check($f);
                $($crate::__provisa_option!($options, $key = $value);)*
            };
        };
    };
}

/// `@table`: a table whose rows the function returns. Options: `requires`, `secret`, and
/// `loader` (`true` for `@loader`, a whole number for `@loader N`).
#[macro_export]
macro_rules! table {
    ($f:ident $(, $key:ident = $value:tt)* $(,)?) => {
        $crate::__provisa_register!(table, table, $f $(, $key = $value)*);
    };
}

/// `@stream`: a live table, each row delivered as the iterator yields it. Options: `requires`,
/// `secret`.
#[macro_export]
macro_rules! stream {
    ($f:ident $(, $key:ident = $value:tt)* $(,)?) => {
        $crate::__provisa_register!(stream, read, $f $(, $key = $value)*);
    };
}

/// `@command`: a mutation. Options: `requires`, `secret`, and `insert`, `update` or `delete`,
/// the table whose writes the command is.
#[macro_export]
macro_rules! command {
    ($f:ident $(, $key:ident = $value:tt)* $(,)?) => {
        $crate::__provisa_register!(command, command, $f $(, $key = $value)*);
    };
}

/// `@query`: a command whose kind is a query (REQ-2226). Options: `requires`, `secret`.
#[macro_export]
macro_rules! query {
    ($f:ident $(, $key:ident = $value:tt)* $(,)?) => {
        $crate::__provisa_register!(command, read, $f $(, $key = $value)*);
    };
}

/// `@columns`: a batch column transform; each parameter is one input column's values, and the
/// result holds one row per input row. Options: `requires`, `secret`.
#[macro_export]
macro_rules! columns {
    ($f:ident $(, $key:ident = $value:tt)* $(,)?) => {
        $crate::__provisa_register!(columns, read, $f $(, $key = $value)*);
    };
}

/// `@change` (REQ-2249): the function that keeps the landed table `table` fresh, given the
/// cursor of its last run: `key` the columns identifying a match, `removed` the column marking
/// one as removed, `cursor` the column carrying each change's cursor value. Then the options
/// `requires`, `secret`.
#[macro_export]
macro_rules! change {
    (
        $f:ident,
        table = $table:literal,
        key = [$($column:literal),+ $(,)?],
        removed = $removed:literal,
        cursor = $cursor:literal
        $(, $key:ident = $value:tt)* $(,)?
    ) => {
        const _: () = {
            let _ = || {
                $crate::check::text($table);
                $($crate::check::text($column);)+
                $crate::check::text($removed);
                $crate::check::text($cursor);
            };
        };
        $crate::__provisa_register!(command, read, $f $(, $key = $value)*);
    };
}

/// `@on-start`: called once when the code starts, with nothing or the setup values as one map.
/// Options: `requires`, `secret`.
#[macro_export]
macro_rules! on_start {
    ($f:ident $(, $key:ident = $value:tt)* $(,)?) => {
        $crate::__provisa_register!(on_start, read, $f $(, $key = $value)*);
    };
}

/// `@on-stop`: called once when the code stops.
#[macro_export]
macro_rules! on_stop {
    ($f:ident $(,)?) => {
        $crate::__provisa_register!(on_stop, read, $f);
    };
}

/// `@health`: the code's status, as text.
#[macro_export]
macro_rules! health {
    ($f:ident $(,)?) => {
        $crate::__provisa_register!(health, read, $f);
    };
}
