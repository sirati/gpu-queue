//! The forms of an admission record. Each record type is declared once, generic over its form: as the service writes
//! it, every figure included, or as the replay's rules read it, its raw inputs and decisions alone.
use super::super::precedence::{Capacity, Lacks, Standing, Stood};
use super::facts::Observation;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Debug;

/// A value a record holds.
pub trait Held: Clone + Debug + PartialEq + Serialize + DeserializeOwned {}
impl<T: Clone + Debug + PartialEq + Serialize + DeserializeOwned> Held for T {}

/// What a record holds of each kind of field.
pub trait Form: Clone + Debug + PartialEq {
    /// A figure: what follows from the record's facts and the facts the tick before left.
    type Figure<T: Held>: Held;
    /// One observation of a fact of one request, its declared bytes or its party, which a record repeats wherever it
    /// names the request.
    type Observed<T: Held>: Held;
    /// What a pending candidate lacks besides memory.
    type Lacks: Clone + Debug + PartialEq + Serialize;
    /// How a kept head stands.
    type Standing: Clone + Debug + PartialEq + Serialize;
}

/// The record as the service writes it: every figure, and the candidates' and the kept head's evidence as their kinds
/// require it.
#[derive(Clone, Debug, PartialEq)]
pub enum Written {}
impl Form for Written {
    type Figure<T: Held> = T;
    type Observed<T: Held> = T;
    type Lacks = Lacks;
    type Standing = Standing;
}

/// The record as the replay's rules read it: no figure, each observation for the tick's request index alone to read,
/// and the evidence as the record holds it, for the index to read as each request's kind requires.
#[derive(Clone, Debug, PartialEq)]
pub enum Projected {}
impl Form for Projected {
    type Figure<T: Held> = Unread;
    type Observed<T: Held> = Observation<T>;
    type Lacks = Option<Capacity<Projected>>;
    type Standing = Stood<Projected>;
}

/// The record as its types read it, for the mutation table's probes: every figure and observation its own type, and
/// the evidence as the record holds it. `Written` only serializes its evidence, and `Projected` reads a figure as any
/// value.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub enum Typed {}
#[cfg(test)]
impl Form for Typed {
    type Figure<T: Held> = T;
    type Observed<T: Held> = T;
    type Lacks = Option<Capacity<Typed>>;
    type Standing = Stood<Typed>;
}

/// A figure the projection does not hold: it reads any value and keeps nothing of it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Unread;
impl Serialize for Unread {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit()
    }
}
impl<'de> Deserialize<'de> for Unread {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(deserializer).map(|_| Self)
    }
}

/// An enum of the record schema: a value of every variant, kept complete by an exhaustive match.
#[cfg(test)]
pub trait Variants: Serialize + Sized {
    fn variants() -> Vec<Self>;
}

/// The record schema's enums, each by its name with a value of every variant as serde writes it: the mutation table's
/// variant obligations. A new enum field shows up as a new leaf path, and its row registers its enum here.
#[cfg(test)]
pub fn enums() -> Vec<(&'static str, Vec<String>)> {
    use super::super::precedence::{Catalogue, Class, RequestId, Step, Verdict, Work};
    use super::{Counted, Decision, Decisions, Entry, Gpu, Launching, Persisting, Subject};
    fn written<E: Variants>() -> (&'static str, Vec<String>) {
        let path = std::any::type_name::<E>().split('<').next().unwrap();
        let variants = E::variants().iter().map(|variant| serde_json::to_string(variant).unwrap()).collect();
        (path.rsplit("::").next().unwrap(), variants)
    }
    vec![
        written::<Counted>(),
        written::<Work>(),
        written::<Entry>(),
        written::<Persisting>(),
        written::<Launching>(),
        written::<Class>(),
        written::<RequestId>(),
        written::<Catalogue>(),
        written::<Verdict>(),
        written::<Step>(),
        written::<Decision>(),
        written::<Subject>(),
        written::<Decisions>(),
        written::<Gpu>(),
    ]
}
