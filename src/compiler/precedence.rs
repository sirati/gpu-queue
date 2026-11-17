//! Precedence between pending RAM requests: the class a request belongs to, which decides the pass that serves
//! it. Large requests, above `LARGE_REQUEST`, are served only from what every smaller request of the tick left.
use super::contract::LARGE_REQUEST;

/// The pass that serves a RAM request: claims and compile jobs alike.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    Ordinary,
    /// More than `LARGE_REQUEST`: served last, never reserved for.
    Large,
}

impl Class {
    /// The one place a request's class is decided. For a compile job the bytes are its next attempt's cell.
    pub fn of(bytes: u64) -> Self {
        match bytes > LARGE_REQUEST {
            true => Self::Large,
            false => Self::Ordinary,
        }
    }

    /// Why a request of this class waits beyond its memory.
    pub fn waiting(self) -> Option<&'static str> {
        match self {
            Self::Ordinary => None,
            Self::Large => Some(
                "declares more than 10 GB: served only from memory no smaller request needs; never reserves memory",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_request_is_large_only_above_the_large_request_bound() {
        assert_eq!(Class::of(LARGE_REQUEST), Class::Ordinary);
        assert_eq!(Class::of(LARGE_REQUEST + 1), Class::Large);
        assert_eq!(Class::of(0), Class::Ordinary);
        assert!(Class::Ordinary < Class::Large, "ordinary requests are served first");
    }
}
