use differential_dataflow::lattice::Lattice;
use serde::{Deserialize, Serialize, Serializer};
use timely::order::TotalOrder;
use timely::progress::timestamp::Refines;
use timely::progress::{PathSummary, Timestamp};
use timely::PartialOrder;

// borrowed from [Materialize](https://github.com/MaterializeInc/materialize)
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SysTime {
    /// note no `pub`.
    internal: u64,
}

impl SysTime {
    pub const MAX: Self = Self { internal: u64::MAX };
    pub const MIN: Self = Self { internal: u64::MIN };

    pub const fn new(timestamp: u64) -> Self {
        Self {
            internal: timestamp,
        }
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        self.internal.to_le_bytes()
    }

    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self {
            internal: u64::from_le_bytes(bytes),
        }
    }

    pub fn saturating_sub<I: Into<Self>>(self, rhs: I) -> Self {
        Self {
            internal: self.internal.saturating_sub(rhs.into().internal),
        }
    }

    pub fn saturating_add<I: Into<Self>>(self, rhs: I) -> Self {
        Self {
            internal: self.internal.saturating_add(rhs.into().internal),
        }
    }

    pub fn saturating_mul<I: Into<Self>>(self, rhs: I) -> Self {
        Self {
            internal: self.internal.saturating_mul(rhs.into().internal),
        }
    }

    pub fn checked_add<I: Into<Self>>(self, rhs: I) -> Option<Self> {
        self.internal
            .checked_add(rhs.into().internal)
            .map(|internal| Self { internal })
    }

    pub fn checked_sub<I: Into<Self>>(self, rhs: I) -> Option<Self> {
        self.internal
            .checked_sub(rhs.into().internal)
            .map(|internal| Self { internal })
    }

    /// Advance a timestamp by the least amount possible such that
    /// `ts.less_than(ts.step_forward())` is true. Panic if unable to do so.
    pub fn step_forward(&self) -> Self {
        match self.checked_add(1) {
            Some(ts) => ts,
            None => panic!("could not step forward"),
        }
    }

    /// Advance a timestamp forward by the given `amount`. Panic if unable to do so.
    pub fn step_forward_by(&self, amount: &Self) -> Self {
        match self.checked_add(*amount) {
            Some(ts) => ts,
            None => panic!("could not step {self} forward by {amount}"),
        }
    }

    /// Advance a timestamp by the least amount possible such that `ts.less_than(ts.step_forward())`
    /// is true. Return `None` if unable to do so.
    pub fn try_step_forward(&self) -> Option<Self> {
        self.checked_add(1)
    }

    /// Advance a timestamp forward by the given `amount`. Return `None` if unable to do so.
    pub fn try_step_forward_by(&self, amount: &Self) -> Option<Self> {
        self.checked_add(*amount)
    }

    /// Retreat a timestamp by the least amount possible such that
    /// `ts.step_back().unwrap().less_than(ts)` is true. Return `None` if unable,
    /// which must only happen if the timestamp is `Timestamp::minimum()`.
    pub fn step_back(&self) -> Option<Self> {
        self.checked_sub(1)
    }
}

impl PartialEq<&SysTime> for SysTime {
    fn eq(&self, other: &&SysTime) -> bool {
        self.eq(*other)
    }
}

impl PartialEq<SysTime> for &SysTime {
    fn eq(&self, other: &SysTime) -> bool {
        self.internal.eq(&other.internal)
    }
}

impl Serialize for SysTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.internal.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SysTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self {
            internal: u64::deserialize(deserializer)?,
        })
    }
}

impl PartialOrder for SysTime {
    fn less_equal(&self, other: &Self) -> bool {
        self.internal.less_equal(&other.internal)
    }
}

impl PartialOrder<&SysTime> for SysTime {
    fn less_equal(&self, other: &&Self) -> bool {
        self.internal.less_equal(&other.internal)
    }
}

impl PartialOrder<SysTime> for &SysTime {
    fn less_equal(&self, other: &SysTime) -> bool {
        self.internal.less_equal(&other.internal)
    }
}

impl TotalOrder for SysTime {}

impl Timestamp for SysTime {
    type Summary = SysTime;

    fn minimum() -> Self {
        Self::MIN
    }
}

impl PathSummary<SysTime> for SysTime {
    #[inline]
    fn results_in(&self, src: &SysTime) -> Option<SysTime> {
        self.internal
            .checked_add(src.internal)
            .map(|internal| Self { internal })
    }
    #[inline]
    fn followed_by(&self, other: &SysTime) -> Option<SysTime> {
        self.internal
            .checked_add(other.internal)
            .map(|internal| Self { internal })
    }
}

impl Refines<()> for SysTime {
    fn to_inner(_: ()) -> SysTime {
        Default::default()
    }
    fn to_outer(self) -> () {
        ()
    }
    fn summarize(_: <SysTime as Timestamp>::Summary) -> () {
        ()
    }
}

impl Lattice for SysTime {
    #[inline]
    fn join(&self, other: &Self) -> Self {
        ::std::cmp::max(*self, *other)
    }
    #[inline]
    fn meet(&self, other: &Self) -> Self {
        ::std::cmp::min(*self, *other)
    }
}

impl std::fmt::Display for SysTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.internal, f)
    }
}

impl std::fmt::Debug for SysTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.internal, f)
    }
}

impl From<u64> for SysTime {
    fn from(internal: u64) -> Self {
        Self { internal }
    }
}
