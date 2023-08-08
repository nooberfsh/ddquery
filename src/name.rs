use std::fmt::{Debug, Display, Formatter};

use compact_str::CompactString;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Name {
    s: CompactString
}

impl<'a> From<&'a str> for Name {
    fn from(value: &'a str) -> Self {
        Name {
            s: CompactString::new(value)
        }
    }
}

impl From<String> for Name {
    fn from(value: String) -> Self {
        Name {
            s: CompactString::from(value)
        }
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
         write!(f, "{}", self.s)
    }
}

impl Debug for Name {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show() {
        let a = Name::from("abc");
        let s = format!("{}",a );
        assert_eq!(s, "abc");

        let s = format!("{:?}", a);
        assert_eq!(s, "abc");
    }
}