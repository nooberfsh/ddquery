#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct GID (pub (crate) u64);

pub struct GIDGen(u64);

impl GIDGen {
    pub fn new() -> GIDGen {
        GIDGen(0)
    }

    pub fn next(&mut self) -> GID {
        let ret = GID(self.0);
        self.0 +=1;
        ret
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_generate_gid() {
        let mut g = GIDGen::new();
        let a = g.next();
        assert_eq!(a, GID(0));

        let a = g.next();
        assert_eq!(a, GID(1));
        let a = g.next();
        assert_eq!(a, GID(2));
    }
}