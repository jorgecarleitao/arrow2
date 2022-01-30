pub enum MaybeMut<I, M> {
    Immutable(I),
    Mutable(M),
}

impl<I, M> MaybeMut<I, M> {
    pub fn unwrap_mut(self) -> M {
        match self {
            MaybeMut::Mutable(m) => m,
            MaybeMut::Immutable(_) => panic!("was immutable"),
        }
    }
}
