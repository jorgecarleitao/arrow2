use super::{Field, Metadata};

/// An ordered sequence of [`Field`]s with associated [`Metadata`].
///
/// [`Schema`] is an abstration used to read from, and write to, Arrow IPC format,
/// Apache Parquet, and Apache Avro. All these formats have a concept of a schema
/// with fields and metadata.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Schema {
    /// The fields composing this schema.
    pub fields: Vec<Field>,
    /// Optional metadata.
    pub metadata: Metadata,
}

impl Schema {
    /// Attaches a [`Metadata`] to [`Schema`]
    #[inline]
    pub fn with_metadata(self, metadata: Metadata) -> Self {
        Self {
            fields: self.fields,
            metadata,
        }
    }
}

impl From<Vec<Field>> for Schema {
    fn from(fields: Vec<Field>) -> Self {
        Self {
            fields,
            ..Default::default()
        }
    }
}
