use solve_db::{IntoQuery, QueryBuilder, RawQuery};

use super::Predicate;

#[derive(Clone, Debug)]
pub struct Delete {
    table: String,
    predicate: Predicate,
}

impl Delete {
    pub fn new() -> Self {
        Self {
            table: Default::default(),
            predicate: Predicate::Bool(false),
        }
    }

    pub fn with_table<T: Into<String>>(mut self, table: T) -> Self {
        self.table = table.into();
        self
    }

    pub fn with_where<T: Into<Predicate>>(mut self, predicate: T) -> Self {
        self.predicate = predicate.into();
        self
    }
}

impl Default for Delete {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoQuery<RawQuery> for Delete {
    fn into_query(self, mut builder: QueryBuilder) -> RawQuery {
        builder.push_str("DELETE FROM ");
        builder.push_name(&self.table);
        builder.push_str(" WHERE ");
        self.predicate.push_into(&mut builder);
        builder.build()
    }
}
