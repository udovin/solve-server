use solve_db::{IntoQuery, IntoRow, QueryBuilder, RawQuery, Value};

use super::Predicate;

#[derive(Clone, Debug)]
pub struct Update {
    table: String,
    update: Vec<(String, Value)>,
    predicate: Predicate,
    returning: Vec<String>,
}

impl Update {
    pub fn new() -> Self {
        Self {
            table: Default::default(),
            update: Default::default(),
            predicate: Predicate::Bool(false),
            returning: Default::default(),
        }
    }

    pub fn with_table<T: Into<String>>(mut self, table: T) -> Self {
        self.table = table.into();
        self
    }

    pub fn with_update(mut self, update: Vec<(String, Value)>) -> Self {
        self.update = update;
        self
    }

    pub fn with_where<T: Into<Predicate>>(mut self, predicate: T) -> Self {
        self.predicate = predicate.into();
        self
    }

    pub fn with_returning(mut self, columns: Vec<String>) -> Self {
        self.returning = columns;
        self
    }

    pub fn with_row<T: IntoRow>(self, row: T) -> Self {
        self.with_update(row.into_row())
    }
}

impl Default for Update {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoQuery<RawQuery> for Update {
    fn into_query(self, mut builder: QueryBuilder) -> RawQuery {
        assert!(!self.update.is_empty());
        builder.push_str("UPDATE ");
        builder.push_name(&self.table);
        builder.push_str(" SET ");
        for (i, (column, value)) in self.update.into_iter().enumerate() {
            if i > 0 {
                builder.push_str(", ");
            }
            builder.push_name(&column);
            builder.push_str(" = ");
            builder.push_value(value);
        }
        builder.push_str(" WHERE ");
        self.predicate.push_into(&mut builder);
        if !self.returning.is_empty() {
            builder.push_str(" RETURNING ");
            for (i, name) in self.returning.into_iter().enumerate() {
                if i > 0 {
                    builder.push_str(", ");
                }
                builder.push_name(&name);
            }
        }
        builder.build()
    }
}
