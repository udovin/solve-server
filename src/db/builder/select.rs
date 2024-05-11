use solve_db::{IntoQuery, QueryBuilder, RawQuery};

use super::Predicate;

#[derive(Clone, Debug)]
pub struct Select {
    table: String,
    columns: Vec<String>,
    predicate: Predicate,
    order_by: Vec<String>,
    limit: usize,
}

impl Select {
    pub fn new() -> Self {
        Self {
            table: Default::default(),
            columns: Default::default(),
            predicate: Predicate::Bool(false),
            order_by: Default::default(),
            limit: 0,
        }
    }

    pub fn with_table<T: Into<String>>(mut self, table: T) -> Self {
        self.table = table.into();
        self
    }

    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    pub fn with_where<T: Into<Predicate>>(mut self, predicate: T) -> Self {
        self.predicate = predicate.into();
        self
    }

    pub fn with_order_by(mut self, columns: Vec<String>) -> Self {
        self.order_by = columns;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

impl Default for Select {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoQuery<RawQuery> for Select {
    fn into_query(self, mut builder: QueryBuilder) -> RawQuery {
        assert!(!self.columns.is_empty());
        builder.push_str("SELECT ");
        for (i, column) in self.columns.into_iter().enumerate() {
            if i > 0 {
                builder.push_str(", ");
            }
            builder.push_name(&column);
        }
        builder.push_str(" FROM ");
        builder.push_name(&self.table);
        builder.push_str(" WHERE ");
        self.predicate.push_into(&mut builder);
        if !self.order_by.is_empty() {
            builder.push_str(" ORDER BY ");
            for (i, name) in self.order_by.into_iter().enumerate() {
                if i > 0 {
                    builder.push_str(", ");
                }
                builder.push_name(&name);
            }
        }
        if self.limit > 0 {
            builder.push_str(" LIMIT ");
            builder.push_str(&self.limit.to_string())
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use solve_db::{driver, IntoQuery, IntoValue, Query, QueryBuilder, RawQuery, Value};

    use super::{super::column, Predicate, Select};

    struct TestBuilder {
        query: String,
        values: Vec<Value>,
    }

    impl TestBuilder {
        pub fn new() -> QueryBuilder {
            QueryBuilder::new(Self {
                query: Default::default(),
                values: Default::default(),
            })
        }
    }

    impl driver::QueryBuilder for TestBuilder {
        fn push(&mut self, ch: char) {
            self.query.push(ch);
        }

        fn push_str(&mut self, part: &str) {
            self.query.push_str(part);
        }

        fn push_name(&mut self, name: &str) {
            assert!(name.find(|c| c == '"' || c == '\\').is_none());
            self.push('"');
            self.push_str(name);
            self.push('"');
        }

        fn push_value(&mut self, value: Value) {
            self.values.push(value);
            self.push_str(format!("${}", self.values.len()).as_str())
        }

        fn build(self: Box<Self>) -> RawQuery {
            RawQuery::new(self.query, self.values)
        }
    }

    #[test]
    fn bool_expression() {
        {
            let mut builder = TestBuilder::new();
            Predicate::Bool(true).push_into(&mut builder);
            assert_eq!(builder.build().query(), "true");
        }
        {
            let mut builder = TestBuilder::new();
            Predicate::Bool(false).push_into(&mut builder);
            assert_eq!(builder.build().query(), "false");
        }
        {
            let mut builder = TestBuilder::new();
            Predicate::Bool(true)
                .and(Predicate::Bool(false))
                .push_into(&mut builder);
            assert_eq!(builder.build().query(), "true AND false");
        }
        {
            let mut builder = TestBuilder::new();
            column("col").equal("42").push_into(&mut builder);
            assert_eq!(builder.build().query(), "\"col\" = $1");
        }
        {
            let mut builder = TestBuilder::new();
            column("col").not_equal("42").push_into(&mut builder);
            assert_eq!(builder.build().query(), "\"col\" <> $1");
        }
        {
            let mut builder = TestBuilder::new();
            column("col").less("42").push_into(&mut builder);
            assert_eq!(builder.build().query(), "\"col\" < $1");
        }
        {
            let mut builder = TestBuilder::new();
            column("col").greater("42").push_into(&mut builder);
            assert_eq!(builder.build().query(), "\"col\" > $1");
        }
        {
            let mut builder = TestBuilder::new();
            column("col").less_equal("42").push_into(&mut builder);
            assert_eq!(builder.build().query(), "\"col\" <= $1");
        }
        {
            let mut builder = TestBuilder::new();
            column("col").greater_equal("42").push_into(&mut builder);
            assert_eq!(builder.build().query(), "\"col\" >= $1");
        }
    }

    #[test]
    fn select_query() {
        {
            let query = Select::new()
                .with_table("tbl")
                .with_columns(vec!["col1".to_string(), "col2".to_string()])
                .into_query(TestBuilder::new());
            assert_eq!(
                query.query(),
                "SELECT \"col1\", \"col2\" FROM \"tbl\" WHERE false"
            );
            assert!(query.values().is_empty());
        }
        {
            let query = Select::new()
                .with_table("tbl")
                .with_columns(vec!["col1".to_string(), "col2".to_string()])
                .with_where(false)
                .into_query(TestBuilder::new());
            assert_eq!(
                query.query(),
                "SELECT \"col1\", \"col2\" FROM \"tbl\" WHERE false"
            );
            assert!(query.values().is_empty());
        }
        {
            let query = Select::new()
                .with_table("tbl")
                .with_columns(vec!["col1".to_string(), "col2".to_string()])
                .with_where(true)
                .into_query(TestBuilder::new());
            assert_eq!(
                query.query(),
                "SELECT \"col1\", \"col2\" FROM \"tbl\" WHERE true"
            );
            assert!(query.values().is_empty());
        }
        {
            let query = Select::new()
                .with_table("tbl")
                .with_columns(vec!["col1".to_string(), "col2".to_string()])
                .with_where(column("col1").greater(5).and(column("col2").equal("abc")))
                .into_query(TestBuilder::new());
            assert_eq!(
                query.query(),
                "SELECT \"col1\", \"col2\" FROM \"tbl\" WHERE \"col1\" > $1 AND \"col2\" = $2"
            );
            assert_eq!(query.values(), vec![5.into_value(), "abc".into_value()],);
        }
    }
}
