use solve_db::{Error, IntoQuery, QueryBuilder, RawQuery, Value};

#[derive(Clone, Debug)]
pub struct Insert {
    table: String,
    columns: Vec<String>,
    values: Vec<Value>,
    returning: Vec<String>,
}

pub type Row = Vec<(String, Value)>;

pub trait FromRow: Sized {
    fn from_row(row: &solve_db::Row) -> Result<Self, Error>;
}

pub trait IntoRow {
    fn into_row(self) -> Row;
}

impl IntoRow for Row {
    fn into_row(self) -> Row {
        self
    }
}

impl Insert {
    pub fn new() -> Self {
        Self {
            table: Default::default(),
            columns: Default::default(),
            values: Default::default(),
            returning: Default::default(),
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

    pub fn with_values(mut self, values: Vec<Value>) -> Self {
        self.values = values;
        self
    }

    pub fn with_returning(mut self, columns: Vec<String>) -> Self {
        self.returning = columns;
        self
    }

    pub fn with_row<T: IntoRow>(self, row: T) -> Self {
        let (columns, values) = row.into_row().into_iter().unzip();
        self.with_columns(columns).with_values(values)
    }
}

impl Default for Insert {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoQuery<RawQuery> for Insert {
    fn into_query(self, mut builder: QueryBuilder) -> RawQuery {
        assert_eq!(self.columns.len(), self.values.len());
        builder.push_str("INSERT INTO ");
        builder.push_name(&self.table);
        builder.push_str(" (");
        for (i, column) in self.columns.into_iter().enumerate() {
            if i > 0 {
                builder.push_str(", ");
            }
            builder.push_name(&column);
        }
        builder.push_str(") VALUES (");
        for (i, value) in self.values.into_iter().enumerate() {
            if i > 0 {
                builder.push_str(", ");
            }
            builder.push_value(value);
        }
        builder.push_str(")");
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
