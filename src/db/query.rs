use crate::db::Value;

pub trait Query: Send + Sync {
    fn query(&self) -> &str;

    fn values(&self) -> &[Value];
}

impl Query for &str {
    fn query(&self) -> &str {
        self
    }

    fn values(&self) -> &[Value] {
        &[]
    }
}

impl Query for (&str, &[Value]) {
    fn query(&self) -> &str {
        self.0
    }

    fn values(&self) -> &[Value] {
        self.1
    }
}

pub struct RawQuery {
    query: String,
    values: Vec<Value>,
}

impl RawQuery {
    pub fn new<Q, V>(query: Q, values: V) -> Self
    where
        Q: Into<String>,
        V: Into<Vec<Value>>,
    {
        Self {
            query: query.into(),
            values: values.into(),
        }
    }
}

impl Query for RawQuery {
    fn query(&self) -> &str {
        &self.query
    }

    fn values(&self) -> &[Value] {
        &self.values
    }
}

pub trait IntoQuery<T: Query>: Send + Sync {
    fn into_query(self, builer: QueryBuilder) -> T;
}

impl<T: Query> IntoQuery<T> for T {
    fn into_query(self, _builer: QueryBuilder) -> T {
        self
    }
}

pub trait QueryBuilderBackend: Send + Sync {
    fn push(&mut self, ch: char);

    fn push_str(&mut self, string: &str);

    fn push_name(&mut self, name: &str);

    fn push_value(&mut self, value: Value);

    fn build(self: Box<Self>) -> RawQuery;
}

pub struct QueryBuilder {
    inner: Box<dyn QueryBuilderBackend>,
}

impl QueryBuilder {
    pub fn new<T: QueryBuilderBackend + 'static>(builder: T) -> Self {
        let inner = Box::new(builder);
        Self { inner }
    }

    pub fn push(&mut self, ch: char) {
        self.inner.push(ch);
    }

    pub fn push_str(&mut self, string: &str) {
        self.inner.push_str(string);
    }

    pub fn push_name(&mut self, name: &str) {
        self.inner.push_name(name);
    }

    pub fn push_value<T: Into<Value>>(&mut self, value: T) {
        self.inner.push_value(value.into());
    }

    pub fn build(self) -> RawQuery {
        self.inner.build()
    }
}
