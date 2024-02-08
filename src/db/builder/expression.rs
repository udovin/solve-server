use crate::db::{QueryBuilder, Value};

#[derive(Debug, Clone)]
pub struct BinaryExpression {
    left: Box<Expression>,
    right: Box<Expression>,
}

impl BinaryExpression {
    fn write_to(self, builder: &mut QueryBuilder, delim: &str) {
        self.left.write_to(builder);
        builder.push_str(delim);
        self.right.write_to(builder);
    }
}

#[derive(Debug, Clone)]
pub enum Expression {
    Value(Value),
    Column(String),
    Raw(String),
}

impl Expression {
    pub fn equal<T: Into<Expression>>(self, rhs: T) -> Predicate {
        match rhs.into() {
            Expression::Value(Value::Null) => Predicate::IsNull(Box::new(self)),
            v => Predicate::Equal(BinaryExpression {
                left: Box::new(self),
                right: Box::new(v),
            }),
        }
    }

    pub fn not_equal<T: Into<Expression>>(self, rhs: T) -> Predicate {
        match rhs.into() {
            Expression::Value(Value::Null) => Predicate::IsNotNull(Box::new(self)),
            v => Predicate::NotEqual(BinaryExpression {
                left: Box::new(self),
                right: Box::new(v),
            }),
        }
    }

    pub fn less<T: Into<Expression>>(self, rhs: T) -> Predicate {
        Predicate::Less(BinaryExpression {
            left: Box::new(self),
            right: Box::new(rhs.into()),
        })
    }

    pub fn greater<T: Into<Expression>>(self, rhs: T) -> Predicate {
        Predicate::Greater(BinaryExpression {
            left: Box::new(self),
            right: Box::new(rhs.into()),
        })
    }

    pub fn less_equal<T: Into<Expression>>(self, rhs: T) -> Predicate {
        Predicate::LessEqual(BinaryExpression {
            left: Box::new(self),
            right: Box::new(rhs.into()),
        })
    }

    pub fn greater_equal<T: Into<Expression>>(self, rhs: T) -> Predicate {
        Predicate::GreaterEqual(BinaryExpression {
            left: Box::new(self),
            right: Box::new(rhs.into()),
        })
    }

    fn write_to(self, builder: &mut QueryBuilder) {
        match self {
            Expression::Value(v) => builder.push_value(v),
            Expression::Column(v) => builder.push_name(&v),
            Expression::Raw(v) => builder.push_str(&v),
        }
    }
}

impl<T: Into<Value>> From<T> for Expression {
    fn from(value: T) -> Self {
        Self::Value(value.into())
    }
}

pub fn column<T: Into<String>>(column: T) -> Expression {
    Expression::Column(column.into())
}

#[derive(Debug, Clone)]
pub struct BinaryPredicate {
    left: Box<Predicate>,
    right: Box<Predicate>,
}

impl BinaryPredicate {
    fn write_to(
        self,
        builder: &mut QueryBuilder,
        delim: &str,
        disc: std::mem::Discriminant<Predicate>,
    ) {
        let wrap_left = std::mem::discriminant(self.left.as_ref()) != disc;
        self.left.write_to(builder, wrap_left);
        builder.push_str(delim);
        let wrap_right = std::mem::discriminant(self.right.as_ref()) != disc;
        self.right.write_to(builder, wrap_right);
    }
}

#[derive(Debug, Clone)]
pub enum Predicate {
    Bool(bool),
    And(BinaryPredicate),
    Or(BinaryPredicate),
    Equal(BinaryExpression),
    NotEqual(BinaryExpression),
    Less(BinaryExpression),
    LessEqual(BinaryExpression),
    Greater(BinaryExpression),
    GreaterEqual(BinaryExpression),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
}

impl Predicate {
    pub fn and(self, rhs: Predicate) -> Predicate {
        Predicate::And(BinaryPredicate {
            left: Box::new(self),
            right: Box::new(rhs),
        })
    }

    pub fn or(self, rhs: Predicate) -> Predicate {
        Predicate::Or(BinaryPredicate {
            left: Box::new(self),
            right: Box::new(rhs),
        })
    }

    pub fn push_into(self, builder: &mut QueryBuilder) {
        let disc = std::mem::discriminant(&self);
        match self {
            Predicate::Bool(v) => builder.push_str(if v { "true" } else { "false" }),
            Predicate::And(v) => v.write_to(builder, " AND ", disc),
            Predicate::Or(v) => v.write_to(builder, " OR ", disc),
            Predicate::Equal(v) => v.write_to(builder, " = "),
            Predicate::NotEqual(v) => v.write_to(builder, " <> "),
            Predicate::Less(v) => v.write_to(builder, " < "),
            Predicate::LessEqual(v) => v.write_to(builder, " <= "),
            Predicate::Greater(v) => v.write_to(builder, " > "),
            Predicate::GreaterEqual(v) => v.write_to(builder, " >= "),
            Predicate::IsNull(v) => {
                v.write_to(builder);
                builder.push_str(" IS NULL");
            }
            Predicate::IsNotNull(v) => {
                v.write_to(builder);
                builder.push_str(" IS NOT NULL");
            }
        }
    }

    fn write_to(self, builder: &mut QueryBuilder, wrap: bool) {
        let wrap = match self {
            Predicate::And(_) | Predicate::Or(_) => wrap,
            _ => false,
        };
        if wrap {
            builder.push_str("(");
        }
        self.push_into(builder);
        if wrap {
            builder.push_str(")");
        }
    }
}

impl From<bool> for Predicate {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}
