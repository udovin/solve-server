use std::{
    collections::{hash_map, HashMap},
    sync::Arc,
};

use crate::{FromValue, Value};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait RowIndex<I> {
    fn index(&self, index: I) -> Option<usize>;
}

#[derive(Clone, Debug)]
pub struct ColumnIndex(Arc<HashMap<String, usize>>);

impl ColumnIndex {
    pub fn new(columns: Vec<String>) -> Self {
        let mut map = HashMap::with_capacity(columns.len());
        for (i, column) in columns.into_iter().enumerate() {
            map.insert(column, i);
        }
        Self(Arc::new(map))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, column: &str) -> Option<usize> {
        self.0.get(column).cloned()
    }
}

pub type SimpleRow = Vec<(String, Value)>;

#[derive(Clone, Debug)]
pub struct Row {
    columns: ColumnIndex,
    values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>, columns: ColumnIndex) -> Self {
        assert_eq!(values.len(), columns.len());
        Self { values, columns }
    }

    pub fn from_iter<I: Iterator<Item = (String, Value)>>(iter: I) -> Row {
        let (columns, values): (Vec<_>, _) = iter.unzip();
        Row {
            values,
            columns: ColumnIndex::new(columns),
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn iter(&self) -> RowIter {
        RowIter {
            iter: self.columns.0.iter(),
            values: &self.values,
        }
    }

    pub fn get_value<I>(&self, index: I) -> Option<&Value>
    where
        Self: RowIndex<I>,
    {
        self.values.get(self.index(index)?)
    }

    pub fn get_parsed<I, T>(&self, index: I) -> Result<T, Error>
    where
        Self: RowIndex<I>,
        T: FromValue,
    {
        self.get_value(index).ok_or("invalid index")?.parse()
    }
}

impl RowIndex<usize> for Row {
    fn index(&self, index: usize) -> Option<usize> {
        if index < self.values.len() {
            Some(index)
        } else {
            None
        }
    }
}

impl RowIndex<&str> for Row {
    fn index(&self, index: &str) -> Option<usize> {
        self.columns.get(index)
    }
}

impl RowIndex<String> for Row {
    fn index(&self, index: String) -> Option<usize> {
        self.columns.get(&index)
    }
}

impl RowIndex<&String> for Row {
    fn index(&self, index: &String) -> Option<usize> {
        self.columns.get(index)
    }
}

pub struct RowIter<'a> {
    iter: hash_map::Iter<'a, String, usize>,
    values: &'a [Value],
}

impl<'a> Iterator for RowIter<'a> {
    type Item = (&'a str, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        let (column, index) = self.iter.next()?;
        let value = self.values.get(*index)?;
        Some((column.as_str(), value))
    }
}

pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self, Error>;
}

pub trait IntoRow: Sized {
    fn into_row(self) -> SimpleRow;
}

impl FromRow for Row {
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok(row.clone())
    }
}

impl FromRow for SimpleRow {
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok(row
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect::<Vec<_>>())
    }
}

impl IntoRow for Row {
    fn into_row(self) -> SimpleRow {
        self.iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect()
    }
}

impl IntoRow for SimpleRow {
    fn into_row(self) -> Self {
        self
    }
}
