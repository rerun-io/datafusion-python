// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::common::TableReference;
use pyo3::prelude::*;

/// PyO3 requires that objects passed between Rust and Python implement the trait `PyClass`
/// Since `TableReference` exists in another package we cannot make that happen here so we wrap
/// `TableReference` as `PyTableReference` This exists solely to satisfy those constraints.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[pyclass(name = "TableReference", module = "datafusion.common", subclass)]
pub struct PyTableReference {
    pub table_reference: TableReference,
}

impl PyTableReference {
    pub fn new(table_reference: TableReference) -> Self {
        Self { table_reference }
    }
}

impl From<PyTableReference> for TableReference {
    fn from(py_table_reference: PyTableReference) -> TableReference {
        py_table_reference.table_reference
    }
}

impl From<TableReference> for PyTableReference {
    fn from(table_reference: TableReference) -> PyTableReference {
        PyTableReference { table_reference }
    }
}

impl std::fmt::Display for PyTableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.table_reference.fmt(f)
    }
}

#[pymethods]
impl PyTableReference {
    /// Create a bare (unqualified) table reference
    #[staticmethod]
    pub fn bare(table: &str) -> Self {
        Self::new(TableReference::bare(table))
    }

    /// Create a partial (schema.table) table reference
    #[staticmethod]
    pub fn partial(schema: &str, table: &str) -> Self {
        Self::new(TableReference::partial(schema, table))
    }

    /// Create a full (catalog.schema.table) table reference
    #[staticmethod]
    pub fn full(catalog: &str, schema: &str, table: &str) -> Self {
        Self::new(TableReference::full(catalog, schema, table))
    }

    /// Get the table name
    pub fn table(&self) -> &str {
        self.table_reference.table()
    }

    /// Get the schema name if present
    pub fn schema(&self) -> Option<&str> {
        self.table_reference.schema()
    }

    /// Get the catalog name if present
    pub fn catalog(&self) -> Option<&str> {
        self.table_reference.catalog()
    }

    /// Check if this is a bare table reference
    pub fn is_bare(&self) -> bool {
        matches!(self.table_reference, TableReference::Bare { .. })
    }

    /// Check if this is a partial table reference
    pub fn is_partial(&self) -> bool {
        matches!(self.table_reference, TableReference::Partial { .. })
    }

    /// Check if this is a full table reference
    pub fn is_full(&self) -> bool {
        matches!(self.table_reference, TableReference::Full { .. })
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    fn __repr__(&self) -> String {
        format!("TableReference('{self}')")
    }
}
