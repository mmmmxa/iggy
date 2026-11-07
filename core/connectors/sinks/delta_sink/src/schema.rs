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

use deltalake::kernel::{DataType, StructType};
use serde::Deserialize;
use std::collections::BTreeMap;

/// A column type as written in the `expected_schema` configuration.
///
/// A string is a leaf type spelled as Delta prints it, for example `"long"`,
/// `"timestamp_ntz"` or `"map<string, long>"`. A one-element list is an array of that
/// element type. A table is a struct of named columns.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ExpectedType {
    Leaf(String),
    Array(Vec<ExpectedType>),
    Struct(BTreeMap<String, ExpectedType>),
}

/// Returns one message per difference between the configured schema and the table schema.
/// The result is empty when the two match.
///
/// Only column names and types are compared. Nullability is left out on purpose: Delta
/// writers disagree on nullable flags, especially for nested fields, so enforcing them
/// causes false mismatches. Enforcing nullability is left to downstream processing.
pub(crate) fn schema_differences(
    expected: &BTreeMap<String, ExpectedType>,
    table: &StructType,
) -> Vec<String> {
    let mut differences = Vec::new();
    struct_differences("", expected, table, &mut differences);
    differences
}

fn struct_differences(
    path: &str,
    expected: &BTreeMap<String, ExpectedType>,
    table: &StructType,
    differences: &mut Vec<String>,
) {
    for (name, expected_type) in expected {
        let column = column_path(path, name);
        match table.field(name) {
            Some(field) => type_differences(&column, expected_type, field.data_type(), differences),
            None => differences.push(format!(
                "column \"{column}\" is in expected_schema but not in the table"
            )),
        }
    }
    for field in table.fields() {
        if !expected.contains_key(field.name()) {
            differences.push(format!(
                "column \"{}\" is in the table but not in expected_schema",
                column_path(path, field.name())
            ));
        }
    }
}

fn type_differences(
    column: &str,
    expected: &ExpectedType,
    table: &DataType,
    differences: &mut Vec<String>,
) {
    match (expected, table) {
        (ExpectedType::Struct(fields), DataType::Struct(table_fields)) => {
            struct_differences(column, fields, table_fields, differences);
        }
        (ExpectedType::Array(elements), DataType::Array(array)) if elements.len() == 1 => {
            type_differences(
                &format!("{column}[]"),
                &elements[0],
                array.element_type(),
                differences,
            );
        }
        (ExpectedType::Leaf(name), _) if *name == table.to_string() => {}
        _ => differences.push(format!(
            "column \"{column}\" has type \"{}\" in expected_schema but \"{table}\" in the table",
            expected.describe()
        )),
    }
}

fn column_path(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_string()
    } else {
        format!("{path}.{name}")
    }
}

impl ExpectedType {
    fn describe(&self) -> String {
        match self {
            ExpectedType::Leaf(name) => name.clone(),
            ExpectedType::Array(elements) if elements.len() == 1 => {
                format!("array<{}>", elements[0].describe())
            }
            ExpectedType::Array(_) => "array".to_string(),
            ExpectedType::Struct(_) => "struct".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::kernel::Schema as DeltaSchema;
    use serde_json::json;

    fn table() -> DeltaSchema {
        serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                { "name": "id", "type": "long", "nullable": false, "metadata": {} },
                { "name": "name", "type": "string", "nullable": true, "metadata": {} },
                {
                    "name": "address",
                    "type": {
                        "type": "struct",
                        "fields": [
                            { "name": "city", "type": "string", "nullable": true, "metadata": {} }
                        ]
                    },
                    "nullable": true, "metadata": {}
                },
                {
                    "name": "tags",
                    "type": { "type": "array", "elementType": "string", "containsNull": false },
                    "nullable": true, "metadata": {}
                },
                {
                    "name": "events",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "struct",
                            "fields": [
                                { "name": "at", "type": "timestamp_ntz", "nullable": true, "metadata": {} }
                            ]
                        },
                        "containsNull": true
                    },
                    "nullable": true, "metadata": {}
                },
                {
                    "name": "attributes",
                    "type": {
                        "type": "map",
                        "keyType": "string",
                        "valueType": "long",
                        "valueContainsNull": true
                    },
                    "nullable": true, "metadata": {}
                }
            ]
        }))
        .unwrap()
    }

    fn expected(value: serde_json::Value) -> BTreeMap<String, ExpectedType> {
        serde_json::from_value(value).unwrap()
    }

    fn matching() -> serde_json::Value {
        json!({
            "id": "long",
            "name": "string",
            "address": { "city": "string" },
            "tags": ["string"],
            "events": [{ "at": "timestamp_ntz" }],
            "attributes": "map<string, long>"
        })
    }

    #[test]
    fn given_matching_names_and_types_when_comparing_should_ignore_nullability() {
        // "id" is non-nullable and "tags" has containsNull = false in the table, and the
        // configuration cannot express either.
        assert!(schema_differences(&expected(matching()), &table()).is_empty());
    }

    #[test]
    fn given_column_missing_from_table_when_comparing_should_name_it() {
        let mut schema = matching();
        schema["email"] = json!("string");

        let differences = schema_differences(&expected(schema), &table());

        assert_eq!(
            differences,
            vec!["column \"email\" is in expected_schema but not in the table"]
        );
    }

    #[test]
    fn given_column_missing_from_configuration_when_comparing_should_name_it() {
        let mut schema = matching();
        schema.as_object_mut().unwrap().remove("name");

        let differences = schema_differences(&expected(schema), &table());

        assert_eq!(
            differences,
            vec!["column \"name\" is in the table but not in expected_schema"]
        );
    }

    #[test]
    fn given_wrong_type_when_comparing_should_name_both_types() {
        let mut schema = matching();
        schema["id"] = json!("integer");

        let differences = schema_differences(&expected(schema), &table());

        assert_eq!(
            differences,
            vec!["column \"id\" has type \"integer\" in expected_schema but \"long\" in the table"]
        );
    }

    #[test]
    fn given_nested_differences_when_comparing_should_report_full_paths() {
        let mut schema = matching();
        schema["address"] = json!({ "city": "integer", "zip": "string" });
        schema["events"] = json!([{ "at": "timestamp" }]);
        schema["tags"] = json!(["long"]);

        let mut differences = schema_differences(&expected(schema), &table());
        differences.sort();

        assert_eq!(
            differences,
            vec![
                "column \"address.city\" has type \"integer\" in expected_schema but \"string\" in the table",
                "column \"address.zip\" is in expected_schema but not in the table",
                "column \"events[].at\" has type \"timestamp\" in expected_schema but \"timestamp_ntz\" in the table",
                "column \"tags[]\" has type \"long\" in expected_schema but \"string\" in the table",
            ]
        );
    }

    #[test]
    fn given_kind_mismatch_when_comparing_should_report_one_difference() {
        let mut schema = matching();
        schema["tags"] = json!("string");
        schema["address"] = json!(["string"]);
        schema["id"] = json!({ "value": "long" });

        let mut differences = schema_differences(&expected(schema), &table());
        differences.sort();

        assert_eq!(differences.len(), 3, "{differences:?}");
        assert!(differences[0].starts_with("column \"address\" has type \"array<string>\""));
        assert!(differences[1].starts_with("column \"id\" has type \"struct\""));
        assert!(differences[2].starts_with("column \"tags\" has type \"string\""));
    }

    #[test]
    fn given_array_with_two_element_types_when_comparing_should_report_a_difference() {
        let mut schema = matching();
        schema["tags"] = json!(["string", "long"]);

        let differences = schema_differences(&expected(schema), &table());

        assert_eq!(differences.len(), 1);
        assert!(differences[0].starts_with("column \"tags\" has type \"array\""));
    }
}
