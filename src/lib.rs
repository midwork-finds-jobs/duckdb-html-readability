extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    ffi,
    ffi::duckdb_string_t,
    types::DuckString,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use readability::extractor;
use std::error::Error;
use std::io::Cursor;

/// State for the parse_html function
#[derive(Default)]
struct ParseHtmlState;

/// The parse_html scalar function implementation
struct ParseHtmlFunction;

impl VScalar for ParseHtmlFunction {
    type State = ParseHtmlState;

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let count = input.len();
        let values = input.flat_vector(0);
        let values = values.as_slice_with_len::<duckdb_string_t>(count);

        // Get the struct vector and its children
        let struct_vec = output.struct_vector();
        let title_vec = struct_vec.child(0, count);
        let content_vec = struct_vec.child(1, count);
        let text_vec = struct_vec.child(2, count);

        let url = url::Url::parse("http://example.com").unwrap();

        for (i, val) in values.iter().enumerate() {
            // Read the HTML string
            let mut val_copy = *val;
            let mut duck_string = DuckString::new(&mut val_copy);
            let html_str = duck_string.as_str();

            // Parse HTML with readability
            let mut cursor = Cursor::new(html_str.as_bytes());

            match extractor::extract(&mut cursor, &url) {
                Ok(product) => {
                    title_vec.insert(i, product.title.as_str());
                    content_vec.insert(i, product.content.as_str());
                    text_vec.insert(i, product.text.as_str());
                }
                Err(_) => {
                    title_vec.insert(i, "");
                    content_vec.insert(i, "");
                    text_vec.insert(i, "");
                }
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        let varchar_type = LogicalTypeHandle::from(LogicalTypeId::Varchar);

        let return_type = LogicalTypeHandle::struct_type(&[
            ("title", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ("content", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ("text", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ]);

        vec![ScalarFunctionSignature::exact(
            vec![varchar_type],
            return_type,
        )]
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<ParseHtmlFunction>("parse_html")
        .expect("Failed to register parse_html function");
    Ok(())
}
