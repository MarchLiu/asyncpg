use std::num::NonZeroUsize;
use pgrx::AnyElement;
use pgrx::prelude::*;

pgrx::pg_module_magic!();

#[pg_extern]
fn log_data(line: &str) -> Result<(), spi::Error> {
    pgrx::log!("data [{}] from function call", line);
    Ok(())
}

#[pg_extern]
fn test_anyelement(type_placeholder: AnyElement)  -> Result<SetOfIterator<'static, AnyElement>, spi::Error> {
    let oid = type_placeholder.oid();
    pgrx::log!("result oid {}", oid);

    let empty:Vec<AnyElement> = vec!();
    Ok(SetOfIterator::new(empty.into_iter()))
}

#[pg_extern]
fn run_query(type_placeholder: AnyElement, query: &str) -> Result<SetOfIterator<'static, AnyElement>, spi::Error> {
    let oid = type_placeholder.oid();
    pgrx::log!("result oid {}", oid);

    Spi::connect(|client| {
        let mut results = Vec::new();
        let mut tup_table = client.select(query, None, None)?;
        let ncols = tup_table.columns().unwrap();

        unsafe {
            while let Some(row) = tup_table.next() {
                // pg_sys::heap_copy_tuple_as_datum(self.tuple.as_ptr(), self.tupdesc.as_ptr())
                // pg_sys::heap_copy_tuple_as_datum(row.tuple.as_ptr(), row.tupdesc.as_ptr());
                let mut heap_tuple = PgHeapTuple::new_composite_type_by_oid(oid).unwrap();
                for idx in 0..ncols {
                    let no: NonZeroUsize = NonZeroUsize::try_from(idx + 1).unwrap();
                    let entry: Option<pg_sys::Datum> = row.get_datum_buffer_by_ordinal(no.into()).unwrap();
                    heap_tuple.set_by(no, entry).unwrap();
                }

                if let Some(datum) = heap_tuple.into_datum() {
                    results.push(AnyElement::from_polymorphic_datum(
                        datum,
                        false,
                        oid).unwrap())
                }
            }
        };

        Ok(SetOfIterator::new(results.into_iter()))
    })
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_asyncpg() {
        assert_eq!("Hello, asyncpg", crate::hello_asyncpg());
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
