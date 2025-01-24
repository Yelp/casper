use std::cell::RefCell;
use std::io;
use std::rc::Rc;

use csv::ByteRecord;
use mlua::{ExternalResult, Lua, Result, String as LuaString, Table};
use ntex::util::BytesMut;

// TODO: Full implementation of CSV writer and reader

/*
--- @class CSV
--- @tag module
---
--- Module to work with CSV format
local csv = {}
*/

#[derive(Clone)]
struct BytesMutCell(Rc<RefCell<BytesMut>>);

impl io::Write for BytesMutCell {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct CSVWriter(csv::Writer<BytesMutCell>, BytesMutCell);

/*
--- @within CSV
--- Encodes the Lua table (array) into CSV row.
---
--- #example
---
--- ```lua
--- local csv = require("@core/csv")
--- local row = csv.encode_record({"a", "b,c", 1})
--- assert(row == `a,"b,c",1\n`)
--- ```
---
--- @param array The Lua array to encode.
function csv.encode_record(array: { string | number }): string
    return nil :: any
end
*/
fn encode_record(lua: &Lua, record: Table) -> Result<LuaString> {
    let mut rec = ByteRecord::new();
    for field in record.sequence_values::<LuaString>() {
        let field = field?;
        rec.push_field(&field.as_bytes());
    }
    if rec.is_empty() {
        return lua.create_string("");
    }

    // Cache the CSV writer in the Lua state
    let mut csv_writer = match lua.app_data_mut::<CSVWriter>() {
        Some(val) => val,
        None => {
            let bytes = BytesMutCell(Rc::new(RefCell::new(BytesMut::new())));
            let csv_writer = CSVWriter(
                csv::WriterBuilder::new()
                    .flexible(true)
                    .has_headers(false)
                    .from_writer(bytes.clone()),
                bytes,
            );
            lua.set_app_data(csv_writer);
            lua.app_data_mut::<CSVWriter>().unwrap()
        }
    };
    // Remove all data if the buffer is not empty
    csv_writer.1 .0.borrow_mut().clear();
    csv_writer.0.write_byte_record(&rec).into_lua_err()?;
    csv_writer.0.flush().into_lua_err()?;

    let s = csv_writer.1 .0.borrow_mut().split().freeze();
    lua.create_string(s)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([("encode_record", lua.create_function(encode_record)?)])
}

/*
return csv
*/

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_module() -> Result<()> {
        let lua = Lua::new();

        let csv = super::create_module(&lua)?;
        lua.load(chunk! {
            assert($csv.encode_record({"a", "b", "c"}) == "a,b,c\n")
            assert($csv.encode_record({"a", "b", 3}) == "a,b,3\n")
            assert($csv.encode_record({"a", "b"}) == "a,b\n")
            assert($csv.encode_record({}) == "")
            assert($csv.encode_record({1, 2.5, "ab,c"}) == "1,2.5,\"ab,c\"\n")
        })
        .exec()?;

        Ok(())
    }
}
