macro_rules! lua_try {
    ($result:expr) => {
        match $result {
            Ok(ok) => ok,
            Err(err) => return Ok(Err(format!("{err:#}"))),
        }
    };
}
