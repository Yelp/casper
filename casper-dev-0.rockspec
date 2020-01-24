package = "casper"
version = "dev-0"
source = {
  url = "git://github.com/Yelp/casper",
}
description = {
  summary = "Casper (a friendly Spectre)",
  homepage = "https://github.com/Yelp/casper",
  license = "Apache"
}
-- List all 3rd party dependencies that we need
dependencies = {
  "busted == 2.0.0-1",
  "cluacov == 0.1.1-1",
  "crc32 == 1.1-1",
  "lua-resty-http == 0.15-0",
  "lua-resty-lock == 0.08-0",
  "luacheck == 0.23.0-1",
  "luafilesystem == 1.7.0-2",
  "luasocket == 3.0rc1-2",
  "lyaml == 6.2.4-1",
}
-- We don't have any c file so there's nothing to compile,
-- however the build target is mandatory.
build = {
  type = "builtin",
  modules = {}
}
