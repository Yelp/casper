return {
  main = {
    -- worker_threads = 4,
    pin_worker_threads = true,
    listen = "0.0.0.0:8888",
  },

  http = {
    middleware = {
      {
        code = "require('lua.v2.redis')"
      },
    },

    access_log = {
      code = [[
        function() end
      ]],
    },
  },

  storage = {
    primary = {
      backend = "redis",
      server = { centralized = { endpoint = "127.0.0.1" } },
      pool_size = 4,
      compression_level = 3,
      wait_for_connect = 3,
    },
  }
}
