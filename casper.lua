return {
  main = {
    -- worker_threads = 4,
    pin_worker_threads = true,
    listen = "0.0.0.0:8888",
  },

  http = {
    middleware = {
      {
        name = "redis",
        code = "require('lua/v2/middleware/redis')"
      },
      {
        name = "zipkin",
        code = "require('lua/v2/middleware/zipkin')"
      },
      {
        name = "spectre",
        code = "require('lua/v2/middleware/spectre')"
      },
      {
        name = "single_endpoint",
        code = "require('lua/v2/middleware/single_endpoint')"
      },
    },

    access_log = {
      code = [[
        function() end
      ]],
    },
  },

  metrics = {
    counters = {
      cache_hits_counter = {description = "Total number of cache HITs."},
      cache_misses_counter = {description = "Total number of cache MISSes."},
    },
  },

  storage = {
    primary = {
      backend = "redis",
      server = { centralized = { endpoint = "127.0.0.1" } },
      pool_size = 4,
      compression_level = 3,
      wait_for_connect = 3,
      internal_cache_size = 64 * 1024 * 1024,
      internal_cache_ttl = 2.0,
    },
  }
}
