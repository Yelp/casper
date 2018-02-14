CREATE TABLE spectre_db.cache_store (
    bucket int,
    namespace text,
    cache_name text,
    id text,
    key text,
    vary_headers text,
    id_set set<text>,
    body blob,
    headers text,
    PRIMARY KEY ((bucket, namespace, cache_name), id, key, vary_headers)
) WITH
    compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

CREATE INDEX cache_store_id_set_index ON spectre_db.cache_store (id_set);