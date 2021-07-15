-- Test system table access
-- Note that system tables reflect the state of chunks, so don't run them
-- with different chunk configurations.
-- IOX_SETUP: TwoMeasurementsManyFieldsOneChunk

SELECT id, partition_key, table_name, storage, memory_bytes, row_count from system.chunks;

SELECT * from system.columns order by table_name, column_name;

SELECT * from system.chunk_columns;
