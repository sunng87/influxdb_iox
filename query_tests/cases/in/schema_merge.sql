-- Test functionality of merging schemas
-- IOX_SETUP: MultiChunkSchemaMerge

-- Different chunks have different schemas that need to be merged
SELECT * from cpu;

-- merge a subset of the columns
SELECT host, region, system from cpu;
